// Real Arrow IPC codec stubs for the OCaml Zerobus SDK (Phase 7b / D3).
//
// Produces the Arrow-over-Flight wire encoding the Zerobus service actually
// expects (matching the Rust SDK / arrow-flight): the schema is sent ONCE as the
// first FlightData's data_header, then each record batch is split into
//   data_header = the IPC *message metadata* FlatBuffer (arrow::ipc::IpcPayload.metadata)
//   data_body   = the raw body buffers, each padded to 8 bytes (== body_length)
// (NOT a self-contained IPC *stream* stuffed opaquely into data_body — that made
// the server read data_header as a bogus FlatBuffer -> "Type i32 ... unaligned").
//
// To keep lib/core free of libarrow, the per-batch (header, body) pair is packed
// into one byte blob as [4-byte BE header_len][header][body]; the core Flight
// protocol splits it mechanically (no Arrow knowledge) into data_header/data_body.
//
// C ABI (extern "C"); results-over-exceptions on the OCaml side (0 ok, <0 fail).

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/message.h>
#include <arrow/util/bit_util.h>

extern "C" {

// A heap-allocated byte buffer handed back to OCaml. OCaml copies it out via
// zbi_buffer_data/len then frees it with zbi_buffer_free.
struct zbi_buffer {
  uint8_t* data;
  int64_t len;
};

static int fill_buffer(zbi_buffer* out, const std::string& s) {
  out->len = (int64_t)s.size();
  out->data = (uint8_t*)std::malloc(s.size() ? s.size() : 1);
  if (out->data == nullptr) return -1;
  std::memcpy(out->data, s.data(), s.size());
  return 0;
}

// The fixed v1 schema: { id: int64, name: utf8 }.
static std::shared_ptr<arrow::Schema> v1_schema() {
  return arrow::schema({arrow::field("id", arrow::int64()),
                        arrow::field("name", arrow::utf8())});
}

// arrow-flight uses 8-byte IPC alignment (default is 64, which bloats the body).
static arrow::ipc::IpcWriteOptions write_options() {
  auto opts = arrow::ipc::IpcWriteOptions::Defaults();
  opts.alignment = 8;
  return opts;
}

// Serialize an IpcPayload's body_buffers into one contiguous, 8-byte-padded blob
// (mirrors Arrow's stream writer: write each buffer, then pad to a multiple of 8).
static std::string payload_body(const arrow::ipc::IpcPayload& p) {
  std::string body;
  body.reserve(p.body_length);
  for (const auto& buf : p.body_buffers) {
    int64_t size = buf ? buf->size() : 0;
    if (size > 0) body.append((const char*)buf->data(), (size_t)size);
    int64_t padded = arrow::bit_util::RoundUpToMultipleOf8(size);
    for (int64_t i = size; i < padded; i++) body.push_back('\0');
  }
  return body;
}

// zbi_schema_message: the schema IPC message metadata FlatBuffer for the fixed v1
// schema -> data_header of the first FlightData. Body is empty for a schema msg.
int zbi_schema_message(zbi_buffer* out, char* err, int errlen) {
  auto fail = [&](const std::string& m) -> int {
    std::snprintf(err, errlen, "%s", m.c_str());
    return -1;
  };
  auto schema = v1_schema();
  arrow::ipc::DictionaryFieldMapper mapper(*schema);
  arrow::ipc::IpcPayload payload;
  auto st = arrow::ipc::GetSchemaPayload(*schema, write_options(), mapper, &payload);
  if (!st.ok()) return fail("GetSchemaPayload failed");
  std::string meta((const char*)payload.metadata->data(), payload.metadata->size());
  if (fill_buffer(out, meta) != 0) return fail("malloc failed");
  return 0;
}

// zbi_encode_batch: build a RecordBatch of [n] rows ({id,name}) and return the
// PACKED [4-byte BE header_len][header][body] blob for one FlightData record.
int zbi_encode_batch(const int64_t* ints, const char* const* strs,
                     const int32_t* strlens, int64_t n, zbi_buffer* out,
                     char* err, int errlen) {
  auto fail = [&](const std::string& m) -> int {
    std::snprintf(err, errlen, "%s", m.c_str());
    return -1;
  };
  arrow::Int64Builder ib;
  arrow::StringBuilder sb;
  for (int64_t i = 0; i < n; i++) {
    if (!ib.Append(ints[i]).ok()) return fail("int append failed");
    if (!sb.Append(strs[i], strlens[i]).ok()) return fail("str append failed");
  }
  std::shared_ptr<arrow::Array> ia, sa;
  if (!ib.Finish(&ia).ok()) return fail("int finish failed");
  if (!sb.Finish(&sa).ok()) return fail("str finish failed");
  auto schema = v1_schema();
  auto batch = arrow::RecordBatch::Make(schema, n, {ia, sa});

  arrow::ipc::IpcPayload payload;
  auto st = arrow::ipc::GetRecordBatchPayload(*batch, write_options(), &payload);
  if (!st.ok()) return fail("GetRecordBatchPayload failed");

  std::string header((const char*)payload.metadata->data(), payload.metadata->size());
  std::string body = payload_body(payload);

  int64_t hlen = (int64_t)header.size();
  std::string packed;
  packed.reserve(4 + header.size() + body.size());
  packed.push_back((char)((hlen >> 24) & 0xff));
  packed.push_back((char)((hlen >> 16) & 0xff));
  packed.push_back((char)((hlen >> 8) & 0xff));
  packed.push_back((char)(hlen & 0xff));
  packed.append(header);
  packed.append(body);
  if (fill_buffer(out, packed) != 0) return fail("malloc failed");
  return 0;
}

// zbi_decode_batch: given the schema message metadata + a PACKED record blob
// ([hdrlen][header][body]), reconstruct the RecordBatch and read the two columns
// out (for the mock/test to verify a faithful round-trip).
int zbi_decode_batch(const uint8_t* schema_meta, int64_t schema_meta_len,
                     const uint8_t* packed, int64_t packed_len, int64_t* out_ints,
                     char* strbuf, int32_t strbuf_cap, int32_t* out_strlens,
                     int64_t max_rows, int64_t* rows, char* err, int errlen) {
  auto fail = [&](const std::string& m) -> int {
    std::snprintf(err, errlen, "%s", m.c_str());
    return -1;
  };
  if (packed_len < 4) return fail("packed too short");
  int64_t hlen = ((int64_t)packed[0] << 24) | ((int64_t)packed[1] << 16) |
                 ((int64_t)packed[2] << 8) | (int64_t)packed[3];
  if (4 + hlen > packed_len) return fail("bad header_len");
  const uint8_t* hdr = packed + 4;
  const uint8_t* body = packed + 4 + hlen;
  int64_t body_len = packed_len - 4 - hlen;

  // Read schema from its message metadata.
  auto schema_meta_buf = std::make_shared<arrow::Buffer>(schema_meta, schema_meta_len);
  auto schema_msg_res = arrow::ipc::Message::Open(schema_meta_buf, nullptr);
  if (!schema_msg_res.ok()) return fail("schema Message::Open failed");
  arrow::ipc::DictionaryMemo memo;
  auto schema_res = arrow::ipc::ReadSchema(**schema_msg_res, &memo);
  if (!schema_res.ok()) return fail("ReadSchema failed");
  auto schema = *schema_res;

  // Reconstruct the record-batch message from (metadata, body) and read it.
  auto hdr_buf = std::make_shared<arrow::Buffer>(hdr, hlen);
  auto body_buf = std::make_shared<arrow::Buffer>(body, body_len);
  auto msg_res = arrow::ipc::Message::Open(hdr_buf, body_buf);
  if (!msg_res.ok()) return fail("batch Message::Open failed");
  auto batch_res = arrow::ipc::ReadRecordBatch(
      **msg_res, schema, &memo, arrow::ipc::IpcReadOptions::Defaults());
  if (!batch_res.ok()) return fail("ReadRecordBatch failed");
  auto batch = *batch_res;

  if (batch->num_columns() != 2) return fail("expected 2 columns");
  int64_t nn = batch->num_rows();
  if (nn > max_rows) return fail("too many rows");
  *rows = nn;
  auto icol = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
  auto scol = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
  int32_t off = 0;
  for (int64_t i = 0; i < nn; i++) {
    out_ints[i] = icol->Value(i);
    auto sv = scol->GetView(i);
    if (off + (int32_t)sv.size() > strbuf_cap) return fail("strbuf overflow");
    std::memcpy(strbuf + off, sv.data(), sv.size());
    out_strlens[i] = (int32_t)sv.size();
    off += (int32_t)sv.size();
  }
  return 0;
}

int64_t zbi_buffer_len(const zbi_buffer* b) { return b->len; }
const uint8_t* zbi_buffer_data(const zbi_buffer* b) { return b->data; }
void zbi_buffer_free(zbi_buffer* b) {
  if (b->data) std::free(b->data);
  b->data = nullptr;
  b->len = 0;
}

}  // extern "C"

// ---------------------------------------------------------------------------
// OCaml-facing stubs (OCaml runtime C API). Return an OCaml [(_, string) result]:
// [Ok v] = tag 0 (1 field), [Error m] = tag 1 (1 field).
// ---------------------------------------------------------------------------

#include <caml/alloc.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>

extern "C" {

// zbi_ocaml_schema_message : unit -> (bytes, string) result
CAMLprim value zbi_ocaml_schema_message(value unit) {
  CAMLparam1(unit);
  CAMLlocal2(result, payload);
  zbi_buffer buf;
  char err[512] = {0};
  if (zbi_schema_message(&buf, err, sizeof(err)) != 0) {
    payload = caml_copy_string(err);
    result = caml_alloc(1, 1);
    Store_field(result, 0, payload);
    CAMLreturn(result);
  }
  payload = caml_alloc_string(buf.len);
  std::memcpy((void*)Bytes_val(payload), buf.data, buf.len);
  zbi_buffer_free(&buf);
  result = caml_alloc(1, 0);
  Store_field(result, 0, payload);
  CAMLreturn(result);
}

// zbi_ocaml_encode_batch : int array -> string array -> (bytes, string) result
CAMLprim value zbi_ocaml_encode_batch(value ids, value names) {
  CAMLparam2(ids, names);
  CAMLlocal2(result, payload);
  int64_t n = Wosize_val(ids);
  std::vector<int64_t> civ(n);
  std::vector<std::string> csv(n);
  std::vector<const char*> cptr(n);
  std::vector<int32_t> clen(n);
  for (int64_t i = 0; i < n; i++) {
    civ[i] = Long_val(Field(ids, i));
    csv[i] = std::string(String_val(Field(names, i)),
                         caml_string_length(Field(names, i)));
    cptr[i] = csv[i].data();
    clen[i] = (int32_t)csv[i].size();
  }
  zbi_buffer buf;
  char err[512] = {0};
  if (zbi_encode_batch(civ.data(), cptr.data(), clen.data(), n, &buf, err,
                       sizeof(err)) != 0) {
    payload = caml_copy_string(err);
    result = caml_alloc(1, 1);
    Store_field(result, 0, payload);
    CAMLreturn(result);
  }
  payload = caml_alloc_string(buf.len);
  std::memcpy((void*)Bytes_val(payload), buf.data, buf.len);
  zbi_buffer_free(&buf);
  result = caml_alloc(1, 0);
  Store_field(result, 0, payload);
  CAMLreturn(result);
}

// zbi_ocaml_decode_batch : bytes -> bytes -> (int array * string array, string) result
//   arg1 = schema message metadata, arg2 = packed [hdrlen][header][body]
CAMLprim value zbi_ocaml_decode_batch(value schema_meta, value packed) {
  CAMLparam2(schema_meta, packed);
  CAMLlocal5(result, pair, ids, names, s);
  const int64_t max_rows = 1 << 20;
  std::vector<int64_t> oi(max_rows);
  std::vector<int32_t> sl(max_rows);
  const int32_t strcap = 1 << 24;
  std::vector<char> sbuf(strcap);
  int64_t rows = 0;
  char err[512] = {0};
  int rc = zbi_decode_batch(
      (const uint8_t*)Bytes_val(schema_meta), caml_string_length(schema_meta),
      (const uint8_t*)Bytes_val(packed), caml_string_length(packed), oi.data(),
      sbuf.data(), strcap, sl.data(), max_rows, &rows, err, sizeof(err));
  if (rc != 0) {
    s = caml_copy_string(err);
    result = caml_alloc(1, 1);
    Store_field(result, 0, s);
    CAMLreturn(result);
  }
  ids = caml_alloc(rows, 0);
  for (int64_t i = 0; i < rows; i++) Store_field(ids, i, Val_long(oi[i]));
  names = caml_alloc(rows, 0);
  int32_t off = 0;
  for (int64_t i = 0; i < rows; i++) {
    s = caml_alloc_initialized_string(sl[i], sbuf.data() + off);
    off += sl[i];
    Store_field(names, i, s);
  }
  pair = caml_alloc_tuple(2);
  Store_field(pair, 0, ids);
  Store_field(pair, 1, names);
  result = caml_alloc(1, 0);
  Store_field(result, 0, pair);
  CAMLreturn(result);
}

}  // extern "C"
