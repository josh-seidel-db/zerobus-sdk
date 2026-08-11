// Real Arrow IPC codec stubs for the OCaml Zerobus SDK spike (Phase 7b / D3).
//
// This is the honest version: it actually #includes Apache Arrow C++ and calls
// the real IPC codec (arrow::ipc RecordBatchStreamWriter / RecordBatchStreamReader).
// It proves the ONE gap in native OCaml Arrow support (DESIGN.md §8.5.2): turning
// columnar data into the Arrow IPC byte blob and back. The Flight DoPut RPC that
// carries those bytes is already native OCaml (spike-flight/).
//
// C ABI (extern "C") so OCaml can bind it with plain `external` + Bytes/Bigarray.
// Everything is results-over-exceptions on the OCaml side: these functions return
// 0 on success and a negative code on failure, writing an error string.

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

extern "C" {

// A heap-allocated IPC byte buffer handed back to OCaml. OCaml copies it into an
// OCaml [bytes] via zbi_buffer_data/len then frees it with zbi_buffer_free.
struct zbi_buffer {
  uint8_t* data;
  int64_t len;
};

// Encode a RecordBatch with two columns — an int64 column and a UTF-8 string
// column — of [n] rows, to Arrow IPC *stream* format bytes. Returns 0 on success
// and fills [*out]; negative on failure with a message in [err]/[errlen].
//
// This deliberately builds the batch inside C++ from OCaml-provided arrays so the
// whole round-trip (OCaml values -> Arrow build -> IPC bytes -> parse -> read back
// -> OCaml) is exercised end to end.
int zbi_encode_int_str(const int64_t* ints, const char* const* strs,
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

  auto schema = arrow::schema({arrow::field("id", arrow::int64()),
                               arrow::field("name", arrow::utf8())});
  auto batch = arrow::RecordBatch::Make(schema, n, {ia, sa});

  auto sink_result = arrow::io::BufferOutputStream::Create();
  if (!sink_result.ok()) return fail("BufferOutputStream::Create failed");
  auto sink = *sink_result;

  auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
  if (!writer_result.ok()) return fail("MakeStreamWriter failed");
  auto writer = *writer_result;
  if (!writer->WriteRecordBatch(*batch).ok())
    return fail("WriteRecordBatch failed");
  if (!writer->Close().ok()) return fail("writer Close failed");

  auto buf_result = sink->Finish();
  if (!buf_result.ok()) return fail("sink Finish failed");
  auto buf = *buf_result;

  out->len = buf->size();
  out->data = (uint8_t*)std::malloc(out->len);
  if (out->data == nullptr) return fail("malloc failed");
  std::memcpy(out->data, buf->data(), out->len);
  return 0;
}

// Decode Arrow IPC *stream* bytes back into a RecordBatch and read the two columns
// out into caller-provided buffers, so OCaml can assert the round-trip is faithful.
// [ints] must have room for the row count returned via [*rows]; the string column
// is flattened into [strbuf] with per-row lengths in [strlens] (caller sizes both
// generously). Returns 0 on success, negative on failure.
int zbi_decode_int_str(const uint8_t* data, int64_t len, int64_t* out_ints,
                       char* strbuf, int32_t strbuf_cap, int32_t* out_strlens,
                       int64_t max_rows, int64_t* rows, char* err, int errlen) {
  auto fail = [&](const std::string& m) -> int {
    std::snprintf(err, errlen, "%s", m.c_str());
    return -1;
  };
  auto buf = std::make_shared<arrow::Buffer>(data, len);
  auto bis = std::make_shared<arrow::io::BufferReader>(buf);
  auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(bis);
  if (!reader_result.ok()) return fail("StreamReader::Open failed");
  auto reader = *reader_result;

  std::shared_ptr<arrow::RecordBatch> batch;
  if (!reader->ReadNext(&batch).ok()) return fail("ReadNext failed");
  if (batch == nullptr) return fail("no batch in stream");

  // Validate schema shape.
  if (batch->num_columns() != 2) return fail("expected 2 columns");
  if (batch->schema()->field(0)->name() != "id" ||
      batch->schema()->field(1)->name() != "name")
    return fail("unexpected column names");

  int64_t n = batch->num_rows();
  if (n > max_rows) return fail("too many rows");
  *rows = n;

  auto ints =
      std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
  auto strs =
      std::static_pointer_cast<arrow::StringArray>(batch->column(1));

  int32_t off = 0;
  for (int64_t i = 0; i < n; i++) {
    out_ints[i] = ints->Value(i);
    auto sv = strs->GetView(i);
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
// OCaml-facing stubs: marshal OCaml values <-> the pure C core above, using the
// OCaml runtime C API. These are what arrow_ipc.ml binds via `external`. They
// return an OCaml [(_, string) result]: [Ok v] = tag 0 (1 field), [Error m] =
// tag 1 (1 field).
// ---------------------------------------------------------------------------

#include <caml/alloc.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>

extern "C" {

// zbi_ocaml_encode_int_str : int array -> string array -> (bytes, string) result
CAMLprim value zbi_ocaml_encode_int_str(value ids, value names) {
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
  int rc = zbi_encode_int_str(civ.data(), cptr.data(), clen.data(), n, &buf, err,
                              sizeof(err));
  if (rc != 0) {
    payload = caml_copy_string(err);
    result = caml_alloc(1, 1);  // Error
    Store_field(result, 0, payload);
    CAMLreturn(result);
  }
  payload = caml_alloc_string(buf.len);
  std::memcpy((void*)Bytes_val(payload), buf.data, buf.len);
  zbi_buffer_free(&buf);
  result = caml_alloc(1, 0);  // Ok
  Store_field(result, 0, payload);
  CAMLreturn(result);
}

// zbi_ocaml_decode_int_str : bytes -> (int array * string array, string) result
CAMLprim value zbi_ocaml_decode_int_str(value ipc) {
  CAMLparam1(ipc);
  CAMLlocal5(result, pair, ids, names, s);
  int64_t len = caml_string_length(ipc);

  const int64_t max_rows = 1 << 20;
  std::vector<int64_t> oi(max_rows);
  std::vector<int32_t> sl(max_rows);
  const int32_t strcap = 1 << 24;
  std::vector<char> sbuf(strcap);
  int64_t rows = 0;
  char err[512] = {0};
  int rc = zbi_decode_int_str((const uint8_t*)Bytes_val(ipc), len, oi.data(),
                              sbuf.data(), strcap, sl.data(), max_rows, &rows,
                              err, sizeof(err));
  if (rc != 0) {
    s = caml_copy_string(err);
    result = caml_alloc(1, 1);  // Error
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
  result = caml_alloc(1, 0);  // Ok
  Store_field(result, 0, pair);
  CAMLreturn(result);
}

}  // extern "C"
