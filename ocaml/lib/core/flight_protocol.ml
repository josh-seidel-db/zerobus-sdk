(** The Arrow Flight [DoPut] protocol for the streaming driver (DESIGN §8.5).

    A {!Stream.PROTOCOL} instance for [record_type = Arrow]. It matches the wire
    format the Zerobus service expects (as the Rust SDK produces):

    - the SCHEMA is sent ONCE, as the first FlightData's [data_header] (the
      schema IPC message metadata FlatBuffer). We carry those schema bytes in
      [table_properties.descriptor] (opaque to core), so core needs no libarrow;
      the [zerobus-arrow] codec produces them via
      [Zerobus_arrow.schema_message].
    - each record is a PACKED blob [ [4-byte BE header_len] header body ] from
      [Zerobus_arrow.encode], where [header] is the record-batch IPC message
      metadata FlatBuffer and [body] is the raw 8-byte-padded buffers. We split
      it into [FlightData.data_header] / [data_body] here — mechanically, no
      Arrow knowledge — and put the Zerobus offset in [app_metadata] as JSON
      [{"offset_id":N}] (the Rust SDK's FlightBatchMetadata).

    The durable watermark comes back in [PutResult.app_metadata]. This module
    needs NO libarrow — it only moves already-encoded bytes through the Flight
    protobuf. *)

module F = Zerobus_proto.Flight

let service = "arrow.flight.protocol.FlightService"
let rpc = "DoPut"

let encode_fd (r : F.flight_data) : string =
  let enc = Pbrt.Encoder.create () in
  F.encode_pb_flight_data r enc;
  Pbrt.Encoder.to_string enc

(* First FlightData: the flight_descriptor names the table (PATH), and data_header
   carries the Arrow SCHEMA message metadata (from table.descriptor). No data_body
   on a schema message. If no descriptor was supplied we send an empty header — the
   server will reject, surfacing the misconfiguration rather than sending garbage. *)
let create_frame ~(table : Options.table_properties)
    ~(options : Options.stream_options) : string =
  ignore options;
  let schema_header =
    match table.Options.descriptor with
    | Some d -> Options.descriptor_to_bytes d
    | None -> Bytes.empty
  in
  encode_fd
    (F.make_flight_data
       ~flight_descriptor:
         (F.make_flight_descriptor ~type_:F.Path
            ~path:[ table.Options.table_name ]
            ~cmd:Bytes.empty ())
       ~data_header:schema_header ~app_metadata:Bytes.empty
       ~data_body:Bytes.empty ())

(* Split a PACKED [ [4-byte BE header_len] header body ] blob (from
   Zerobus_arrow.encode) into (data_header, data_body). Malformed/short blobs fall
   back to an empty header + whole blob as body (the server then rejects it). *)
let split_packed (b : bytes) : bytes * bytes =
  let n = Bytes.length b in
  if n < 4 then (Bytes.empty, b)
  else
    let hlen =
      (Char.code (Bytes.get b 0) lsl 24)
      lor (Char.code (Bytes.get b 1) lsl 16)
      lor (Char.code (Bytes.get b 2) lsl 8)
      lor Char.code (Bytes.get b 3)
    in
    if 4 + hlen > n then (Bytes.empty, b)
    else
      let header = Bytes.sub b 4 hlen in
      let body = Bytes.sub b (4 + hlen) (n - 4 - hlen) in
      (header, body)

(* One record batch: split the packed IPC (header -> data_header, body ->
   data_body); the Zerobus offset rides in app_metadata as JSON {"offset_id":N}
   (rust/sdk FlightBatchMetadata). *)
let record_frame ~(record_type : Options.record_type) ~offset (b : bytes) :
    string =
  ignore record_type;
  let header, body = split_packed b in
  encode_fd
    (F.make_flight_data ~data_header:header
       ~app_metadata:
         (Bytes.of_string (Printf.sprintf {|{"offset_id":%Ld}|} offset))
       ~data_body:body ())

(* Read the integer value of a JSON key from a flat one-object payload like
   {"ack_up_to_offset":N,"ack_up_to_records":M} — minimal scan, no JSON dep in
   core. Returns the digits (incl. a leading '-') following [ "key" : ]. *)
let json_int_field (json : string) (key : string) : int64 option =
  let needle = "\"" ^ key ^ "\"" in
  let klen = String.length needle in
  let n = String.length json in
  let rec find i =
    if i + klen > n then None
    else if String.sub json i klen = needle then Some (i + klen)
    else find (i + 1)
  in
  match find 0 with
  | None -> None
  | Some after_key ->
      (* skip spaces and the ':' *)
      let rec skip j =
        if j < n && (json.[j] = ' ' || json.[j] = ':') then skip (j + 1) else j
      in
      let start = skip after_key in
      let is_num c = (c >= '0' && c <= '9') || c = '-' in
      let rec take j = if j < n && is_num json.[j] then take (j + 1) else j in
      let stop = take start in
      if stop > start then
        Int64.of_string_opt (String.sub json start (stop - start))
      else None

(* Ack: PutResult.app_metadata carries the durable watermark. The live Zerobus
   Arrow endpoint sends JSON {"ack_up_to_offset":N,"ack_up_to_records":M}; we also
   accept {"offset_id":N} (the batch-metadata key) and a bare decimal int64. *)
let decode_ack (s : string) : (Stream.ack, Error.t) result =
  match F.decode_pb_put_result (Pbrt.Decoder.of_string s) with
  | pr -> (
      let meta = String.trim (Bytes.to_string pr.F.app_metadata) in
      let w =
        if String.length meta > 0 && meta.[0] = '{' then
          match json_int_field meta "ack_up_to_offset" with
          | Some _ as o -> o
          | None -> json_int_field meta "offset_id"
        else Int64.of_string_opt meta
      in
      match w with
      | Some w -> Ok (Stream.Watermark w)
      | None ->
          (* A PutResult with empty / non-offset app_metadata is NOT fatal: the
             server may send a schema-accept, heartbeat, or otherwise metadata-less
             frame that carries no ack watermark. Treat it as [Created] (keep
             reading) — the Ephemeral protocol's equivalent — rather than a
             Protocol_error that would kill the whole Arrow stream on the first such
             frame. Surfaced to observers via [on_ack]-adjacent logging is left to
             the caller; here we simply do not advance the watermark. *)
          Ok Stream.Created)
  | exception Pbrt.Decoder.Failure _ ->
      (* A frame that isn't even a decodable PutResult is a genuine wire fault. *)
      Error (Error.Protocol_error "could not decode PutResult")
