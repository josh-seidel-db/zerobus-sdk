(** OCaml binding to the real Apache Arrow C++ IPC codec (DESIGN §8.5.2).

    The optional [zerobus-arrow] package: the ONLY part of the SDK that links
    libarrow. It produces the Arrow-over-Flight encoding the Zerobus service
    expects (matching the Rust SDK): a one-time SCHEMA message, then per-record
    a PACKED [header_len][header][body] blob where [header] is the record-batch
    IPC message metadata FlatBuffer and [body] is the raw, 8-byte-padded
    buffers. The core Flight protocol ({!Zerobus_core.Flight_protocol}) splits
    that blob into [FlightData.data_header] / [FlightData.data_body]
    mechanically, without any libarrow dependency itself.

    A caller who wants [record_type = Arrow] links this package, calls
    {!schema_message} once (the driver sends it as the first FlightData) and
    {!encode} per record, handing the bytes to [ingest].
    Results-over-exceptions. *)

(* --- external C stubs (see zerobus_arrow_stubs.cc) --- *)

(* The schema IPC message metadata for the fixed v1 schema {id:int64; name:utf8}.
   Sent ONCE as the first FlightData's data_header (schema precedes batches). *)
external c_schema_message : unit -> (bytes, string) result
  = "zbi_ocaml_schema_message"

(* Encode one record batch to the PACKED [hdrlen][header][body] blob. *)
external c_encode_batch : int array -> string array -> (bytes, string) result
  = "zbi_ocaml_encode_batch"

(* Decode: (schema_message_metadata, packed_blob) -> (ids, names) or error. *)
external c_decode_batch :
  bytes -> bytes -> (int array * string array, string) result
  = "zbi_ocaml_decode_batch"

type row = { id : int; name : string }

let schema_message () : (bytes, string) result = c_schema_message ()

let encode (rows : row list) : (bytes, string) result =
  let ids = Array.of_list (List.map (fun r -> r.id) rows) in
  let names = Array.of_list (List.map (fun r -> r.name) rows) in
  c_encode_batch ids names

let decode ~schema_message:(schema : bytes) (packed : bytes) :
    (row list, string) result =
  match c_decode_batch schema packed with
  | Error _ as e -> e
  | Ok (ids, names) ->
      Ok
        (List.map2
           (fun id name -> { id; name })
           (Array.to_list ids) (Array.to_list names))
