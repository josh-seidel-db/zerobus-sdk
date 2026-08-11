(** Apache Arrow IPC codec for the Zerobus SDK's [record_type = Arrow] path
    (DESIGN §8.5.2) — the optional [zerobus-arrow] package, the only component that
    links libarrow.

    Produces the Arrow-over-Flight encoding the Zerobus service expects (matching
    the Rust SDK): the schema is conveyed ONCE via {!schema_message} (the driver
    sends it as the first FlightData's [data_header]); each record is then encoded
    with {!encode} into a PACKED blob [ [4-byte big-endian header_len] header body ]
    where [header] is the record-batch IPC message metadata FlatBuffer and [body]
    is the raw buffers padded to 8 bytes. The core Flight protocol splits that blob
    into [FlightData.data_header] / [data_body] with no Arrow knowledge — so
    nothing else in the SDK links libarrow.

    v1 schema is a fixed two-column row [{ id : int64 ; name : utf8 }]; a
    schema-parametric API is future work. *)

(** One row of the fixed v1 schema: an int64 [id] and a utf8 [name]. *)
type row = { id : int; name : string }

(** [schema_message ()] is the IPC schema-message metadata FlatBuffer for the v1
    schema — sent once as the first FlightData's [data_header] before any batch.
    [Error msg] if the Arrow C++ writer fails. *)
val schema_message : unit -> (bytes, string) result

(** [encode rows] is the PACKED [ header_len | header | body ] blob for [rows] as
    one record batch (see the module doc), or [Error msg] on failure. Hand the
    result to the SDK's [ingest] with [record_type = Arrow]. *)
val encode : row list -> (bytes, string) result

(** [decode ~schema_message packed] reconstructs the rows from the schema message
    metadata and a {!encode}-packed blob, or [Error msg] if the bytes are not a
    well-formed batch of the expected schema. (Used by tests / a verifying peer.) *)
val decode : schema_message:bytes -> bytes -> (row list, string) result
