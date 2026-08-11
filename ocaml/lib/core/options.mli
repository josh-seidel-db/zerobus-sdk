(** Stream configuration, record types, and the public value types of the §5 API.
    Defaults are lifted from the Go SDK's [DefaultStreamConfigurationOptions] so
    behavior matches across the SDK family. *)

(** A per-record durability handle: the offset the server acks against. Opaque so
    callers treat it as a token to wait on, not an int to compute with. *)
type offset = private int64

val offset_of_int64 : int64 -> offset
val int64_of_offset : offset -> int64

(** Which encoding the stream carries. Proto/JSON are wired in Phase 5; Arrow
    (Flight DoPut + the proven IPC codec) is Phase 7. *)
type record_type = Proto | Json | Arrow

(** A typed wrapper over the serialized proto [DescriptorProto] the server needs
    (see §5.5). Phase 5 only carries it; the smart constructors land with Proto
    ingest wiring. *)
type descriptor

val descriptor_of_bytes : bytes -> descriptor
val descriptor_to_bytes : descriptor -> bytes

type table_properties = {
  table_name : string;  (** catalog.schema.table *)
  descriptor : descriptor option;  (** required for Proto, [None] for JSON *)
}

(** Async ack notification (style (b), §5.3): [on_ack]/[on_error] fire from the
    ack-reader fiber. *)
type ack_callback = {
  on_ack : offset -> unit;
  on_error : offset -> string -> unit;
}

type stream_options = {
  record_type : record_type;
  max_inflight_requests : int;
  recovery : bool;
  recovery_timeout_ms : int;
  recovery_backoff_ms : int;
  recovery_retries : int;
  server_lack_of_ack_timeout_ms : int;
  flush_timeout_ms : int;
  ack_callback : ack_callback option;
}

(** The Go-SDK defaults: Proto, 1M inflight, recovery on (15s timeout, 2s backoff,
    4 retries), 60s ack watchdog, 5min flush, no callback. *)
val default_stream_options : stream_options
