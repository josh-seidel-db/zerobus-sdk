type offset = int64

let offset_of_int64 x = x
let int64_of_offset x = x

type record_type = Proto | Json | Arrow
type descriptor = bytes

let descriptor_of_bytes b = b
let descriptor_to_bytes b = b

type table_properties = {
  table_name : string;
  descriptor : descriptor option;
}

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

let default_stream_options =
  {
    record_type = Proto;
    max_inflight_requests = 1_000_000;
    recovery = true;
    recovery_timeout_ms = 15_000;
    recovery_backoff_ms = 2_000;
    recovery_retries = 4;
    server_lack_of_ack_timeout_ms = 60_000;
    flush_timeout_ms = 300_000;
    ack_callback = None;
  }
