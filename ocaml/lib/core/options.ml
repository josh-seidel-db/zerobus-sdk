type offset = int64

let offset_of_int64 x = x
let int64_of_offset x = x

type record_type = Proto | Json | Arrow
type descriptor = bytes

let descriptor_of_bytes b = b
let descriptor_to_bytes b = b

type table_properties = { table_name : string; descriptor : descriptor option }

type ack_callback = {
  on_ack : offset -> unit;
  on_error : offset -> string -> unit;
}

(* What to do when the un-acked buffer reaches [max_inflight_requests] (or the byte
   ceiling). NEVER drops records — that would be silent data loss on recovery. *)
type overflow_policy =
  | Block
      (** Backpressure: [ingest] waits until the ack-reader drains the buffer
          below the bound, then proceeds. Mirrors the Rust/Go semaphore.
          Default. *)
  | Fail
      (** [ingest] returns [Error (Backpressure ...)] instead of blocking, so
          the caller decides (retry later, slow down, drop deliberately). The
          buffer is never silently truncated. *)

type stream_options = {
  record_type : record_type;
  max_inflight_requests : int;
  overflow_policy : overflow_policy;
  max_inflight_bytes : int option;
      (** Byte ceiling on the un-acked replay buffer. [None] = derive a smart
          budget from the process's available memory at stream open (see below).
      *)
  recovery : bool;
  recovery_timeout_ms : int;
  recovery_backoff_ms : int;
  recovery_retries : int;
  server_lack_of_ack_timeout_ms : int;
  flush_timeout_ms : int;
  stream_paused_max_wait_time_ms : int option;
  connection_timeout_ms : int;
  ack_callback : ack_callback option;
}

let default_stream_options =
  {
    record_type = Proto;
    max_inflight_requests = 1_000_000;
    overflow_policy = Block;
    max_inflight_bytes = None;
    recovery = true;
    recovery_timeout_ms = 15_000;
    recovery_backoff_ms = 2_000;
    recovery_retries = 4;
    server_lack_of_ack_timeout_ms = 60_000;
    flush_timeout_ms = 300_000;
    stream_paused_max_wait_time_ms = None;
    connection_timeout_ms = 30_000;
    ack_callback = None;
  }
