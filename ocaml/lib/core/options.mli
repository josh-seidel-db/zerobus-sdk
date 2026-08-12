(** Stream configuration, record types, and the public value types of the §5 API.
    Defaults are lifted from the Go SDK's [DefaultStreamConfigurationOptions] so
    behavior matches across the SDK family.

    {2 Flow control (no data loss)}

    Ingestion is pipelined: [ingest] queues a record and returns immediately while
    the send/ack happen in the background, so the driver holds an un-acked replay
    buffer for recovery. That buffer is bounded — by {!stream_options.max_inflight_requests}
    (count) and {!stream_options.max_inflight_bytes} (bytes) — but a record is
    {b never dropped} to stay within the bound; that would be silent data loss on
    recovery. When the buffer is full, {!overflow_policy} chooses between blocking
    the producer ([Block], the default) and failing the [ingest] with
    [Error (Backpressure _)] ([Fail]). The byte bound defaults to a smart budget
    derived from available memory, so a capable machine buffers far more than the
    1M-record count cap without risking an out-of-memory. *)

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

(** What [ingest] does when the un-acked replay buffer is full (by count or bytes).
    Neither policy ever DROPS a record — silent data loss on recovery is not
    acceptable. *)
type overflow_policy =
  | Block
      (** Backpressure: [ingest] waits until the ack-reader drains the buffer below
          the bound, then queues the record. Mirrors the Rust/Go semaphore. This is
          the default. *)
  | Fail
      (** [ingest] returns [Error (Backpressure _)] instead of blocking, letting the
          caller decide (retry later, throttle, deliberately drop). The buffer is
          never silently truncated. *)

type stream_options = {
  record_type : record_type;
  max_inflight_requests : int;
      (** Soft cap on the count of un-acked records held for replay. When reached,
          the {!overflow_policy} decides (block or fail) — records are never
          dropped. The effective cap is [min] of this and the byte budget. *)
  overflow_policy : overflow_policy;  (** Block (default) or Fail. See above. *)
  max_inflight_bytes : int option;
      (** Byte ceiling on the un-acked replay buffer. [None] derives a smart budget
          from the process's currently-available memory when the stream opens (grows
          well beyond 1M small records on a capable machine, but stays clear of a
          fraction of free memory so it never OOMs the process). *)
  recovery : bool;
  recovery_timeout_ms : int;
  recovery_backoff_ms : int;
  recovery_retries : int;
  server_lack_of_ack_timeout_ms : int;
  flush_timeout_ms : int;
  stream_paused_max_wait_time_ms : int option;
      (** Behavior on a server graceful close ([CloseStreamSignal] carrying the
          server's own close duration [d]), when [recovery] is on: [None] drains
          in-flight acks until the server ends the body, then reconnects+replays
          (most graceful); [Some 0] reconnects immediately; [Some x] drains but caps
          the wait at exactly [min (x, d)] before reconnecting. Matches the
          Rust/Go/Java [stream_paused_max_wait_time_ms]. Default [None]. With
          [recovery] off, a graceful close is terminal (all waiters fail with a
          stream-closed error rather than hang). *)
  connection_timeout_ms : int;
      (** Hard connection-establishment deadline (ms): [open_stream] races the
          transport connect against this timer and returns [Timeout] if the connect
          loses (the connect is then abandoned — cancelled on Lwt/Eio, detached on
          Async). Applies to every record type (not just Arrow). Config-parity with
          the Go/Java/Rust [connection_timeout_ms]; default 30000. [<= 0] disables
          the deadline. *)
  ack_callback : ack_callback option;
}

(** The Go-SDK defaults: Proto, 1M inflight, recovery on (15s timeout, 2s backoff,
    4 retries), 60s ack watchdog, 5min flush, graceful close = full server duration
    ([stream_paused_max_wait_time_ms = None]), 30s connection timeout, no callback. *)
val default_stream_options : stream_options
