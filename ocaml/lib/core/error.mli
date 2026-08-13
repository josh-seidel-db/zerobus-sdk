(** The SDK error taxonomy. Mirrors the sister SDK's [Error.t] shape and the
    Rust core's retryable/non-retryable classification (see rust
    `is_retryable`), so recovery (§12.3) can decide what to retry.
    Results-over-exceptions is a golden rule: public operations return
    [(_, Error.t) result], they do not raise. *)

type t =
  | Transport_error of string
      (** connection / TLS / ALPN / h2 failure — generally retryable *)
  | Stream_error of string
      (** the stream is closed or misused (e.g. ingest after close) *)
  | Auth_error of string  (** OAuth / token minting failure *)
  | Server_status of { code : int; message : string }
      (** a non-OK gRPC status from the service *)
  | Protocol_error of string  (** malformed / unexpected wire message *)
  | Timeout of string  (** ack watchdog / flush / connect deadline *)
  | Backpressure of string
      (** the un-acked buffer is full and [overflow_policy = Fail]: the caller
          over-produced relative to the ack rate. The stream is healthy — retry
          the [ingest] after waiting / flushing. Not retryable by the recovery
          driver. *)

val to_string : t -> string

val is_retryable : t -> bool
(** Whether an operation that failed with this error is worth retrying — the
    recovery driver consults this. Transport errors, timeouts, and the retryable
    gRPC status codes (UNAVAILABLE=14, etc.) are retryable; auth, protocol, and
    stream-misuse errors are not. *)
