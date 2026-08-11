(** The public Async API for the Zerobus Ingest SDK (DESIGN.md §5.2, §5.3).

    The Async counterpart of the Lwt {!Zerobus} module, bridging
    {!Zerobus_core.Make}[(Zerobus_io_async)] to an ergonomic entry point.

    {b Scope of this façade.} Unlike the Lwt reference, this module does NOT offer
    a built-in client-credentials [create_stream]: the Async switch has no HTTP
    client (cohttp-async) / JSON wired, and the Async TLS transport is deferred
    (Lwt is the live-verified TLS+ALPN reference). Auth is therefore supplied by
    the caller through [headers_provider] — which, with [~tls:false], also drives
    the full façade→driver path against a cleartext mock. Live TLS + built-in OAuth
    on Async are tracked follow-ups.

    The cardinal rule of ingestion: queue records in a loop, then {!flush} once.
    Never wait after every {!ingest} — throughput collapses. *)

open! Core
open! Async

(** A Zerobus client: workspace metadata + the derived gRPC endpoint. Opaque. *)
type t

(** A live stream bound to its owning scope (§5.2, §5.3). Opaque. *)
type stream

(** A per-record durability handle (re-exported from core). *)
type offset = Zerobus_core.Options.offset

(** Stream configuration types (re-exported for convenience). *)
type table_properties = Zerobus_core.Options.table_properties
type stream_options = Zerobus_core.Options.stream_options

(** The Go-SDK defaults (Proto, 1M inflight, recovery on, etc.). *)
val default_stream_options : stream_options

(** {1 Ingestion operations} *)

(** Queue one record and return its offset immediately. Does NOT wait for the ack
    — the record is sent on a background job. *)
val ingest : stream -> bytes -> (offset, Zerobus_core.Error.t) result Deferred.t

(** Queue a batch; returns the offset of the last record. Same non-waiting
    semantics as {!ingest}; prefer this in hot paths. *)
val ingest_records :
  stream -> bytes list -> (offset, Zerobus_core.Error.t) result Deferred.t

(** Wait until [offset] (and all prior offsets) are durably acked. Monotonic
    watermark. Use sparingly — for normal ingestion use {!flush}. *)
val wait_for_offset :
  stream -> offset -> (unit, Zerobus_core.Error.t) result Deferred.t

(** Wait once for all pending records to be durable — the end-of-loop call in the
    loop-then-flush pattern. *)
val flush : stream -> (unit, Zerobus_core.Error.t) result Deferred.t

(** Close the stream and tear down its scope (best-effort join of the ack-reader).
    Normally handled by the {!with_stream} bracket; exposed for explicit close. *)
val close : stream -> (unit, Zerobus_core.Error.t) result Deferred.t

(** {1 Construction} *)

(** Construct a client from a workspace URL and optional gRPC endpoint. When
    [endpoint] is empty or ["default"] it is derived from [workspace_url].
    Returns [Auth_error]/[Transport_error] if the URL cannot be parsed. *)
val create :
  ?application_name:string ->
  endpoint:string ->
  workspace_url:string ->
  unit ->
  (t, Zerobus_core.Error.t) result Deferred.t

(** Bracket form: open a stream with caller-supplied gRPC headers (custom auth),
    run [f] with it inside the owning scope, then tear the stream + scope down.

    The [headers_provider] returns the header list (e.g. an [authorization]
    bearer token + the table-name header). [:authority] is added from the
    endpoint if the provider omits it. [tls] defaults to [true]; pass [false] for
    a cleartext-h2c mock backend. Returns the result of [f], or [Error] if the
    stream could not be opened. *)
val with_stream :
  ?tls:bool ->
  t ->
  table_properties ->
  headers_provider:
    (unit -> ((string * string) list, Zerobus_core.Error.t) result Deferred.t) ->
  ?options:stream_options ->
  (stream -> 'a Deferred.t) ->
  ('a, Zerobus_core.Error.t) result Deferred.t

(** {1 Low-level access (tests / advanced)} *)

(** The Async {!Zerobus_core.Io.IO} instantiation (transport, [Scope], …). *)
module Io_async_for_test : module type of Zerobus_io_async

(** The Async streaming driver ([Zerobus_core.Make] over the Async IO). *)
module Driver : module type of Zerobus_io_async.Stream
