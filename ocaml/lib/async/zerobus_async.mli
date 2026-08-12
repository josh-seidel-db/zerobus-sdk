(** The public Async API for the Zerobus Ingest SDK (DESIGN.md §5.2, §5.3).

    The Async counterpart of the Lwt {!Zerobus} module, bridging
    {!Zerobus_core.Make}[(Zerobus_io_async)] to an ergonomic entry point.

    {b Scope of this façade.} It is bracket-shaped ([with_stream] /
    [with_stream_oauth]) rather than detached, matching Async's lack of hard fiber
    cancellation. Live TLS and built-in client-credentials OAuth are both available
    as dune [select]s on optional deps ([tls-async] and [cohttp-async]): when the
    dep is present they work; when absent, [with_stream_oauth]/[mint_token] return
    an [Auth_error] ([oauth_available] reports which) and TLS connects return an
    honest error — so the caller supplies a bearer via [headers_provider] and, with
    [~tls:false], drives the full façade→driver path against a cleartext mock.
    (Lwt/Eio are the always-on TLS references; see doc/arch/tls_async_status.md.)

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

(** {1 Built-in client-credentials OAuth (§12.2)} *)

(** Whether built-in OAuth is compiled in — [true] iff [cohttp-async] was present
    at build time (a dune [select]). When [false], {!mint_token} /
    {!with_stream_oauth} return an [Auth_error]; supply a bearer via
    {!with_stream}'s [headers_provider] instead. *)
val oauth_available : bool

(** Mint a table-scoped OAuth token via the client-credentials grant (HTTPS POST
    to [<workspace_url>/oidc/v1/token]; id/secret in the HTTP Basic header). The
    token is cached on [client] with its expiry and refreshed ~30s early. Returns
    [Auth_error] on HTTP/parse failure, [Transport_error] on a request exception. *)
val mint_token :
  client:t ->
  table:string ->
  client_id:string ->
  client_secret:string ->
  (string, Zerobus_core.Error.t) result Deferred.t

(** Open a stream with built-in client-credentials OAuth and run [f] with it
    (bracket form) — the ergonomic Async counterpart of the Lwt
    [Zerobus.create_stream] / the Eio [Zerobus_eio.with_stream_oauth]. Mints a
    table-scoped token, builds the bearer + table-name headers, opens the stream
    over TLS inside the owning scope, then tears it down on exit. *)
val with_stream_oauth :
  t ->
  table_properties ->
  client_id:string ->
  client_secret:string ->
  ?options:stream_options ->
  (stream -> 'a Deferred.t) ->
  ('a, Zerobus_core.Error.t) result Deferred.t

(** {1 Low-level access (tests / advanced)} *)

(** The Async {!Zerobus_core.Io.IO} instantiation (transport, [Scope], …). *)
module Io_async_for_test : module type of Zerobus_io_async

(** The Async streaming driver ([Zerobus_core.Make] over the Async IO). *)
module Driver : module type of Zerobus_io_async.Stream

(** The TLS connect backend (a dune [select] on tls-async: the real backend when
    tls-async is installed, else an honest-error stub). Exposed so the in-tree
    live-TLS test can pin a self-signed cert via [pinned_cert_fp_sha256_b64] — the
    Async analogue of the Eio [~authenticator] override. Not part of the ergonomic
    surface (see doc/arch/tls_async_status.md). *)
module Tls_connect : sig
  val available : bool

  (** When [Some "<sha256-b64>"], authenticate the TLS peer by pinning that exact
      certificate fingerprint instead of the system trust store — for testing
      against a self-signed h2 mock. Prod leaves it [None]. Inert in the
      honest-error stub backend. *)
  val pinned_cert_fp_sha256_b64 : string option ref
end
