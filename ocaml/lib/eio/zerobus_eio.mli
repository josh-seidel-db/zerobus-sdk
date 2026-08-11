(** The public Eio API for the Zerobus Ingest SDK (DESIGN.md §5.2, §6.2).

    The direct-style counterpart of the Lwt {!Zerobus} module. Because Eio is
    scope-based — a background fiber must live inside an {!Eio.Switch} — this
    façade is bracket-shaped: {!with_stream} / {!with_stream_oauth} own the switch
    for the stream's lifetime and tear everything down on exit.

    The cardinal rule of ingestion: queue records in a loop, then {!flush} once.
    Never wait after every {!ingest} — throughput collapses. *)

(** A Zerobus client: workspace metadata + the derived gRPC endpoint. Opaque. *)
type t

(** A live stream, valid only within the {!with_stream} / {!with_stream_oauth}
    body that owns it. Opaque. *)
type stream

(** A per-record durability handle (re-exported from core). *)
type offset = Zerobus_core.Options.offset

(** Stream configuration types (re-exported for convenience). *)
type table_properties = Zerobus_core.Options.table_properties
type stream_options = Zerobus_core.Options.stream_options

(** The Go-SDK defaults (Proto, 1M inflight, recovery on, etc.). *)
val default_stream_options : stream_options

(** {1 Ingestion operations (direct-style — no monad)} *)

(** Queue one record for ingestion and return its offset immediately. Does NOT
    wait for the ack — the record is sent on a background fiber. *)
val ingest : stream -> bytes -> (offset, Zerobus_core.Error.t) result

(** Queue a batch; returns the offset of the last record. Same non-waiting
    semantics as {!ingest}; prefer this in hot paths (amortized framing). *)
val ingest_records : stream -> bytes list -> (offset, Zerobus_core.Error.t) result

(** Wait until [offset] (and all prior offsets) are durably acked. The watermark
    is monotonic. Use sparingly — for normal ingestion use {!flush}. *)
val wait_for_offset : stream -> offset -> (unit, Zerobus_core.Error.t) result

(** Wait once for all pending records to be durable — the end-of-loop call in the
    loop-then-flush pattern. *)
val flush : stream -> (unit, Zerobus_core.Error.t) result

(** Half-close and tear down the stream. Normally handled by the bracket
    ({!with_stream} closes on exit); exposed for explicit early close. *)
val close : stream -> (unit, Zerobus_core.Error.t) result

(** {1 Construction} *)

(** Construct a client from a workspace URL and optional gRPC endpoint. When
    [endpoint] is empty or ["default"] it is derived from [workspace_url].
    Returns [Auth_error]/[Transport_error] if the URL cannot be parsed. *)
val create :
  ?application_name:string ->
  endpoint:string ->
  workspace_url:string ->
  unit ->
  (t, Zerobus_core.Error.t) result

(** Open a stream with caller-supplied gRPC [headers] and run [f] with it,
    tearing the stream + its scope down on exit (bracket form).

    [env] / [sw] come from the caller's [Eio_main.run] / [Switch.run]; they are
    installed into the transport context for the duration of [f]. [tls] defaults
    to [true]; pass [false] for a cleartext-h2c mock. [:authority] is derived from
    the endpoint if [headers] omit it. [authenticator] overrides the system trust
    store for the TLS handshake (default: [Ca_certs]) — mainly for tests pinning a
    self-signed cert. Returns the result of [f], or [Error] if the stream could
    not be opened. *)
val with_stream :
  env:< net : [ `Generic | `Unix ] Eio.Net.ty Eio.Resource.t ; .. > ->
  sw:Eio.Switch.t ->
  ?tls:bool ->
  ?authenticator:X509.Authenticator.t ->
  t ->
  table_properties ->
  headers:(string * string) list ->
  ?options:stream_options ->
  (stream -> 'a) ->
  ('a, Zerobus_core.Error.t) result

(** {1 Built-in client-credentials OAuth (§12.2)} *)

(** Mint a table-scoped OAuth token via the client-credentials grant (HTTPS POST
    to [<workspace_url>/oidc/v1/token], scope + UC authorization_details; the
    id/secret ride in the HTTP Basic header). Direct-style, over cohttp-eio + real
    TLS. Exposed for callers that want the token itself. *)
val mint_token :
  env:< net : [ `Generic | `Unix ] Eio.Net.ty Eio.Resource.t ; .. > ->
  sw:Eio.Switch.t ->
  client:t ->
  table:string ->
  client_id:string ->
  client_secret:string ->
  (string, Zerobus_core.Error.t) result

(** Open a stream with built-in client-credentials OAuth and run [f] with it
    (bracket form) — the ergonomic counterpart of the Lwt [Zerobus.create_stream].
    Mints a table-scoped token, builds the bearer + table-name headers, opens the
    stream over TLS, runs [f], and tears down on exit. *)
val with_stream_oauth :
  env:< net : [ `Generic | `Unix ] Eio.Net.ty Eio.Resource.t ; .. > ->
  sw:Eio.Switch.t ->
  t ->
  table_properties ->
  client_id:string ->
  client_secret:string ->
  ?options:stream_options ->
  (stream -> 'a) ->
  ('a, Zerobus_core.Error.t) result

(** {1 Low-level access (tests / advanced)} *)

(** The Eio {!Zerobus_core.Io.IO} instantiation (transport, [Scope], [Ctx], …). *)
module Io_eio_for_test : module type of Zerobus_io_eio

(** The Eio streaming driver ([Zerobus_core.Make] over the Eio IO). *)
module Driver : module type of Zerobus_io_eio.Stream
