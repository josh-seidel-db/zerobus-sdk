(** The public Lwt API for the Zerobus Ingest SDK (DESIGN.md §5.2, §5.3).

    Entry point: call [create] with workspace credentials to get a client, then
    [create_stream] to open a stream to a table.

    The three ingestion operations ({!ingest}, {!ingest_records}, {!flush})
    follow the cardinal rule: queue in a loop, flush once. Never wait after
    every ingest — throughput collapses.

    Example:

    {[
    let* client =
      Zerobus.create
        ~endpoint:"my-workspace.zerobus.us-west-2.cloud.databricks.com:443"
        ~workspace_url:"https://my-workspace.azuredatabricks.net" ()
    in
    let* stream =
      Zerobus.create_stream client
        { table_name = "my_catalog.my_schema.my_table"; descriptor = None }
        ~client_id:"..." ~client_secret:"..." ()
    in
    let* () =
      Lwt_list.iter_s (fun r -> Zerobus.ingest stream r >|= ignore) records
    in
    let* () = Zerobus.flush stream in
    let* () = Zerobus.close stream in
    Lwt.return_unit
    ]} *)

type t
(** A Zerobus client holds workspace credentials and endpoint metadata. *)

type stream
(** A live stream to a table, ready for ingestion. *)

type offset = Zerobus_core.Options.offset
(** An offset handle: the durability watermark the caller can wait on. *)

type table_properties = Zerobus_core.Options.table_properties
(** Stream configuration (re-exported for convenience). *)

type stream_options = Zerobus_core.Options.stream_options

val default_stream_options : stream_options
(** The default stream options (Proto encoding, 1M inflight, recovery enabled,
    etc.). *)

val create :
  ?application_name:string ->
  endpoint:string ->
  workspace_url:string ->
  unit ->
  (t, Zerobus_core.Error.t) result Lwt.t
(** Construct a client from workspace URL and optional gRPC endpoint.

    Parameters:
    - [application_name]: optional app name for diagnostics/headers
    - [endpoint]: gRPC endpoint (host:port). If empty or "default", derived from
      workspace_url as <workspace-id>.zerobus.<region>.cloud.databricks.com:443
    - [workspace_url]: Databricks workspace URL (e.g.,
      https://my-workspace.azuredatabricks.net). Used to extract workspace-id
      and region if endpoint is not provided.

    Returns [Auth_error] or [Transport_error] if the workspace URL is invalid.
*)

val create_stream :
  t ->
  table_properties ->
  client_id:string ->
  client_secret:string ->
  ?options:stream_options ->
  unit ->
  (stream, Zerobus_core.Error.t) result Lwt.t
(** Open a stream with client-credentials OAuth.

    Mints a table-scoped token (client-credentials grant with
    authorization_details for the catalog/schema/table), builds gRPC headers,
    and opens a bidi RPC to the Zerobus endpoint.

    The [table_properties] must include [table_name] (catalog.schema.table). The
    [descriptor] is required for Proto encoding; pass [None] for JSON.

    Parameters:
    - [client_id], [client_secret]: service principal credentials
    - [options]: stream configuration (defaults to Go SDK defaults)

    Returns a live [stream] ready for ingestion. Must be closed with {!close}.
    Raises [Auth_error] on token-mint failure, [Transport_error] on connection
    failure, or [Protocol_error] on framing issues. *)

val create_stream_with_headers :
  ?tls:bool ->
  t ->
  table_properties ->
  headers_provider:
    (unit -> ((string * string) list, Zerobus_core.Error.t) result Lwt.t) ->
  ?options:stream_options ->
  unit ->
  (stream, Zerobus_core.Error.t) result Lwt.t
(** Custom-auth variant: supply headers directly.

    Bypasses OAuth and uses the [headers_provider] to supply gRPC headers on
    each call (useful for federated auth or custom bearer token schemes).

    The [headers_provider] is a function that returns a fresh list of headers
    [(name, value)]. It may be called on recovery reconnects.

    Returns [Stream_error] if the provider fails, or connection/protocol errors
    if the stream cannot be opened.

    NOTE: The provider is called once at stream creation; it is NOT called on
    recovery. For long-lived streams that need automatic token refresh, use
    {!create_stream} with client-credentials, which handles caching internally.
*)

val ingest : stream -> bytes -> (offset, Zerobus_core.Error.t) result Lwt.t
(** Queue one record for ingestion (does NOT wait for ack).

    Returns the offset immediately; the record is sent in the background. Wait
    later with {!wait_for_offset} or {!flush}.

    Never call this in a tight loop followed by {!wait_for_offset} — that
    serializes ingestion and loses orders of magnitude of throughput. Instead,
    queue in a loop and {!flush} once at the end.

    Returns [Stream_error] if the stream is closed or misused. *)

val ingest_records :
  stream -> bytes list -> (offset, Zerobus_core.Error.t) result Lwt.t
(** Queue a batch of records (does NOT wait for acks).

    Returns the offset of the last record. Same semantics as {!ingest}: use for
    hot paths; follow with {!flush} at the end, not per-record.

    Returns [Stream_error] if the stream is closed. *)

val wait_for_offset :
  stream -> offset -> (unit, Zerobus_core.Error.t) result Lwt.t
(** Wait until an offset is durably acked (and all prior offsets).

    The offset watermark is monotonic: waiting for offset N also waits for all
    offsets < N.

    Use sparingly — this is the slow-path. For normal ingestion, use {!flush}.
    Reserve per-record wait_for_offset for genuinely low-volume cases where each
    record must be confirmed durable before proceeding.

    Returns [Timeout] if the ack watchdog fires (server_lack_of_ack_timeout_ms),
    [Stream_error] if the stream encountered an error, or [Ok ()] when the
    offset is durable. *)

val flush : stream -> (unit, Zerobus_core.Error.t) result Lwt.t
(** Wait once for all pending records to be durable.

    The correct pattern for efficient ingestion:

    {[
      let* () =
        Lwt_list.iter_s (fun r -> ingest stream r >|= ignore) records
      in
      let* () = flush stream in
      ...
    ]}

    Returns [Timeout] if the flush deadline (flush_timeout_ms) is exceeded, or
    [Stream_error] if the stream encountered an error. *)

val close : stream -> (unit, Zerobus_core.Error.t) result Lwt.t
(** Close the stream.

    Half-closes the send side, flushes pending acks, and tears down the scope
    (cancels the ack-reader fiber). Idempotent: safe to call multiple times.

    After [close], the stream is no longer usable; further ingestion calls will
    return [Stream_error].

    Returns [Stream_error] if the stream was already closed (and encountered an
    error), or [Ok ()] on successful close. *)

(** {1 Low-level access (tests / advanced)} *)

module Io_lwt_for_test : module type of Zerobus_io_lwt
(** The Lwt {!Zerobus_core.Io.IO} instantiation — exposes [Scope], [Mailbox],
    the transport, etc. Prefer {!create}/{!create_stream}; this is for tests and
    advanced callers that drive the driver directly. *)

module Driver : module type of Zerobus_io_lwt.Stream
(** The Lwt streaming driver ([Zerobus_core.Make] applied to the Lwt IO):
    exposes [open_stream]/[ingest]/[flush]/[wait_for_offset]/[close] over the
    raw transport (e.g. to a cleartext-h2c mock without OAuth). *)
