(** The OTLP (OpenTelemetry) exporter for the Zerobus Ingest SDK (DESIGN.md
    §4.3).

    An [otlp/grpc] exporter for callers already emitting OpenTelemetry logs and
    metrics: it ships them to the Zerobus OTLP endpoint via the two unary RPCs
    - [opentelemetry.proto.collector.logs.v1.LogsService/Export]
    - [opentelemetry.proto.collector.metrics.v1.MetricsService/Export] over the
      {b same} TLS 1.3 + ALPN-h2 transport the gRPC streaming SDK uses (reusing
      [Zerobus]'s Lwt [H2_client]).

    A unary Export is a degenerate bidi call: send one framed request,
    half-close, read one framed response, then the gRPC status. Unlike
    {!Zerobus} there is no stream, no offset, no recovery — each [export_*] is
    one request/response.

    The OTLP wire types (the OpenTelemetry collector protos, vendored
    independently of the Zerobus protos) are in the [Zerobus_otlp_proto]
    library: build a [Logs.resource_logs list] / [Metrics.resource_metrics list]
    with its generated [make_*] builders and hand them to {!export_logs} /
    {!export_metrics}.

    Auth reuses the same table-scoped client-credentials grant as {!Zerobus}
    (via [Zerobus_core.Config]); tokens are cached and refreshed ~30s before
    expiry. *)

type t
(** An OTLP exporter client. Holds the endpoint, credentials, and a token cache.
    Opaque; construct with {!create}. *)

type error = Zerobus_core.Error.t
(** Errors are the shared SDK taxonomy. *)

type export_result = { rejected : int64; error_message : string }
(** The outcome of an Export: whether the collector reported a partial success,
    and how many records it rejected (0 when fully accepted). *)

val create :
  ?application_name:string ->
  ?tls:bool ->
  endpoint:string ->
  workspace_url:string ->
  table:string ->
  client_id:string ->
  client_secret:string ->
  unit ->
  (t, error) result Lwt.t
(** Construct an OTLP exporter.

    Parameters:
    - [application_name]: optional app name for diagnostics (reserved).
    - [endpoint]: the Zerobus gRPC/OTLP endpoint ([host] or [host:port]). If
      empty or ["default"], it is derived from [workspace_url] (same host
      derivation as the gRPC endpoint; port defaults to 443).
    - [workspace_url]: the Databricks workspace URL; used for the OAuth
      [resource] and endpoint derivation.
    - [table]: the target Unity Catalog table ([catalog.schema.table]) the OTLP
      records land in — used to scope the OAuth token and stamped as the
      table-name metadata header.
    - [client_id], [client_secret]: service-principal credentials.
    - [tls]: negotiate TLS 1.3 + ALPN-h2 (default [true]); [false] uses
      cleartext h2c (loopback mock only — a bearer token is never sent over
      cleartext).

    Returns [Auth_error] or [Transport_error] if the workspace URL cannot be
    parsed. *)

val export_logs :
  t ->
  Zerobus_otlp_proto.Logs.resource_logs list ->
  (export_result, error) result Lwt.t
(** Export a batch of OpenTelemetry logs via [LogsService/Export].

    An empty list is a no-op ([Ok] with no rejections). Returns [Server_status]
    if the collector returns a non-OK gRPC status, [Transport_error] on a
    connection failure, or [Auth_error] on token-mint failure. A 2xx with a
    [partial_success] is returned as [Ok] with [rejected] > 0. *)

val export_metrics :
  t ->
  Zerobus_otlp_proto.Metrics.resource_metrics list ->
  (export_result, error) result Lwt.t
(** Export a batch of OpenTelemetry metrics via [MetricsService/Export].

    Same semantics as {!export_logs}. *)
