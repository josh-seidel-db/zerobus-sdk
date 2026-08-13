(** The OTLP (OpenTelemetry) exporter for the Zerobus Ingest SDK — Async runtime
    (DESIGN.md §4.3).

    The Async counterpart of {!Zerobus_otlp} (the Lwt reference): an [otlp/grpc]
    exporter for callers already emitting OpenTelemetry logs and metrics. It
    ships them to the Zerobus OTLP endpoint via the two unary RPCs
    - [opentelemetry.proto.collector.logs.v1.LogsService/Export]
    - [opentelemetry.proto.collector.metrics.v1.MetricsService/Export] over the
      {b same} TLS 1.3 + ALPN-h2 transport the gRPC streaming SDK uses (reusing
      {!Zerobus_async}'s Async [H2_client]).

    A unary Export is a degenerate bidi call: send one framed request,
    half-close, read one framed response, then the gRPC status. Unlike
    {!Zerobus_async} there is no stream, no offset, no recovery — each
    [export_*] is one request/response.

    The OTLP wire types come from the shared [Zerobus_otlp_proto] library (the
    same vendored OpenTelemetry collector protos the Lwt exporter uses).

    Auth reuses the same table-scoped client-credentials grant as
    {!Zerobus_async} (via [Zerobus_core.Config]); tokens are cached and
    refreshed ~30s before expiry.

    This library is [optional] — it builds only on a switch with [cohttp-async]
    installed. *)

open! Async

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
  (t, error) result
(** Construct an OTLP exporter. Identical semantics to {!Zerobus_otlp.create}
    (see there for the parameter contract). This is a pure computation (no I/O)
    — direct return, no [Deferred]. *)

val export_logs :
  t ->
  Zerobus_otlp_proto.Logs.resource_logs list ->
  (export_result, error) result Deferred.t
(** Export a batch of OpenTelemetry logs via [LogsService/Export].

    An empty list is a no-op ([Ok] with no rejections). Returns [Server_status]
    if the collector returns a non-OK gRPC status, [Transport_error] on a
    connection failure, or [Auth_error] on token-mint failure. A 2xx with a
    [partial_success] is returned as [Ok] with [rejected] > 0. *)

val export_metrics :
  t ->
  Zerobus_otlp_proto.Metrics.resource_metrics list ->
  (export_result, error) result Deferred.t
(** Export a batch of OpenTelemetry metrics via [MetricsService/Export].

    Same semantics as {!export_logs}. *)
