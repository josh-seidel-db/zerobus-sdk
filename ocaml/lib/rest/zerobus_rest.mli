(** The stateless REST interface for the Zerobus Ingest SDK (DESIGN.md §4.2).

    A thin, optional helper for low-frequency / edge cases where the persistent
    gRPC stream ({!Zerobus}) is overkill: one HTTP [POST] per batch, no streams,
    no offsets, no recovery. The "throughput tax" tradeoff — high latency per
    call, but nothing to keep alive.

    {[
      POST <endpoint>/zerobus/v1/tables/<catalog>.<schema>.<table>/insert
      Authorization: Bearer <oauth-token>
      Content-Type: application/json
      [ {record}, {record}, ... ]
    ]}

    Reuses the same table-scoped client-credentials OAuth grant as {!Zerobus}
    (via [Zerobus_core.Config]); tokens are cached per table and refreshed ~30s
    before expiry.

    For high-volume producers, prefer the gRPC streaming {!Zerobus} interface —
    this REST path serializes each batch behind a full request/response and
    cannot pipeline. *)

(** A REST client. Holds workspace credentials, the derived REST endpoint, and a
    per-table OAuth token cache. Opaque; construct with {!create}. *)
type t

(** Errors are the shared SDK taxonomy. *)
type error = Zerobus_core.Error.t

(** Construct a REST client from workspace credentials.

    Parameters:
    - [application_name]: optional app name for diagnostics (reserved).
    - [endpoint]: the Zerobus REST base URL. If it carries a scheme
      ([http://]/[https://]) it is used verbatim (an override for edge
      deployments or a local test server). If empty or ["default"], the host is
      derived from [workspace_url] (same host derivation as the gRPC endpoint)
      and reached over https; a non-443 derived port is preserved.
    - [workspace_url]: the Databricks workspace URL (e.g.
      [https://my-workspace.azuredatabricks.net]); used to extract the
      workspace-id for the OAuth [resource] and to derive the endpoint.
    - [client_id], [client_secret]: service-principal credentials for the
      client-credentials grant.

    Returns [Auth_error] or [Transport_error] if the workspace URL cannot be
    parsed. *)
val create :
  ?application_name:string ->
  endpoint:string ->
  workspace_url:string ->
  client_id:string ->
  client_secret:string ->
  unit ->
  (t, error) result Lwt.t

(** Insert a batch of JSON records into [table] (catalog.schema.table).

    Mints (or reuses a cached) table-scoped token, then [POST]s the records as a
    JSON array to the [/insert] endpoint. Returns [Ok ()] on a 2xx response.

    An empty [records] list is a no-op ([Ok ()]) — no request is sent.

    Returns [Auth_error] on token-mint failure or a non-2xx from the token
    endpoint, [Server_status] on a non-2xx from the insert endpoint (carrying the
    HTTP code), or [Transport_error] on a network/connection failure. *)
val insert :
  t -> table:string -> Yojson.Safe.t list -> (unit, error) result Lwt.t
