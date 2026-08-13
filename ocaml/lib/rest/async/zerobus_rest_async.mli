(** The stateless REST interface for the Zerobus Ingest SDK — Async runtime
    (DESIGN.md §4.2).

    The Async counterpart of {!Zerobus_rest} (the Lwt reference): a thin,
    optional helper for low-frequency / edge cases where the persistent gRPC
    stream ({!Zerobus_async}) is overkill. One HTTP [POST] per batch, no
    streams, no offsets, no recovery.

    {[
      POST <endpoint>/zerobus/v1/tables/<catalog>.<schema>.<table>/insert
      Authorization: Bearer <oauth-token>
      Content-Type: application/json
      [ {record}, {record}, ... ]
    ]}

    Reuses the same table-scoped client-credentials OAuth grant as {!Zerobus}
    (via [Zerobus_core.Config]); tokens are cached per table and refreshed ~30s
    before expiry.

    This library is [optional] — it builds only on a switch with [cohttp-async]
    installed. For high-volume producers, prefer the gRPC streaming
    {!Zerobus_async} interface. *)

open! Async

type t
(** A REST client. Holds workspace credentials, the derived REST endpoint, and a
    per-table OAuth token cache. Opaque; construct with {!create}. *)

type error = Zerobus_core.Error.t
(** Errors are the shared SDK taxonomy. *)

val create :
  ?application_name:string ->
  endpoint:string ->
  workspace_url:string ->
  client_id:string ->
  client_secret:string ->
  unit ->
  (t, error) result
(** Construct a REST client from workspace credentials. Identical semantics to
    {!Zerobus_rest.create}:
    - [application_name]: optional app name for diagnostics (reserved).
    - [endpoint]: the Zerobus REST base URL. If it carries a scheme
      ([http://]/[https://]) it is used verbatim (an override for edge
      deployments or a local test server). If empty or ["default"], the host is
      derived from [workspace_url] (same host derivation as the gRPC endpoint)
      and reached over https; a non-443 derived port is preserved.
    - [workspace_url]: the Databricks workspace URL; used to extract the
      workspace-id for the OAuth [resource] and to derive the endpoint.
    - [client_id], [client_secret]: service-principal credentials.

    Returns [Auth_error] or [Transport_error] if the workspace URL cannot be
    parsed. This is a pure computation (no I/O) — direct return, no [Deferred].
*)

val insert :
  t -> table:string -> Yojson.Safe.t list -> (unit, error) result Deferred.t
(** Insert a batch of JSON records into [table] (catalog.schema.table).

    Mints (or reuses a cached) table-scoped token, then [POST]s the records as a
    JSON array to the [/insert] endpoint. Returns [Ok ()] on a 2xx response.

    An empty [records] list is a no-op ([Ok ()]) — no request is sent.

    Returns [Auth_error] on token-mint failure or a non-2xx from the token
    endpoint, [Server_status] on a non-2xx from the insert endpoint (carrying
    the HTTP code), or [Transport_error] on a network/connection failure. *)
