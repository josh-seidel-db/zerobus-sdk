(** Configuration utilities for SDK construction (DESIGN.md §5.2, §12.2). *)

val workspace_id_of_url : string -> (string, Error.t) result
(** Parse workspace-id from a Databricks workspace URL.

    Handles standard Databricks URL formats across Azure, AWS, and GCP. Returns
    [Error] if the URL cannot be parsed. *)

val endpoint_of_workspace :
  endpoint:string -> workspace_url:string -> (string * int, Error.t) result
(** Derive gRPC endpoint (host, port) from workspace metadata.

    If [endpoint] is provided (non-empty, not "default"), it is parsed and
    returned as-is. Otherwise, the endpoint is derived from [workspace_url].

    Returns [(host, port)] where port defaults to 443. *)

val oauth_token_request_body :
  workspace_id:string -> table:string -> (string, Error.t) result
(** Build the OAuth 2.0 client-credentials token request body.

    Returns the URL-encoded request body for POST to <workspace>/oidc/v1/token.
    The body grants table-scoped access via UC authorization_details. The client
    id/secret are NOT in the body — they go in the HTTP Basic [Authorization]
    header (see the proven spike-live flow).

    Returns [Error.Auth_error] if the table is not in "catalog.schema.table"
    format. *)
