(** Configuration utilities for SDK construction: endpoint derivation and OAuth
    token-request building (DESIGN.md §5.2, §12.2).

    Helpers for deriving the gRPC endpoint host from workspace metadata and
    building the OAuth token request body. These are pure functions intended for
    use by all per-runtime packages.
*)

(** Parse workspace-id from a Databricks workspace URL.

    Expects formats like:
    - https://adb-...-azuredatabricks.net
    - https://dbc-...-dbc.cloud.databricks.com
    - https://e2-...databricks.com (AWS)
    - https://adb-...azuredatabricks.net (Azure)
    - https://...gcp.databricks.com (GCP)

    Returns [Error] if parsing fails.
*)
let workspace_id_of_url (url : string) : (string, Error.t) result =
  try
    (* Remove protocol if present *)
    let url =
      if String.starts_with ~prefix:"https://" url then
        String.sub url 8 (String.length url - 8)
      else if String.starts_with ~prefix:"http://" url then
        String.sub url 7 (String.length url - 7)
      else
        url
    in
    (* Split on dots to extract components *)
    let parts = String.split_on_char '.' url in
    (* For most Databricks URLs, the workspace-id is the first part after "adb-" or
       similar prefix. For dbc-..., it's after "dbc-". Pattern: extract the numeric
       suffix from the first label. *)
    match parts with
    | [] -> Error (Error.Transport_error "empty workspace_url")
    | first :: _ ->
        (* Extract the numeric workspace id from the first host label. Databricks
           host labels prefix the id with a scheme tag, e.g. Azure "adb-<id>" (and
           AWS "dbc-<id>..."); the id is the first maximal digit run in the label.
           Returning the whole label (the old behavior) yielded e.g.
           "adb-984752964297111", which produced a wrong OAuth token audience
           ("Invalid token audience", grpc-status 16) on Azure. *)
        let digit c = Char.code c >= Char.code '0' && Char.code c <= Char.code '9' in
        let len = String.length first in
        let rec find_start i = if i >= len then -1 else if digit first.[i] then i else find_start (i + 1) in
        let s = find_start 0 in
        if s < 0 then
          Error (Error.Transport_error (Printf.sprintf "no workspace_id in URL: %s" url))
        else
          let rec find_end i = if i < len && digit first.[i] then find_end (i + 1) else i in
          let e = find_end s in
          Ok (String.sub first s (e - s))
  with
  | _ -> Error (Error.Transport_error "failed to parse workspace_url")

(** Derive the gRPC endpoint host from workspace URL and optional region.

    Given a workspace URL like https://adb-...eastus2.-azuredatabricks.net,
    derives the gRPC endpoint as <workspace-id>.zerobus.<region>.cloud.databricks.com
    (or the region-specific variant for Azure: <workspace-id>.zerobus.<region>.azuredatabricks.net).

    If [endpoint] is provided and is not empty, it is validated and returned as-is.
    Otherwise, the endpoint is derived from [workspace_url].

    Returns [(host, port)].
*)
let endpoint_of_workspace ~endpoint ~workspace_url : (string * int, Error.t) result =
  (* If endpoint is explicitly provided, use it *)
  if endpoint <> "" && endpoint <> "default" then begin
    (* Parse host:port or just host *)
    match String.split_on_char ':' endpoint with
    | [ host; port_str ] ->
        (try
          let port = int_of_string port_str in
          Ok (host, port)
        with _ ->
          Error (Error.Transport_error
            (Printf.sprintf "invalid port in endpoint: %s" endpoint)))
    | [ host ] -> Ok (host, 443)
    | _ -> Error (Error.Transport_error
        (Printf.sprintf "invalid endpoint format: %s" endpoint))
  end else begin
    (* Derive from workspace_url *)
    match workspace_id_of_url workspace_url with
    | Error _ as e -> e
    | Ok wsid ->

    (* Extract region from workspace_url. Common patterns:
       - eastus2, westus2, etc. (Azure)
       - us-west-2, us-east-1, etc. (AWS)
       - us-central1, etc. (GCP)
    *)
    let region =
      try
        let url =
          if String.starts_with ~prefix:"https://" workspace_url then
            String.sub workspace_url 8 (String.length workspace_url - 8)
          else
            workspace_url
        in
        let parts = String.split_on_char '.' url in
        (* Look for region-like parts: typically 2nd component after workspace-id *)
        match parts with
        | [ _first; second; _ ] -> Some second
        | _first :: second :: _ when String.length second > 0 -> Some second
        | _ -> None
      with _ -> None
    in

    let region = match region with Some r -> r | None -> "us-west-2" in
    (* NOTE: This assumes AWS cloud (.cloud.databricks.com). Azure endpoints use
       .azuredatabricks.net. For production use with non-AWS clouds, pass an
       explicit endpoint parameter instead of relying on derivation. *)
    let host = Printf.sprintf "%s.zerobus.%s.cloud.databricks.com" wsid region in
    Ok (host, 443)
  end

(** Build the OAuth 2.0 client-credentials token request body (DESIGN.md §12.2).

    Returns the URL-encoded request body for POST to <workspace>/oidc/v1/token.

    Parameters:
    - [workspace_id]: the Databricks workspace ID (e.g. "123456789")
    - [table]: the Unity Catalog table (e.g. "catalog.schema.table") to scope to
    - [client_id], [client_secret]: service principal credentials

    The body includes:
    - grant_type=client_credentials
    - scope=all-apis
    - resource=api://databricks/workspaces/<workspace_id>/zerobusDirectWriteApi
    - authorization_details: a UC-privilege JSON array for [catalog/schema/table]
*)
let oauth_token_request_body ~workspace_id ~table : (string, Error.t) result =
  try
    (* Parse table into catalog.schema.table *)
    let parts = String.split_on_char '.' table in
    let (cat, sch) = match parts with
      | [ c; s; _t ] -> (c, s)
      | _ -> raise (Failure "table must be catalog.schema.table")
    in

    (* Build authorization_details: UC privileges for the catalog, schema, and table *)
    let authorization_details =
      Printf.sprintf
        {|[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"%s"},{"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],"object_type":"SCHEMA","object_full_path":"%s.%s"},{"type":"unity_catalog_privileges","privileges":["MODIFY","SELECT"],"object_type":"TABLE","object_full_path":"%s"}]|}
        cat cat sch table
    in

    let resource =
      Printf.sprintf "api://databricks/workspaces/%s/zerobusDirectWriteApi" workspace_id
    in

    (* URL-encode the values *)
    let encode s = Uri.pct_encode ~component:`Query_value s in

    let body_parts = [
      "grant_type=client_credentials";
      "scope=all-apis";
      "resource=" ^ encode resource;
      "authorization_details=" ^ encode authorization_details;
    ] in

    Ok (String.concat "&" body_parts)
  with
  | Failure msg -> Error (Error.Auth_error msg)
  | _ -> Error (Error.Auth_error "failed to build token request body")
