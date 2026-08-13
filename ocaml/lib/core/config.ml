(** Configuration utilities for SDK construction: endpoint derivation and OAuth
    token-request building (DESIGN.md §5.2, §12.2).

    Helpers for deriving the gRPC endpoint host from workspace metadata and
    building the OAuth token request body. These are pure functions intended for
    use by all per-runtime packages. *)

(** Parse workspace-id from a Databricks workspace URL.

    Expects formats like:
    - https://adb-...-azuredatabricks.net
    - https://dbc-...-dbc.cloud.databricks.com
    - https://e2-...databricks.com (AWS)
    - https://adb-...azuredatabricks.net (Azure)
    - https://...gcp.databricks.com (GCP)

    Returns [Error] if parsing fails. *)
let workspace_id_of_url (url : string) : (string, Error.t) result =
  try
    (* Remove protocol if present *)
    let url =
      if String.starts_with ~prefix:"https://" url then
        String.sub url 8 (String.length url - 8)
      else if String.starts_with ~prefix:"http://" url then
        String.sub url 7 (String.length url - 7)
      else url
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
        let digit c =
          Char.code c >= Char.code '0' && Char.code c <= Char.code '9'
        in
        let len = String.length first in
        let rec find_start i =
          if i >= len then -1
          else if digit first.[i] then i
          else find_start (i + 1)
        in
        let s = find_start 0 in
        if s < 0 then
          Error
            (Error.Transport_error
               (Printf.sprintf "no workspace_id in URL: %s" url))
        else
          let rec find_end i =
            if i < len && digit first.[i] then find_end (i + 1) else i
          in
          let e = find_end s in
          Ok (String.sub first s (e - s))
  with _ -> Error (Error.Transport_error "failed to parse workspace_url")

(* Strip a leading scheme and any trailing path/port, returning the bare host. *)
let host_of_url (url : string) : string =
  let s =
    if String.starts_with ~prefix:"https://" url then
      String.sub url 8 (String.length url - 8)
    else if String.starts_with ~prefix:"http://" url then
      String.sub url 7 (String.length url - 7)
    else url
  in
  (* drop path and port *)
  let s =
    match String.index_opt s '/' with Some i -> String.sub s 0 i | None -> s
  in
  match String.index_opt s ':' with Some i -> String.sub s 0 i | None -> s

(* The three Databricks clouds and their Zerobus endpoint domain suffixes. The
   Zerobus gRPC host is [<workspace-id>.zerobus.<region>.<suffix>]. *)
type cloud = AWS | Azure | GCP

let ends_with host suf =
  let ls = String.length suf and lh = String.length host in
  lh >= ls && String.sub host (lh - ls) ls = suf

let cloud_of_host host : cloud option =
  (* Order matters: the more specific GCP suffix before AWS's cloud.databricks.com. *)
  if ends_with host ".gcp.databricks.com" then Some GCP
  else if ends_with host ".azuredatabricks.net" then Some Azure
  else if ends_with host ".cloud.databricks.com" then Some AWS
  else None

let zerobus_suffix = function
  | AWS -> "cloud.databricks.com"
  | Azure -> "azuredatabricks.net"
  | GCP -> "gcp.databricks.com"

let cloud_name = function AWS -> "AWS" | Azure -> "Azure" | GCP -> "GCP"

(* Pull the region out of a host IFF it is already a zerobus-form host, i.e.
   [<id>.zerobus.<region>.<suffix>...]. Region is NOT present in a plain console
   workspace host (AWS [dbc-xxx.cloud.databricks.com], Azure
   [adb-<id>.<n>.azuredatabricks.net]) — those carry no region, which is why the
   reference SDKs require an explicit endpoint. Returns [Some region] only when the
   [.zerobus.] marker is present. *)
let region_of_zerobus_host host : string option =
  let parts = String.split_on_char '.' host in
  let rec find = function
    | "zerobus" :: region :: _ when String.length region > 0 -> Some region
    | _ :: rest -> find rest
    | [] -> None
  in
  find parts

(** Derive the gRPC endpoint [(host, port)] from an explicit [endpoint] or from
    [workspace_url].

    - If [endpoint] is a non-empty, non-["default"] value it is used verbatim
      (host or host:port) — the reliable path, matching every other Zerobus SDK,
      which require an explicit endpoint.
    - Otherwise we DERIVE. The Zerobus host is
      [<workspace-id>.zerobus.<region>.<cloud-suffix>] with suffix
      [cloud.databricks.com] (AWS), [azuredatabricks.net] (Azure), or
      [gcp.databricks.com] (GCP), detected from [workspace_url]. Derivation
      needs a REGION, which a plain console workspace URL usually does NOT
      contain; we can only recover it when [workspace_url] is itself already a
      zerobus-form host. If the cloud is unknown or the region can't be
      determined, we return a precise [Transport_error] telling the caller the
      exact template to pass as [endpoint] — NEVER a silently-wrong host (the
      previous bug, which hard-coded AWS and mis- picked the region for
      Azure/GCP). *)
let endpoint_of_workspace ~endpoint ~workspace_url :
    (string * int, Error.t) result =
  if endpoint <> "" && endpoint <> "default" then
    begin match String.split_on_char ':' endpoint with
    | [ host; port_str ] -> (
        try Ok (host, int_of_string port_str)
        with _ ->
          Error
            (Error.Transport_error
               (Printf.sprintf "invalid port in endpoint: %s" endpoint)))
    | [ host ] -> Ok (host, 443)
    | _ ->
        Error
          (Error.Transport_error
             (Printf.sprintf "invalid endpoint format: %s" endpoint))
    end
  else
    begin match workspace_id_of_url workspace_url with
    | Error _ as e -> e
    | Ok wsid -> (
        let host = host_of_url workspace_url in
        (* If the workspace_url is already a zerobus host, use it directly. *)
        if region_of_zerobus_host host <> None then Ok (host, 443)
        else
          match cloud_of_host host with
          | None ->
              Error
                (Error.Transport_error
                   (Printf.sprintf
                      "cannot derive a Zerobus endpoint from workspace_url %S \
                       (unrecognized cloud). Pass an explicit endpoint like \
                       <workspace-id>.zerobus.<region>.<cloud-suffix>."
                      workspace_url))
          | Some cloud -> (
              match region_of_zerobus_host host with
              | Some region ->
                  Ok
                    ( Printf.sprintf "%s.zerobus.%s.%s" wsid region
                        (zerobus_suffix cloud),
                      443 )
              | None ->
                  (* Region absent from a console workspace URL — do NOT guess. *)
                  Error
                    (Error.Transport_error
                       (Printf.sprintf
                          "cannot derive the %s Zerobus endpoint from \
                           workspace_url %S: the region is not present in the \
                           workspace URL. Pass an explicit endpoint: \
                           %s.zerobus.<region>.%s"
                          (cloud_name cloud) workspace_url wsid
                          (zerobus_suffix cloud)))))
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
    - authorization_details: a UC-privilege JSON array for
      [catalog/schema/table] *)
let oauth_token_request_body ~workspace_id ~table : (string, Error.t) result =
  try
    (* Parse table into catalog.schema.table *)
    let parts = String.split_on_char '.' table in
    let cat, sch =
      match parts with
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
      Printf.sprintf "api://databricks/workspaces/%s/zerobusDirectWriteApi"
        workspace_id
    in

    (* URL-encode the values *)
    let encode s = Uri.pct_encode ~component:`Query_value s in

    let body_parts =
      [
        "grant_type=client_credentials";
        "scope=all-apis";
        "resource=" ^ encode resource;
        "authorization_details=" ^ encode authorization_details;
      ]
    in

    Ok (String.concat "&" body_parts)
  with
  | Failure msg -> Error (Error.Auth_error msg)
  | _ -> Error (Error.Auth_error "failed to build token request body")
