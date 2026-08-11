(** Live spike — DESIGN.md §12.2: native OCaml scoped-OAuth token acquisition.

    Reproduces, in pure OCaml over [cohttp-lwt-unix] (TLS via the same tls-lwt
    backend), the exact client-credentials grant Zerobus requires:
      - endpoint  <workspace>/oidc/v1/token
      - scope=all-apis
      - resource=api://databricks/workspaces/<workspace_id>/zerobusDirectWriteApi
      - authorization_details = UC-privilege JSON array scoping to the target
        catalog / schema / table (USE CATALOG / USE SCHEMA / MODIFY+SELECT).

    This is the token-body builder that §12.2 says goes in zerobus-core, exercised
    against the live OIDC endpoint. It asserts a scoped access_token comes back.

    Credentials come from env (never hard-coded / never logged):
      DATABRICKS_HOST          e.g. https://adb-...azuredatabricks.net
      ZEROBUS_WORKSPACE_ID     e.g. 984752964297111
      ZEROBUS_CLIENT_ID        service-principal application id
      ZEROBUS_CLIENT_SECRET    service-principal oauth secret
      ZEROBUS_TABLE            catalog.schema.table  (already granted to the SP)

    Exit 0 = proven; non-zero = failure with a (secret-free) diagnostic. *)

let env k = try Sys.getenv k with Not_found -> failwith ("missing env: " ^ k)

(* §12.2 authorization_details builder — the pure part destined for zerobus-core. *)
let authorization_details ~table =
  match String.split_on_char '.' table with
  | [ cat; sch; _tbl ] ->
      Printf.sprintf
        {|[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"%s"},{"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],"object_type":"SCHEMA","object_full_path":"%s.%s"},{"type":"unity_catalog_privileges","privileges":["MODIFY","SELECT"],"object_type":"TABLE","object_full_path":"%s"}]|}
        cat cat sch table
  | _ -> failwith ("ZEROBUS_TABLE must be catalog.schema.table, got: " ^ table)

let () =
  Lwt_main.run
    (let open Lwt.Syntax in
     let host = env "DATABRICKS_HOST" in
     let wsid = env "ZEROBUS_WORKSPACE_ID" in
     let client_id = env "ZEROBUS_CLIENT_ID" in
     let client_secret = env "ZEROBUS_CLIENT_SECRET" in
     let table = env "ZEROBUS_TABLE" in

     Printf.printf "== §12.2 scoped-OAuth spike ==\nhost : %s\ntable: %s\n%!" host table;

     let token_url = host ^ "/oidc/v1/token" in
     let resource =
       Printf.sprintf "api://databricks/workspaces/%s/zerobusDirectWriteApi" wsid
     in
     let ad = authorization_details ~table in
     (* application/x-www-form-urlencoded body; values pct-encoded. *)
     let body_str =
       String.concat "&"
         [ "grant_type=client_credentials";
           "scope=all-apis";
           "resource=" ^ Uri.pct_encode ~component:`Query_value resource;
           "authorization_details=" ^ Uri.pct_encode ~component:`Query_value ad ]
     in
     (* HTTP Basic client auth (client_id:client_secret), per the token endpoint. *)
     let basic =
       Base64.encode_string (client_id ^ ":" ^ client_secret)
     in
     let headers =
       Cohttp.Header.of_list
         [ ("Content-Type", "application/x-www-form-urlencoded");
           ("Authorization", "Basic " ^ basic) ]
     in
     let* resp, body =
       Cohttp_lwt_unix.Client.post
         ~headers
         ~body:(Cohttp_lwt.Body.of_string body_str)
         (Uri.of_string token_url)
     in
     let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
     let* body_str = Cohttp_lwt.Body.to_string body in
     Printf.printf "http: %d from %s\n%!" status token_url;

     if status < 200 || status >= 300 then begin
       (* body may contain error info but not our secret; still keep it short *)
       Printf.printf "RESULT: FAIL — token endpoint returned HTTP %d: %s\n%!"
         status (String.sub body_str 0 (min 200 (String.length body_str)));
       exit 1
     end;

     let json = Yojson.Safe.from_string body_str in
     let field k =
       match json with
       | `Assoc l -> List.assoc_opt k l
       | _ -> None
     in
     let token_len =
       match field "access_token" with Some (`String t) -> String.length t | _ -> 0
     in
     let expires_in =
       match field "expires_in" with
       | Some (`Int n) -> n
       | Some (`String s) -> (try int_of_string s with _ -> 0)
       | _ -> 0
     in
     let scope =
       match field "scope" with Some (`String s) -> s | _ -> "(none)" in

     let t = Unix.gmtime (Unix.time ()) in
     Printf.printf
       "\n-- EVIDENCE (§12.2) --\n\
        timestamp_utc  : %04d-%02d-%02dT%02d:%02d:%02dZ\n\
        token_endpoint : %s\n\
        scope          : %s\n\
        resource       : %s\n\
        authorization_details: 3 UC entries (CATALOG/SCHEMA/TABLE) for %s\n\
        access_token   : received, length %d (value NOT logged)\n\
        expires_in     : %d s\n%!"
       (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min t.tm_sec
       token_url scope resource table token_len expires_in;

     if token_len > 0 then begin
       Printf.printf
         "RESULT: PASS — native OCaml obtained a table-scoped Zerobus token.\n%!";
       Lwt.return_unit
     end
     else begin
       Printf.printf "RESULT: FAIL — no access_token in response\n%!";
       exit 1
     end)
