(** The stateless REST interface for the Zerobus Ingest SDK (DESIGN.md §4.2).

    See {!Zerobus_rest} (the .mli) for the API contract. This reuses
    [Zerobus_core.Config.oauth_token_request_body] (the same table-scoped
    client-credentials grant the gRPC SDK uses, proven live in spike-live) and
    the cohttp-lwt HTTP client. No streams, no offsets, no recovery. *)

module Config = Zerobus_core.Config
module Error = Zerobus_core.Error

let ( let* ) = Lwt.bind

type error = Error.t

(* Per-table cached token: (access_token, expiry_epoch_seconds). *)
type token_entry = string * float

type t = {
  workspace_url : string;
  workspace_id : string;
  base_url : string;
      (* e.g. https://<wsid>.zerobus.<region>....  (no trailing /) *)
  application_name : string option; [@ocaml.warning "-69"]
  client_id : string;
  client_secret : string;
  (* Token cache keyed by table name; each table has its own scope, so tokens
     are not interchangeable across tables. *)
  mutable token_cache : (string * token_entry) list;
  token_cache_lock : Lwt_mutex.t;
}

let _ = fun (x : t) -> ignore x.application_name

(* Derive the REST base URL (no trailing slash). The REST host is the same as the
   gRPC endpoint host (per DESIGN §4.2), reached over https.

   - An explicit [endpoint] carrying a scheme ([http://] / [https://]) is used
     verbatim (an override for edge deployments and for tests against a local
     cleartext mock).
   - Otherwise the (host, port) from the shared gRPC derivation is used over
     https; the port is kept only when it is non-default (443), so the common
     case stays a bare [https://<host>]. (Reusing the gRPC derivation but never
     dropping a non-443 port.) *)
let base_url_of ~endpoint ~workspace_url : (string, Error.t) result =
  if
    String.length endpoint > 0
    && (String.starts_with ~prefix:"http://" endpoint
       || String.starts_with ~prefix:"https://" endpoint)
  then
    (* Verbatim override; strip any trailing slash for clean path joining. *)
    let e =
      if
        String.length endpoint > 0
        && endpoint.[String.length endpoint - 1] = '/'
      then String.sub endpoint 0 (String.length endpoint - 1)
      else endpoint
    in
    Ok e
  else
    match Config.endpoint_of_workspace ~endpoint ~workspace_url with
    | Error _ as e -> e
    | Ok (host, port) ->
        if port = 443 then Ok ("https://" ^ host)
        else Ok (Printf.sprintf "https://%s:%d" host port)

let create ?(application_name : string option) ~endpoint ~workspace_url
    ~client_id ~client_secret () : (t, error) result Lwt.t =
  match Config.workspace_id_of_url workspace_url with
  | Error e -> Lwt.return (Error e)
  | Ok workspace_id -> (
      match base_url_of ~endpoint ~workspace_url with
      | Error e -> Lwt.return (Error e)
      | Ok base_url ->
          Lwt.return
            (Ok
               {
                 workspace_url;
                 workspace_id;
                 base_url;
                 application_name;
                 client_id;
                 client_secret;
                 token_cache = [];
                 token_cache_lock = Lwt_mutex.create ();
               }))

(* Mint (or reuse a cached) table-scoped token. Mirrors Zerobus.mint_token: a
   client-credentials grant with UC authorization_details, cached with a 30s
   early-refresh buffer. Keyed by table because each token is table-scoped. *)
let mint_token t ~table : (string, error) result Lwt.t =
  Lwt_mutex.with_lock t.token_cache_lock (fun () ->
      let now = Unix.gettimeofday () in
      match List.assoc_opt table t.token_cache with
      | Some (token, expiry) when now +. 30.0 < expiry -> Lwt.return (Ok token)
      | _ -> (
          match
            Config.oauth_token_request_body ~workspace_id:t.workspace_id ~table
          with
          | Error e -> Lwt.return (Error e)
          | Ok body ->
              let token_url = t.workspace_url ^ "/oidc/v1/token" in
              let basic =
                Base64.encode_string (t.client_id ^ ":" ^ t.client_secret)
              in
              let headers =
                Cohttp.Header.of_list
                  [
                    ("Content-Type", "application/x-www-form-urlencoded");
                    ("Authorization", "Basic " ^ basic);
                  ]
              in
              Lwt.catch
                (fun () ->
                  let* resp, resp_body =
                    Cohttp_lwt_unix.Client.post ~headers
                      ~body:(Cohttp_lwt.Body.of_string body)
                      (Uri.of_string token_url)
                  in
                  let status =
                    Cohttp.Response.status resp |> Cohttp.Code.code_of_status
                  in
                  let* resp_str = Cohttp_lwt.Body.to_string resp_body in
                  if status < 200 || status >= 300 then
                    Lwt.return
                      (Error
                         (Error.Auth_error
                            (Printf.sprintf "token endpoint HTTP %d" status)))
                  else
                    try
                      let json = Yojson.Safe.from_string resp_str in
                      let field k =
                        match json with
                        | `Assoc l -> List.assoc_opt k l
                        | _ -> None
                      in
                      let token =
                        match field "access_token" with
                        | Some (`String s) -> Some s
                        | _ -> None
                      in
                      let expires_in =
                        match field "expires_in" with
                        | Some (`Int n) -> float_of_int n
                        | Some (`Float f) -> f
                        | Some (`String s) -> (
                            try float_of_string s with _ -> 3600.0)
                        | _ -> 3600.0
                      in
                      match token with
                      | Some tok ->
                          let expiry = now +. expires_in in
                          t.token_cache <-
                            (table, (tok, expiry))
                            :: List.remove_assoc table t.token_cache;
                          Lwt.return (Ok tok)
                      | None ->
                          Lwt.return
                            (Error
                               (Error.Auth_error "no access_token in response"))
                    with _ ->
                      Lwt.return
                        (Error (Error.Auth_error "malformed token response")))
                (fun exn ->
                  Lwt.return
                    (Error
                       (Error.Transport_error
                          (Printf.sprintf "token request failed: %s"
                             (Printexc.to_string exn)))))))

let insert t ~table (records : Yojson.Safe.t list) : (unit, error) result Lwt.t
    =
  match records with
  | [] -> Lwt.return (Ok ())
  | _ -> (
      let* token_result = mint_token t ~table in
      match token_result with
      | Error e -> Lwt.return (Error e)
      | Ok token ->
          let url =
            Printf.sprintf "%s/zerobus/v1/tables/%s/insert" t.base_url table
          in
          let headers =
            Cohttp.Header.of_list
              [
                ("Authorization", "Bearer " ^ token);
                ("Content-Type", "application/json");
              ]
          in
          let body = Yojson.Safe.to_string (`List records) in
          Lwt.catch
            (fun () ->
              let* resp, resp_body =
                Cohttp_lwt_unix.Client.post ~headers
                  ~body:(Cohttp_lwt.Body.of_string body)
                  (Uri.of_string url)
              in
              let status =
                Cohttp.Response.status resp |> Cohttp.Code.code_of_status
              in
              (* Drain the body so the connection can be reused/closed cleanly. *)
              let* body_str = Cohttp_lwt.Body.to_string resp_body in
              if status >= 200 && status < 300 then Lwt.return (Ok ())
              else
                Lwt.return
                  (Error
                     (Error.Server_status
                        {
                          code = status;
                          message =
                            String.sub body_str 0
                              (min 256 (String.length body_str));
                        })))
            (fun exn ->
              Lwt.return
                (Error
                   (Error.Transport_error
                      (Printf.sprintf "insert request failed: %s"
                         (Printexc.to_string exn))))))
