(** The stateless REST interface for the Zerobus Ingest SDK — Eio runtime
    (DESIGN.md §4.2).

    The Eio (direct-style) counterpart of {!Zerobus_rest} (the Lwt reference).
    Same contract — a table-scoped client-credentials OAuth grant (reusing
    [Zerobus_core.Config.oauth_token_request_body]) plus one HTTP [POST] per
    batch — but built on [cohttp-eio] over a real TLS connection (system trust
    store), mirroring [Zerobus_eio.mint_token]. No streams, no offsets, no
    recovery.

    See {!Zerobus_rest_eio} (the .mli) for the API contract. *)

module Config = Zerobus_core.Config
module Error = Zerobus_core.Error

type error = Error.t

(* Per-table cached token: (access_token, expiry_epoch_seconds). *)
type token_entry = string * float

type t = {
  workspace_url : string;
  workspace_id : string;
  base_url : string;  (* e.g. https://<wsid>.zerobus.<region>....  (no trailing /) *)
  application_name : string option [@ocaml.warning "-69"];
  client_id : string;
  client_secret : string;
  (* Token cache keyed by table name; each table has its own scope, so tokens
     are not interchangeable across tables. Eio fibers on one domain are
     cooperatively scheduled; the mutex guards the cache across the HTTP round
     trip's suspension points. *)
  mutable token_cache : (string * token_entry) list;
  token_cache_lock : Eio.Mutex.t;
}

let _ = fun (x : t) -> ignore x.application_name

(* Derive the REST base URL (no trailing slash). Identical policy to the Lwt
   {!Zerobus_rest.base_url_of}: a scheme-carrying [endpoint] is used verbatim (an
   override for edge deployments / a local cleartext mock); otherwise the shared
   gRPC host derivation is used over https, keeping a non-443 port. *)
let base_url_of ~endpoint ~workspace_url : (string, Error.t) result =
  if
    String.length endpoint > 0
    && (String.starts_with ~prefix:"http://" endpoint
       || String.starts_with ~prefix:"https://" endpoint)
  then
    let e =
      if String.length endpoint > 0 && endpoint.[String.length endpoint - 1] = '/'
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
    ~client_id ~client_secret () : (t, error) result =
  match Config.workspace_id_of_url workspace_url with
  | Error e -> Error e
  | Ok workspace_id -> (
      match base_url_of ~endpoint ~workspace_url with
      | Error e -> Error e
      | Ok base_url ->
          Ok
            {
              workspace_url;
              workspace_id;
              base_url;
              application_name;
              client_id;
              client_secret;
              token_cache = [];
              token_cache_lock = Eio.Mutex.create ();
            })

(* Build a cohttp-eio client whose TLS layer uses the system trust store. The
   [https] callback is only invoked for https:// URIs, so a cleartext http://
   endpoint (the test mock) sidesteps TLS entirely. Mirrors
   Zerobus_eio.mint_token's https setup. *)
let make_httpc env =
  Mirage_crypto_rng_unix.use_default ();
  let authenticator =
    match Ca_certs.authenticator () with
    | Ok a -> a
    | Error (`Msg m) -> failwith ("ca-certs: " ^ m)
  in
  let https =
    let cfg =
      match Tls.Config.client ~authenticator () with
      | Ok c -> c
      | Error (`Msg m) -> failwith ("tls config: " ^ m)
    in
    fun uri raw ->
      let host =
        Uri.host uri
        |> Option.map (fun h -> Domain_name.(host_exn (of_string_exn h)))
      in
      Tls_eio.client_of_flow ?host cfg raw
  in
  Cohttp_eio.Client.make ~https:(Some https) (Eio.Stdenv.net env)

(* Mint (or reuse a cached) table-scoped token. Same client-credentials grant as
   the Lwt {!Zerobus_rest.mint_token}: UC authorization_details in the form body,
   id/secret in the HTTP Basic header, cached per table with a 30s early-refresh
   buffer. *)
let mint_token ~env ~sw t ~table : (string, error) result =
  Eio.Mutex.use_rw ~protect:true t.token_cache_lock (fun () ->
      let now = Unix.gettimeofday () in
      match List.assoc_opt table t.token_cache with
      | Some (token, expiry) when now +. 30.0 < expiry -> Ok token
      | _ -> (
          match
            Config.oauth_token_request_body ~workspace_id:t.workspace_id ~table
          with
          | Error e -> Error e
          | Ok body -> (
              try
                let httpc = make_httpc env in
                let basic =
                  Base64.encode_string (t.client_id ^ ":" ^ t.client_secret)
                in
                let headers =
                  Http.Header.of_list
                    [
                      ("Content-Type", "application/x-www-form-urlencoded");
                      ("Authorization", "Basic " ^ basic);
                    ]
                in
                let uri = Uri.of_string (t.workspace_url ^ "/oidc/v1/token") in
                let resp, rbody =
                  Cohttp_eio.Client.post ~sw httpc ~headers
                    ~body:(Cohttp_eio.Body.of_string body) uri
                in
                let resp_str =
                  Eio.Buf_read.(parse_exn take_all) rbody ~max_size:max_int
                in
                let status = Http.Status.to_int resp.Http.Response.status in
                if status < 200 || status >= 300 then
                  Error
                    (Error.Auth_error
                       (Printf.sprintf "token endpoint HTTP %d" status))
                else
                  match Yojson.Safe.from_string resp_str with
                  | `Assoc l -> (
                      let token =
                        match List.assoc_opt "access_token" l with
                        | Some (`String s) -> Some s
                        | _ -> None
                      in
                      let expires_in =
                        match List.assoc_opt "expires_in" l with
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
                          Ok tok
                      | None ->
                          Error (Error.Auth_error "no access_token in response"))
                  | _ -> Error (Error.Auth_error "malformed token response")
              with exn ->
                Error
                  (Error.Transport_error
                     (Printf.sprintf "token request failed: %s"
                        (Printexc.to_string exn))))))

let insert ~env ~sw t ~table (records : Yojson.Safe.t list) :
    (unit, error) result =
  match records with
  | [] -> Ok ()
  | _ -> (
      match mint_token ~env ~sw t ~table with
      | Error e -> Error e
      | Ok token -> (
          let url =
            Printf.sprintf "%s/zerobus/v1/tables/%s/insert" t.base_url table
          in
          let headers =
            Http.Header.of_list
              [
                ("Authorization", "Bearer " ^ token);
                ("Content-Type", "application/json");
              ]
          in
          let body = Yojson.Safe.to_string (`List records) in
          try
            let httpc = make_httpc env in
            let resp, rbody =
              Cohttp_eio.Client.post ~sw httpc ~headers
                ~body:(Cohttp_eio.Body.of_string body) (Uri.of_string url)
            in
            let body_str =
              Eio.Buf_read.(parse_exn take_all) rbody ~max_size:max_int
            in
            let status = Http.Status.to_int resp.Http.Response.status in
            if status >= 200 && status < 300 then Ok ()
            else
              Error
                (Error.Server_status
                   {
                     code = status;
                     message =
                       String.sub body_str 0 (min 256 (String.length body_str));
                   })
          with exn ->
            Error
              (Error.Transport_error
                 (Printf.sprintf "insert request failed: %s"
                    (Printexc.to_string exn)))))
