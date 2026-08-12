(** The stateless REST interface for the Zerobus Ingest SDK — Async runtime
    (DESIGN.md §4.2).

    The Async counterpart of {!Zerobus_rest} (the Lwt reference). Same contract —
    a table-scoped client-credentials OAuth grant (reusing
    [Zerobus_core.Config.oauth_token_request_body]) plus one HTTP [POST] per batch
    — but built on [cohttp-async]. No streams, no offsets, no recovery.

    This library is [optional]: it only builds on a switch where [cohttp-async] is
    installed (which downgrades cohttp 6.0->5.3 — benign, see
    doc/arch/tls_async_status.md). Switches that only want the gRPC transport do
    not pull it. See {!Zerobus_rest_async} (the .mli) for the API contract. *)

open! Core
open! Async
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
     are not interchangeable across tables. Guarded by a Sequencer (concurrency
     1) so concurrent mints don't stampede the token endpoint. *)
  mutable token_cache : (string * token_entry) list;
  token_cache_lock : unit Sequencer.t;
}

let _ = fun (x : t) -> ignore x.application_name

(* Derive the REST base URL (no trailing slash). Identical policy to the Lwt
   {!Zerobus_rest.base_url_of}: a scheme-carrying [endpoint] is used verbatim;
   otherwise the shared gRPC host derivation is used over https, keeping a
   non-443 port. *)
let base_url_of ~endpoint ~workspace_url : (string, Error.t) result =
  if
    String.length endpoint > 0
    && (String.is_prefix ~prefix:"http://" endpoint
       || String.is_prefix ~prefix:"https://" endpoint)
  then
    let e =
      if
        String.length endpoint > 0
        && Char.equal endpoint.[String.length endpoint - 1] '/'
      then String.sub endpoint ~pos:0 ~len:(String.length endpoint - 1)
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
              token_cache_lock = Sequencer.create ();
            })

(* Mint (or reuse a cached) table-scoped token — same client-credentials grant as
   the Lwt reference, 30s early-refresh, keyed by table. The HTTP POST is the
   shared pure-ocaml-tls backend from zerobus-async ({!Zerobus_async.Oauth}, over
   [Tls_async.connect]) — NOT cohttp-async (whose conduit needs OpenSSL). *)
let mint_token t ~table : (string, error) result Deferred.t =
  Throttle.enqueue t.token_cache_lock (fun () ->
      let now = Unix.gettimeofday () in
      match List.Assoc.find t.token_cache table ~equal:String.equal with
      | Some (token, expiry) when Float.( < ) (now +. 30.0) expiry ->
          return (Ok token)
      | _ -> (
          match
            Config.oauth_token_request_body ~workspace_id:t.workspace_id ~table
          with
          | Error e -> return (Error e)
          | Ok body -> (
              let token_url = t.workspace_url ^ "/oidc/v1/token" in
              match%map
                Zerobus_async.Oauth.post_token ~token_url
                  ~client_id:t.client_id ~client_secret:t.client_secret ~body
              with
              | Error exn ->
                  Error
                    (Error.Transport_error
                       (Printf.sprintf "token request failed: %s"
                          (Exn.to_string exn)))
              | Ok (code, body_str) ->
                  if code < 200 || code >= 300 then
                    Error
                      (Error.Auth_error
                         (Printf.sprintf "token endpoint HTTP %d" code))
                  else (
                    match Zerobus_async.Oauth.parse_token_response ~now body_str with
                    | Some (tok, expiry) ->
                        t.token_cache <-
                          (table, (tok, expiry))
                          :: List.Assoc.remove t.token_cache table
                               ~equal:String.equal;
                        Ok tok
                    | None -> Error (Error.Auth_error "malformed token response"))
              )))

let insert t ~table (records : Yojson.Safe.t list) : (unit, error) result Deferred.t
    =
  match records with
  | [] -> return (Ok ())
  | _ -> (
      match%bind mint_token t ~table with
      | Error e -> return (Error e)
      | Ok token -> (
          let url =
            Printf.sprintf "%s/zerobus/v1/tables/%s/insert" t.base_url table
          in
          let headers =
            [
              ("Authorization", "Bearer " ^ token);
              ("Content-Type", "application/json");
            ]
          in
          let body = Yojson.Safe.to_string (`List records) in
          (* Pure-ocaml-tls HTTP POST (shared zerobus-async backend), NOT
             cohttp-async — same OpenSSL-free posture as the token mint. *)
          match%map Zerobus_async.Oauth.https_post ~url ~headers ~body with
          | Error exn ->
              Error
                (Error.Transport_error
                   (Printf.sprintf "insert request failed: %s"
                      (Exn.to_string exn)))
          | Ok (code, body_str) ->
              if code >= 200 && code < 300 then Ok ()
              else
                Error
                  (Error.Server_status
                     {
                       code;
                       message =
                         String.sub body_str ~pos:0
                           ~len:(min 256 (String.length body_str));
                     })))
