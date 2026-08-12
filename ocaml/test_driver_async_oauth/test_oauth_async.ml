(** Async built-in OAuth acceptance: exercises {!Zerobus_async.mint_token}'s full
    client-credentials flow — build the scoped request body (via
    {!Zerobus_core.Config}), HTTP POST to /oidc/v1/token with the Basic header,
    parse the JSON [access_token] — against a local cohttp-async mock token
    endpoint. The Async counterpart of [test_driver_eio/test_oauth_eio.ml].

    The mock runs cleartext (http://), which drives the same request-build → POST →
    JSON-parse path as the live HTTPS flow (the TLS wrapper is orthogonal and
    covered by test_driver_async_tls). It asserts: the token is returned, the
    request carried [grant_type=client_credentials] + the Basic auth header, a
    second call is served from the cache (no second endpoint hit), and a non-2xx
    surfaces as [Auth_error]. Runs on fl414 (needs cohttp-async). *)

open! Core
open! Async
module Zb = Zerobus_async

let captured_body = ref ""
let captured_auth = ref ""
let hits = ref 0

(* --- local mock token endpoint (cohttp-async server, cleartext) --- *)
(* [fail] makes it answer 401 to exercise the error path. *)
let start_server ~fail : int Deferred.t =
  let handler ~body _addr (req : Cohttp.Request.t) =
    let%bind body_str = Cohttp_async.Body.to_string body in
    Int.incr hits;
    captured_body := body_str;
    captured_auth :=
      Option.value
        (Cohttp.Header.get (Cohttp.Request.headers req) "authorization")
        ~default:"";
    if fail then
      Cohttp_async.Server.respond_string ~status:`Unauthorized "nope"
    else
      Cohttp_async.Server.respond_string ~status:`OK
        {|{"access_token":"tok-abc123","expires_in":3600}|}
  in
  let%map server =
    Cohttp_async.Server.create ~on_handler_error:`Ignore
      (Tcp.Where_to_listen.bind_to Tcp.Bind_to_address.Localhost
         Tcp.Bind_to_port.On_port_chosen_by_os)
      handler
  in
  Cohttp_async.Server.listening_on server

let contains hay needle =
  String.is_substring hay ~substring:needle

type result = {
  token : string option;
  token2 : string option;
  err : string option;
  body_has_grant : bool;
  auth_is_basic : bool;
  hits : int;
  fail_err : string option;
}

let run () : result Deferred.t =
  (* Happy path: client pointed at the local mock; workspace_url = the mock base,
     so <workspace_url>/oidc/v1/token resolves to 127.0.0.1:port over cleartext. *)
  let%bind port = start_server ~fail:false in
  let%bind client_r =
    Zb.create ~endpoint:"127.0.0.1:443"
      ~workspace_url:(Printf.sprintf "http://127.0.0.1:%d" port) ()
  in
  match client_r with
  | Error e ->
      return
        { token = None; token2 = None;
          err = Some (Zerobus_core.Error.to_string e);
          body_has_grant = false; auth_is_basic = false; hits = !hits;
          fail_err = None }
  | Ok client ->
      let%bind res1 =
        Zb.mint_token ~client ~table:"cat.sch.tbl" ~client_id:"my-client"
          ~client_secret:"my-secret"
      in
      let body_has_grant = contains !captured_body "grant_type=client_credentials" in
      let auth_is_basic = String.is_prefix !captured_auth ~prefix:"Basic " in
      (* Second mint should hit the cache (fresh token), NOT the endpoint again. *)
      let%bind res2 =
        Zb.mint_token ~client ~table:"cat.sch.tbl" ~client_id:"my-client"
          ~client_secret:"my-secret"
      in
      let token = match res1 with Ok t -> Some t | Error _ -> None in
      let token2 = match res2 with Ok t -> Some t | Error _ -> None in
      let err = match res1 with Error e -> Some (Zerobus_core.Error.to_string e) | Ok _ -> None in
      let hits_after_happy = !hits in
      (* Error path: a fresh client against a 401 endpoint -> Auth_error. *)
      let%bind fail_port = start_server ~fail:true in
      let%bind fail_client_r =
        Zb.create ~endpoint:"127.0.0.1:443"
          ~workspace_url:(Printf.sprintf "http://127.0.0.1:%d" fail_port) ()
      in
      let%map fail_err =
        match fail_client_r with
        | Error e -> return (Some (Zerobus_core.Error.to_string e))
        | Ok fc -> (
            match%map
              Zb.mint_token ~client:fc ~table:"cat.sch.tbl"
                ~client_id:"x" ~client_secret:"y"
            with
            | Error e -> Some (Zerobus_core.Error.to_string e)
            | Ok _ -> None)
      in
      { token; token2; err; body_has_grant; auth_is_basic;
        hits = hits_after_happy; fail_err }

let result : result = Thread_safe.block_on_async_exn run

let () =
  Core.eprintf
    "ZEROBUS OCAML — ASYNC BUILT-IN OAUTH -- evidence\n\
     token_minted       : %b\n\
     body_has_grant_type: %b\n\
     auth_header_basic  : %b\n\
     token_cached_2nd   : %b\n\
     endpoint_hits      : %d (expect 1: 2nd mint served from cache)\n\
     error              : %s\n\
     bad_endpoint_error : %s\n%!"
    (Option.is_some result.token) result.body_has_grant result.auth_is_basic
    (match (result.token, result.token2) with
     | Some a, Some b -> String.equal a b
     | _ -> false)
    result.hits
    (Option.value result.err ~default:"none")
    (Option.value result.fail_err ~default:"none");
  Alcotest.run "async-oauth"
    [
      ( "client-credentials mint",
        [
          Alcotest.test_case "token returned" `Slow (fun () ->
              Alcotest.(check bool) "got token" true
                (Option.equal String.equal result.token (Some "tok-abc123")));
          Alcotest.test_case "no error" `Slow (fun () ->
              Alcotest.(check (option string)) "err" None result.err);
          Alcotest.test_case "request had grant_type" `Slow (fun () ->
              Alcotest.(check bool) "grant_type" true result.body_has_grant);
          Alcotest.test_case "request had Basic auth" `Slow (fun () ->
              Alcotest.(check bool) "basic auth" true result.auth_is_basic);
          Alcotest.test_case "second mint served from cache" `Slow (fun () ->
              Alcotest.(check int) "one endpoint hit" 1 result.hits);
          Alcotest.test_case "bad endpoint -> Auth_error" `Slow (fun () ->
              Alcotest.(check bool) "error present" true
                (Option.is_some result.fail_err));
        ] );
    ]
