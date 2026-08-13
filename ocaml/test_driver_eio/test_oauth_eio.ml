(** Eio built-in OAuth acceptance: exercises {!Zerobus_eio.mint_token}'s full
    client-credentials flow — build the scoped request body (via
    {!Zerobus_core.Config}), HTTP POST to /oidc/v1/token with the Basic header,
    parse the JSON [access_token] — against a local cohttp-eio mock token
    endpoint.

    The mock runs cleartext (http://), which drives the same request-build →
    POST → JSON-parse path as the live HTTPS flow (the TLS wrapper is orthogonal
    and covered by test_tls_eio). It asserts: the token is returned, the request
    carried [grant_type=client_credentials] + the Basic auth header, and a
    non-2xx surfaces as [Auth_error]. Runs on zbeio. *)

module Zb = Zerobus_eio

let port = ref 0
let captured_body = ref ""
let captured_auth = ref ""

(* --- local mock token endpoint (cohttp-eio server, cleartext) --- *)
let start_server ~sw ~env : unit =
  let net = Eio.Stdenv.net env in
  let socket =
    Eio.Net.listen ~sw ~backlog:16 ~reuse_addr:true net
      (`Tcp (Eio.Net.Ipaddr.V4.loopback, 0))
  in
  (match Eio.Net.listening_addr socket with
  | `Tcp (_, p) -> port := p
  | `Unix _ -> ());
  let handler _conn request body =
    (* capture what the client sent, then reply with a token *)
    captured_auth :=
      Option.value
        (Http.Header.get (Http.Request.headers request) "authorization")
        ~default:"";
    captured_body := Eio.Buf_read.(parse_exn take_all) body ~max_size:max_int;
    let json = {|{"access_token":"tok-abc123","expires_in":3600}|} in
    Cohttp_eio.Server.respond_string ~status:`OK ~body:json ()
  in
  Eio.Fiber.fork_daemon ~sw (fun () ->
      Cohttp_eio.Server.run socket (Cohttp_eio.Server.make ~callback:handler ())
        ~on_error:(fun _ -> ()))

type result = {
  token : string option;
  err : string option;
  body_has_grant : bool;
  auth_is_basic : bool;
}

let contains hay needle =
  let nl = String.length needle and hl = String.length hay in
  let rec go i =
    if i + nl > hl then false
    else if String.sub hay i nl = needle then true
    else go (i + 1)
  in
  go 0

let starts_with s pre =
  String.length s >= String.length pre
  && String.sub s 0 (String.length pre) = pre

let run ~env ~sw : result =
  start_server ~sw ~env;
  (* Client pointed at the local mock: workspace_url = the mock base, so the token
     URL <workspace_url>/oidc/v1/token resolves to 127.0.0.1:port over cleartext
     http. [endpoint] is explicit so no workspace-URL derivation is exercised. *)
  match
    Zb.create ~endpoint:"127.0.0.1:443"
      ~workspace_url:(Printf.sprintf "http://127.0.0.1:%d" !port)
      ()
  with
  | Error e ->
      {
        token = None;
        err = Some (Zerobus_core.Error.to_string e);
        body_has_grant = false;
        auth_is_basic = false;
      }
  | Ok client -> (
      let res =
        Zb.mint_token ~env ~sw ~client ~table:"cat.sch.tbl"
          ~client_id:"my-client" ~client_secret:"my-secret"
      in
      let body_has_grant =
        contains !captured_body "grant_type=client_credentials"
      in
      let auth_is_basic = starts_with !captured_auth "Basic " in
      match res with
      | Error e ->
          {
            token = None;
            err = Some (Zerobus_core.Error.to_string e);
            body_has_grant;
            auth_is_basic;
          }
      | Ok token ->
          { token = Some token; err = None; body_has_grant; auth_is_basic })

let result =
  lazy
    ( Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      let r = run ~env ~sw in
      Printf.eprintf
        "ZEROBUS OCAML — EIO BUILT-IN OAUTH -- evidence\n\
         token_minted  : %b\n\
         body_has_grant_type: %b\n\
         auth_header_basic  : %b\n\
         error         : %s\n\
         %!"
        (r.token <> None) r.body_has_grant r.auth_is_basic
        (match r.err with Some e -> e | None -> "none");
      r )

let () =
  Alcotest.run "eio-oauth"
    [
      ( "client-credentials mint",
        [
          Alcotest.test_case "token returned" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool)
                "got token" true
                (r.token = Some "tok-abc123"));
          Alcotest.test_case "no error" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check (option string)) "err" None r.err);
          Alcotest.test_case "request had grant_type" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "grant_type" true r.body_has_grant);
          Alcotest.test_case "request had Basic auth" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "basic auth" true r.auth_is_basic);
        ] );
    ]
