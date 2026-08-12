(** Eio REST acceptance: {!Zerobus_rest_eio} (the stateless REST insert helper,
    Eio runtime) against an in-process cohttp-eio mock that plays BOTH endpoints:
      - [POST /oidc/v1/token]                              -> the OAuth grant
      - [POST /zerobus/v1/tables/<table>/insert]           -> the batch insert

    The Eio counterpart of test_rest/test_rest_lwt.ml — same assertions, same
    coverage, direct-style. Proves the full REST flow end-to-end without a live
    workspace:
    - the client-credentials grant is issued (HTTP Basic client auth, form body),
      and the returned bearer token is attached to the insert request;
    - records are POSTed as a single JSON array to the right table path;
    - a 2xx insert -> [Ok ()]; a non-2xx insert -> [Server_status] with the code;
    - an empty batch sends nothing ([Ok ()]);
    - the token is cached (a second insert to the same table mints only once).

    Cleartext HTTP, loopback, ephemeral port. Runs on zbeio (OCaml 5.x). *)

module Rest = Zerobus_rest_eio

let table = "main.default.rest_mock"

(* --- observed server state, asserted after the run --- *)
let token_mints = ref 0
let insert_bodies = ref [] (* most-recent-first list of raw JSON bodies *)
let insert_paths = ref []
let saw_bearer = ref false
let saw_basic = ref false
let fail_insert = ref false

let starts_with s pre =
  String.length s >= String.length pre && String.sub s 0 (String.length pre) = pre

(* Start the cohttp-eio mock on an ephemeral loopback port; return its base URL.
   The server runs as a daemon fiber for the life of the switch. *)
let start_server ~sw ~env : string =
  let net = Eio.Stdenv.net env in
  let socket =
    Eio.Net.listen ~sw ~backlog:16 ~reuse_addr:true net
      (`Tcp (Eio.Net.Ipaddr.V4.loopback, 0))
  in
  let port =
    match Eio.Net.listening_addr socket with
    | `Tcp (_, p) -> p
    | `Unix _ -> failwith "no port"
  in
  let handler _conn (request : Http.Request.t) body =
    let path = Uri.path (Uri.of_string request.Http.Request.resource) in
    let headers = request.Http.Request.headers in
    let body_str = Eio.Buf_read.(parse_exn take_all) body ~max_size:max_int in
    if path = "/oidc/v1/token" then begin
      (match Http.Header.get headers "authorization" with
      | Some v when starts_with v "Basic " -> saw_basic := true
      | _ -> ());
      incr token_mints;
      let json =
        {|{"access_token":"tok-abc123","token_type":"Bearer","expires_in":3600}|}
      in
      Cohttp_eio.Server.respond_string ~status:`OK ~body:json ()
    end
    else if starts_with path "/zerobus/v1/t" then begin
      (match Http.Header.get headers "authorization" with
      | Some v when v = "Bearer tok-abc123" -> saw_bearer := true
      | _ -> ());
      insert_paths := path :: !insert_paths;
      insert_bodies := body_str :: !insert_bodies;
      if !fail_insert then
        Cohttp_eio.Server.respond_string ~status:`Bad_request
          ~body:{|{"error":"schema mismatch"}|} ()
      else Cohttp_eio.Server.respond_string ~status:`OK ~body:"{}" ()
    end
    else Cohttp_eio.Server.respond_string ~status:`Not_found ~body:"" ()
  in
  Eio.Fiber.fork_daemon ~sw (fun () ->
      Cohttp_eio.Server.run socket
        (Cohttp_eio.Server.make ~callback:handler ())
        ~on_error:(fun _ -> ()));
  Printf.sprintf "http://127.0.0.1:%d" port

type outcome = {
  mints : int;
  bearer : bool;
  basic : bool;
  first_body : string;
  n_inserts : int;
  empty_ok : bool;
  err_code : int option;
  path_ok : bool;
}

let run ~env ~sw : outcome =
  let base_url = start_server ~sw ~env in
  match
    Rest.create ~endpoint:base_url ~workspace_url:base_url
      ~client_id:"sp-app-id" ~client_secret:"sp-secret" ()
  with
  | Error e -> failwith (Zerobus_core.Error.to_string e)
  | Ok client ->
      let recs =
        [
          `Assoc [ ("id", `Int 1); ("name", `String "a") ];
          `Assoc [ ("id", `Int 2); ("name", `String "b") ];
          `Assoc [ ("id", `Int 3); ("name", `String "unic\xc3\xb8de") ];
        ]
      in
      let r1 = Rest.insert ~env ~sw client ~table recs in
      let r2 = Rest.insert ~env ~sw client ~table [ `Assoc [ ("id", `Int 4) ] ] in
      let r_empty = Rest.insert ~env ~sw client ~table [] in
      fail_insert := true;
      let r_err = Rest.insert ~env ~sw client ~table [ `Assoc [ ("id", `Int 5) ] ] in
      fail_insert := false;
      let err_code =
        match r_err with
        | Error (Zerobus_core.Error.Server_status { code; _ }) -> Some code
        | _ -> None
      in
      let path_ok =
        List.for_all
          (fun p -> p = "/zerobus/v1/tables/" ^ table ^ "/insert")
          !insert_paths
      in
      {
        mints = !token_mints;
        bearer = !saw_bearer;
        basic = !saw_basic;
        first_body = (match List.rev !insert_bodies with b :: _ -> b | [] -> "");
        n_inserts = List.length !insert_bodies;
        empty_ok = (r_empty = Ok ());
        err_code;
        path_ok = (path_ok && r1 = Ok () && r2 = Ok ());
      }

let result =
  lazy
    (Eio_main.run @@ fun env ->
     Eio.Switch.run @@ fun sw ->
     let o = run ~env ~sw in
     Printf.eprintf
       "ZEROBUS OCAML — EIO REST TEST -- evidence\n\
        ocaml_version : %s\n\
        token_mints   : %d (expect 1 — cached across inserts)\n\
        basic_auth_seen: %b   bearer_seen: %b\n\
        inserts_sent  : %d (expect 3: batch, single, error; empty skipped)\n\
        first_body    : %s\n\
        empty_is_noop : %b\n\
        error_code    : %s\n\
        paths_correct : %b\n%!"
       Sys.ocaml_version o.mints o.basic o.bearer o.n_inserts o.first_body
       o.empty_ok
       (match o.err_code with Some c -> string_of_int c | None -> "none")
       o.path_ok;
     o)

let () =
  Alcotest.run "rest-eio"
    [
      ( "rest-insert-eio",
        [
          Alcotest.test_case "basic-auth grant issued" `Quick (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool) "basic" true o.basic);
          Alcotest.test_case "bearer token attached to insert" `Quick (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool) "bearer" true o.bearer);
          Alcotest.test_case "token cached (mints once)" `Quick (fun () ->
              let o = Lazy.force result in
              Alcotest.(check int) "mints" 1 o.mints);
          Alcotest.test_case "records sent as one JSON array" `Quick (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool) "json array"
                true
                (String.length o.first_body > 0
                && o.first_body.[0] = '['
                && o.first_body.[String.length o.first_body - 1] = ']'));
          Alcotest.test_case "insert path correct + 2xx -> Ok" `Quick (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool) "path+ok" true o.path_ok);
          Alcotest.test_case "empty batch is a no-op" `Quick (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool) "empty" true o.empty_ok);
          Alcotest.test_case "3 inserts sent (empty skipped)" `Quick (fun () ->
              let o = Lazy.force result in
              Alcotest.(check int) "n" 3 o.n_inserts);
          Alcotest.test_case "non-2xx -> Server_status 400" `Quick (fun () ->
              let o = Lazy.force result in
              Alcotest.(check (option int)) "code" (Some 400) o.err_code);
        ] );
    ]
