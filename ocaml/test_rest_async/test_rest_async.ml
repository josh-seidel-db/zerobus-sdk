(** Async REST acceptance: {!Zerobus_rest_async} (the stateless REST insert
    helper, Async runtime) against an in-process cohttp-async mock that plays
    BOTH endpoints:
    - [POST /oidc/v1/token] -> the OAuth grant
    - [POST /zerobus/v1/tables/<table>/insert] -> the batch insert

    The Async counterpart of test_rest/test_rest_lwt.ml — same assertions, same
    coverage, Async idioms. Proves the full REST flow end-to-end without a live
    workspace:
    - the client-credentials grant is issued (HTTP Basic client auth, form
      body), and the returned bearer token is attached to the insert request;
    - records are POSTed as a single JSON array to the right table path;
    - a 2xx insert -> [Ok ()]; a non-2xx insert -> [Server_status] with the
      code;
    - an empty batch sends nothing ([Ok ()]);
    - the token is cached (a second insert to the same table mints only once).

    Cleartext HTTP, loopback, ephemeral port. Runs on a switch with
    cohttp-async. *)

open! Core
open! Async
module Rest = Zerobus_rest_async

let table = "main.default.rest_mock"

(* --- observed server state, asserted after the run --- *)
let token_mints = ref 0
let insert_bodies = ref [] (* most-recent-first list of raw JSON bodies *)
let insert_paths = ref []
let saw_bearer = ref false
let saw_basic = ref false
let fail_insert = ref false

(* --- local mock (cohttp-async server, cleartext) playing both endpoints --- *)
let start_server () : int Deferred.t =
  let handler ~body _addr (req : Cohttp.Request.t) =
    let uri = Cohttp.Request.uri req in
    let path = Uri.path uri in
    let headers = Cohttp.Request.headers req in
    let%bind body_str = Cohttp_async.Body.to_string body in
    if String.equal path "/oidc/v1/token" then begin
      (match Cohttp.Header.get headers "authorization" with
      | Some v when String.is_prefix v ~prefix:"Basic " -> saw_basic := true
      | _ -> ());
      Int.incr token_mints;
      Cohttp_async.Server.respond_string ~status:`OK
        {|{"access_token":"tok-abc123","token_type":"Bearer","expires_in":3600}|}
    end
    else if String.is_prefix path ~prefix:"/zerobus/v1/t" then begin
      (match Cohttp.Header.get headers "authorization" with
      | Some v when String.equal v "Bearer tok-abc123" -> saw_bearer := true
      | _ -> ());
      insert_paths := path :: !insert_paths;
      insert_bodies := body_str :: !insert_bodies;
      if !fail_insert then
        Cohttp_async.Server.respond_string ~status:`Bad_request
          {|{"error":"schema mismatch"}|}
      else Cohttp_async.Server.respond_string ~status:`OK "{}"
    end
    else Cohttp_async.Server.respond_string ~status:`Not_found ""
  in
  let%map server =
    Cohttp_async.Server.create ~on_handler_error:`Ignore
      (Tcp.Where_to_listen.bind_to Tcp.Bind_to_address.Localhost
         Tcp.Bind_to_port.On_port_chosen_by_os)
      handler
  in
  Cohttp_async.Server.listening_on server

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

let run () : outcome Deferred.t =
  let%bind port = start_server () in
  let base_url = Printf.sprintf "http://127.0.0.1:%d" port in
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
      let%bind r1 = Rest.insert client ~table recs in
      let%bind r2 = Rest.insert client ~table [ `Assoc [ ("id", `Int 4) ] ] in
      let%bind r_empty = Rest.insert client ~table [] in
      fail_insert := true;
      let%bind r_err =
        Rest.insert client ~table [ `Assoc [ ("id", `Int 5) ] ]
      in
      fail_insert := false;
      let err_code =
        match r_err with
        | Error (Zerobus_core.Error.Server_status { code; _ }) -> Some code
        | _ -> None
      in
      let path_ok =
        List.for_all
          ~f:(fun p ->
            String.equal p ("/zerobus/v1/tables/" ^ table ^ "/insert"))
          !insert_paths
      in
      let ok = function Ok () -> true | Error _ -> false in
      return
        {
          mints = !token_mints;
          bearer = !saw_bearer;
          basic = !saw_basic;
          first_body =
            (match List.rev !insert_bodies with b :: _ -> b | [] -> "");
          n_inserts = List.length !insert_bodies;
          empty_ok = ok r_empty;
          err_code;
          path_ok = path_ok && ok r1 && ok r2;
        }

let result : outcome = Thread_safe.block_on_async_exn run

let () =
  Core.eprintf
    "ZEROBUS OCAML — ASYNC REST TEST -- evidence\n\
     token_mints   : %d (expect 1 — cached across inserts)\n\
     basic_auth_seen: %b   bearer_seen: %b\n\
     inserts_sent  : %d (expect 3: batch, single, error; empty skipped)\n\
     first_body    : %s\n\
     empty_is_noop : %b\n\
     error_code    : %s\n\
     paths_correct : %b\n\
     %!"
    result.mints result.basic result.bearer result.n_inserts result.first_body
    result.empty_ok
    (match result.err_code with Some c -> Int.to_string c | None -> "none")
    result.path_ok;
  Alcotest.run "async-rest"
    [
      ( "rest-insert-async",
        [
          Alcotest.test_case "basic-auth grant issued" `Slow (fun () ->
              Alcotest.(check bool) "basic" true result.basic);
          Alcotest.test_case "bearer token attached to insert" `Slow (fun () ->
              Alcotest.(check bool) "bearer" true result.bearer);
          Alcotest.test_case "token cached (mints once)" `Slow (fun () ->
              Alcotest.(check int) "mints" 1 result.mints);
          Alcotest.test_case "records sent as one JSON array" `Slow (fun () ->
              Alcotest.(check bool)
                "json array" true
                (String.length result.first_body > 0
                && Char.equal result.first_body.[0] '['
                && Char.equal
                     result.first_body.[String.length result.first_body - 1]
                     ']'));
          Alcotest.test_case "insert path correct + 2xx -> Ok" `Slow (fun () ->
              Alcotest.(check bool) "path+ok" true result.path_ok);
          Alcotest.test_case "empty batch is a no-op" `Slow (fun () ->
              Alcotest.(check bool) "empty" true result.empty_ok);
          Alcotest.test_case "3 inserts sent (empty skipped)" `Slow (fun () ->
              Alcotest.(check int) "n" 3 result.n_inserts);
          Alcotest.test_case "non-2xx -> Server_status 400" `Slow (fun () ->
              Alcotest.(check (option int)) "code" (Some 400) result.err_code);
        ] );
    ]
