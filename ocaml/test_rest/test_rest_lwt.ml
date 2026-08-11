(** Phase 7c acceptance: {!Zerobus_rest} (the stateless REST insert helper)
    against an in-process cohttp mock that plays BOTH endpoints:
      - [POST /oidc/v1/token]                              -> the OAuth grant
      - [POST /zerobus/v1/tables/<table>/insert]           -> the batch insert

    Proves the full REST flow end-to-end without a live workspace:
    - the client-credentials grant is issued (HTTP Basic client auth, form body),
      and the returned bearer token is attached to the insert request;
    - records are POSTed as a single JSON array to the right table path;
    - a 2xx insert -> [Ok ()]; a non-2xx insert -> [Server_status] with the code;
    - an empty batch sends nothing ([Ok ()]);
    - the token is cached (a second insert to the same table mints only once).

    Cleartext HTTP, loopback, ephemeral port. Runs on fl414. *)

module Rest = Zerobus_rest

let ( let* ) = Lwt.bind
let table = "main.default.rest_mock"

(* --- observed server state, asserted after the run --- *)
let token_mints = ref 0
let insert_bodies = ref [] (* most-recent-first list of raw JSON bodies *)
let insert_paths = ref []
let saw_bearer = ref false
let saw_basic = ref false

(* Fail the next insert with 400 when set (to exercise the error path). *)
let fail_insert = ref false

let handler _conn (req : Cohttp.Request.t) (body : Cohttp_lwt.Body.t) =
  let uri = Cohttp.Request.uri req in
  let path = Uri.path uri in
  let headers = Cohttp.Request.headers req in
  let* body_str = Cohttp_lwt.Body.to_string body in
  if path = "/oidc/v1/token" then begin
    (match Cohttp.Header.get headers "authorization" with
    | Some v when String.length v >= 6 && String.sub v 0 6 = "Basic " ->
        saw_basic := true
    | _ -> ());
    incr token_mints;
    let json =
      {|{"access_token":"tok-abc123","token_type":"Bearer","expires_in":3600}|}
    in
    Cohttp_lwt_unix.Server.respond_string ~status:`OK ~body:json ()
  end
  else if
    String.length path > 12
    && String.sub path 0 13 = "/zerobus/v1/t"
    (* /zerobus/v1/tables/<table>/insert *)
  then begin
    (match Cohttp.Header.get headers "authorization" with
    | Some v when v = "Bearer tok-abc123" -> saw_bearer := true
    | _ -> ());
    insert_paths := path :: !insert_paths;
    insert_bodies := body_str :: !insert_bodies;
    if !fail_insert then
      Cohttp_lwt_unix.Server.respond_string ~status:`Bad_request
        ~body:{|{"error":"schema mismatch"}|} ()
    else Cohttp_lwt_unix.Server.respond_string ~status:`OK ~body:"{}" ()
  end
  else Cohttp_lwt_unix.Server.respond_string ~status:`Not_found ~body:"" ()

(* Start the mock on an ephemeral loopback port; return its base URL. The server
   fiber runs detached (Lwt.async) for the life of the test process. *)
let start_server () : string Lwt.t =
  let ctx = Cohttp_lwt_unix.Net.init () in
  let server = Cohttp_lwt_unix.Server.make ~callback:handler () in
  (* Bind the listening socket ourselves so we can read back the OS-chosen port
     (Server.create over `Port 0 gives no way to recover it). *)
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  let* () = Lwt_unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0)) in
  Lwt_unix.listen sock 16;
  let port =
    match Lwt_unix.getsockname sock with
    | Unix.ADDR_INET (_, p) -> p
    | _ -> failwith "no port"
  in
  Lwt.async (fun () ->
      Cohttp_lwt_unix.Server.create ~ctx ~mode:(`TCP (`Socket sock)) server);
  Lwt.return (Printf.sprintf "http://127.0.0.1:%d" port)

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

let run () : outcome Lwt.t =
  let* base_url = start_server () in
  (* Both the OIDC token endpoint (workspace_url) and the insert endpoint
     (endpoint override) point at the mock. *)
  let* client_r =
    Rest.create ~endpoint:base_url ~workspace_url:base_url
      ~client_id:"sp-app-id" ~client_secret:"sp-secret" ()
  in
  match client_r with
  | Error e -> failwith (Zerobus_core.Error.to_string e)
  | Ok client ->
      (* 1) a real batch of 3 JSON records *)
      let recs =
        [
          `Assoc [ ("id", `Int 1); ("name", `String "a") ];
          `Assoc [ ("id", `Int 2); ("name", `String "b") ];
          `Assoc [ ("id", `Int 3); ("name", `String "unicøde") ];
        ]
      in
      let* r1 = Rest.insert client ~table recs in
      (* 2) a second insert to the same table — should reuse the cached token *)
      let* r2 = Rest.insert client ~table [ `Assoc [ ("id", `Int 4) ] ] in
      (* 3) empty batch — no request, Ok () *)
      let* r_empty = Rest.insert client ~table [] in
      (* 4) error path — server returns 400 *)
      fail_insert := true;
      let* r_err = Rest.insert client ~table [ `Assoc [ ("id", `Int 5) ] ] in
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
      Lwt.return
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
    (Lwt_main.run
       (let* o = run () in
        Printf.eprintf
          "ZEROBUS OCAML PHASE 7c REST TEST (Lwt) -- evidence\n\
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
        Lwt.return o))

let () =
  Alcotest.run "phase7c-rest"
    [
      ( "rest-insert",
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
