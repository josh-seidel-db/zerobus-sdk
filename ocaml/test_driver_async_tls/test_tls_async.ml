(** Live-TLS acceptance on Async: drives the {!Zerobus_async} façade over a REAL
    TLS 1.3 + ALPN-h2 handshake against the self-signed [tls_server_async] mock
    (separate process). Proves the Async transport's TLS path end to end —
    handshake, ALPN negotiation of h2, and gRPC framing over the encrypted flow —
    the Async counterpart of [test_driver_eio/test_tls_eio.ml].

    The client can't use the system trust store for a self-signed cert, so the
    server prints its cert SHA-256 (base64) on the READY line and the client pins
    it via {!Zerobus_async.Io_async_for_test}'s [Tls_connect.pinned_cert_fp_sha256_b64]
    — the Async equivalent of the Eio [~authenticator] override.

    Two cases:
    - {b positive}: with the correct pin, 200 records are issued and [flush]
      confirms they were all acked over TLS.
    - {b negative}: with a WRONG pin, the handshake's peer-cert verification must
      fail, so stream creation returns an error (no records acked). This is what
      proves we do real certificate verification (not a null authenticator).

    Builds/runs only on a switch with tls-async present (see
    doc/arch/tls_async_status.md); the dune [enabled_if] gates it. *)

open! Core
open! Async
module Zb = Zerobus_async
module Tls_connect = Zb.Tls_connect

let n_records = 200
let port = ref 0
let cert_fp = ref ""

let server_exe () =
  let dir = Filename.dirname (Sys.get_argv ()).(0) in
  let cand = Filename.concat dir "tls_server_async.exe" in
  if Sys_unix.file_exists_exn cand then cand else "./tls_server_async.exe"

let with_server (f : unit -> 'a Deferred.t) : 'a Deferred.t =
  let%bind process = Process.create_exn ~prog:(server_exe ()) ~args:[ "0" ] () in
  let stdout_pipe = Reader.lines (Process.stdout process) in
  let%bind ready_line = Pipe.read stdout_pipe in
  (match ready_line with
   | `Ok line -> (
       match String.split ~on:' ' line with
       | [ "READY"; p; fp ] -> port := Int.of_string p; cert_fp := fp
       | _ -> failwithf "unexpected server line: %s" line ())
   | `Eof -> failwith "server closed before READY");
  Monitor.protect f ~finally:(fun () ->
      Process.send_signal process Signal.kill;
      let%bind _ = Process.wait process in
      return ())

type result = { acked_last : bool; issued : int; err : string option }

let empty ?err () = { acked_last = false; issued = 0; err }

(* Run one façade session over TLS with the given cert-fp pin. *)
let run_with_pin ~pin : result Deferred.t =
  Tls_connect.pinned_cert_fp_sha256_b64 := Some pin;
  let%bind client_r =
    Zb.create ~endpoint:(Printf.sprintf "127.0.0.1:%d" !port)
      ~workspace_url:"https://adb-1234567890.11.azuredatabricks.net" ()
  in
  match client_r with
  | Error e -> return (empty ~err:(Zerobus_core.Error.to_string e) ())
  | Ok client ->
      let table =
        { Zerobus_core.Options.table_name = "main.default.mock"; descriptor = None }
      in
      let options =
        { Zerobus_core.Options.default_stream_options with
          record_type = Zerobus_core.Options.Json }
      in
      let headers_provider () =
        return
          (Ok
             [ ("authorization", "Bearer mock-token");
               ( "x-databricks-zerobus-table-name",
                 table.Zerobus_core.Options.table_name ) ])
      in
      let%map r =
        Zb.with_stream ~tls:true client table ~headers_provider ~options
          (fun stream ->
            let%bind ingested =
              Deferred.repeat_until_finished (0, None) (fun (i, last) ->
                  if i >= n_records then return (`Finished (Ok last))
                  else
                    let%map r =
                      Zb.ingest stream
                        (Bytes.of_string (Printf.sprintf {|{"id":%d}|} i))
                    in
                    match r with
                    | Ok off -> `Repeat (i + 1, Some off)
                    | Error _ as e -> `Finished e)
            in
            match ingested with
            | Error e -> return (empty ~err:(Zerobus_core.Error.to_string e) ())
            | Ok last_off ->
                let%map flush_r = Zb.flush stream in
                let acked_last =
                  match (flush_r, last_off) with Ok (), Some _ -> true | _ -> false
                in
                {
                  acked_last;
                  issued = n_records;
                  err =
                    (match flush_r with
                     | Error e -> Some (Zerobus_core.Error.to_string e)
                     | Ok () -> None);
                })
      in
      (match r with
       | Ok res -> res
       | Error e -> empty ~err:(Zerobus_core.Error.to_string e) ())

(* Flip one hex nibble of the real fingerprint to get a valid-shape but WRONG pin. *)
let wrong_pin (fp : string) : string =
  if String.length fp = 0 then "AAAA"
  else
    let c = fp.[0] in
    let c' = if Char.equal c 'A' then 'B' else 'A' in
    String.of_char c' ^ String.drop_prefix fp 1

(* Each case gets its own server process (fresh self-signed cert). *)
let positive : result =
  Thread_safe.block_on_async_exn (fun () ->
      with_server (fun () -> run_with_pin ~pin:!cert_fp))

let negative : result =
  Thread_safe.block_on_async_exn (fun () ->
      with_server (fun () -> run_with_pin ~pin:(wrong_pin !cert_fp)))

let () =
  Alcotest.run "phase3-tls-async"
    [
      ( "live-tls",
        [
          Alcotest.test_case "no error (correct pin)" `Slow (fun () ->
              Alcotest.(check (option string)) "err" None positive.err);
          Alcotest.test_case "all records issued over TLS" `Slow (fun () ->
              Alcotest.(check int) "issued" n_records positive.issued);
          Alcotest.test_case "flush confirms all acked over TLS" `Slow (fun () ->
              Alcotest.(check bool) "acked" true positive.acked_last);
          Alcotest.test_case "wrong cert pin is rejected" `Slow (fun () ->
              Alcotest.(check bool) "no ack on bad cert" false negative.acked_last;
              Alcotest.(check bool) "error present on bad cert" true
                (Option.is_some negative.err));
        ] );
    ]
