(** Phase 5 acceptance on the Async runtime: the real streaming driver
    ({!Zerobus_core.Make}) over the Async instantiation, against the mock
    [EphemeralStream] server (separate process, cleartext h2c). Proves the same
    loop-then-flush data plane the Lwt and Eio tests prove — create_stream,
    ingest N records (queue-only), flush once, assert all acked, close — on Jane
    Street Async ([Deferred]/[Pipe]).

    Runs on fl414. Separate-process topology (the proven pattern). *)

open! Core
open! Async
module Io = Zerobus_async.Io_async_for_test
module Z = Io.Stream
module Opt = Zerobus_core.Options

let n_records = 200
let port = ref 0

let server_exe () =
  let dir = Filename.dirname (Sys.get_argv ()).(0) in
  let cand = Filename.concat dir "ephemeral_server_async.exe" in
  if Sys_unix.file_exists_exn cand then cand else "./ephemeral_server_async.exe"

let with_server (f : unit -> 'a Deferred.t) : 'a Deferred.t =
  let%bind process =
    Process.create_exn ~prog:(server_exe ()) ~args:[ "0" ] ()
  in
  let stdout_pipe = Reader.lines (Process.stdout process) in
  let%bind ready_line = Pipe.read stdout_pipe in
  (match ready_line with
  | `Ok line -> (
      match String.split ~on:' ' line with
      | [ "READY"; p ] -> port := Int.of_string p
      | _ -> failwithf "unexpected server line: %s" line ())
  | `Eof -> failwith "server closed before READY");
  Monitor.protect f ~finally:(fun () ->
      Process.send_signal process Signal.kill;
      let%bind _ = Process.wait process in
      return ())

type result = { acked_last : bool; issued : int; err : string option }

let run_driver () : result Deferred.t =
  Io.Scope.with_scope (fun scope ->
      let options =
        { Opt.default_stream_options with Opt.record_type = Opt.Json }
      in
      let table = { Opt.table_name = "main.default.mock"; descriptor = None } in
      let%bind stream_r =
        Z.open_stream ~host:"127.0.0.1" ~port:!port ~tls:false ~headers:[]
          ~options ~table scope
      in
      match stream_r with
      | Error e ->
          return
            {
              acked_last = false;
              issued = 0;
              err = Some (Zerobus_core.Error.to_string e);
            }
      | Ok stream -> (
          (* loop-then-flush: queue N records, do NOT wait per record *)
          let%bind ingested =
            Deferred.repeat_until_finished (0, None) (fun (i, last) ->
                if i >= n_records then return (`Finished (Ok last))
                else
                  let%map r =
                    Z.ingest stream
                      (Bytes.of_string (Printf.sprintf {|{"id":%d}|} i))
                  in
                  match r with
                  | Ok off -> `Repeat (i + 1, Some off)
                  | Error _ as e -> `Finished e)
          in
          match ingested with
          | Error e ->
              return
                {
                  acked_last = false;
                  issued = 0;
                  err = Some (Zerobus_core.Error.to_string e);
                }
          | Ok last_off ->
              (* flush once — wait for all pending acks *)
              let%bind flush_r = Z.flush stream in
              let%map _ = Z.close stream in
              let acked_last =
                match (flush_r, last_off) with
                | Ok (), Some _ -> true
                | _ -> false
              in
              {
                acked_last;
                issued = n_records;
                err =
                  (match flush_r with
                  | Error e -> Some (Zerobus_core.Error.to_string e)
                  | Ok () -> None);
              }))

let result : result =
  Thread_safe.block_on_async_exn (fun () ->
      with_server (fun () ->
          let%map r = run_driver () in
          Core.eprintf
            "ZEROBUS OCAML PHASE 5 DRIVER TEST (Async) -- evidence\n\
             ocaml_version : %s\n\
             records_issued: %d\n\
             flush_all_acked: %b\n\
             error         : %s\n\
             %!"
            Sys.ocaml_version r.issued r.acked_last
            (match r.err with Some e -> e | None -> "none");
          r))

let () =
  Alcotest.run "phase5-driver-async"
    [
      ( "loop-then-flush",
        [
          Alcotest.test_case "no error" `Slow (fun () ->
              Alcotest.(check (option string)) "err" None result.err);
          Alcotest.test_case "all records issued" `Slow (fun () ->
              Alcotest.(check int) "issued" n_records result.issued);
          Alcotest.test_case "flush confirms all acked" `Slow (fun () ->
              Alcotest.(check bool) "acked" true result.acked_last);
        ] );
    ]
