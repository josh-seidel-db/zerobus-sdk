(** No-data-loss overflow policy (#1) acceptance. The un-acked replay buffer must
    NEVER silently drop records; when it fills, [overflow_policy] decides:

    - [Fail]: [ingest] returns [Error (Backpressure _)] once the buffer is full
      (here [max_inflight_requests = 5] against the never-acking mock), so the
      caller can react. No record is dropped or corrupted.
    - [Block]: [ingest] parks until the ack-reader drains the buffer, then proceeds.
      Against the well-behaved mock (which acks everything), a tiny bound must still
      let ALL records through — the flush confirms every one durably acked, proving
      backpressure throttles rather than loses.

    Separate-process mocks, cleartext h2c. *)

module Z = Zerobus.Driver
module Opt = Zerobus_core.Options

let ( let* ) = Lwt.bind

let exe_in_cwd name =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir name in
  if Sys.file_exists cand then cand else "./" ^ name

let with_server exe_name (f : int -> 'a Lwt.t) : 'a Lwt.t =
  let exe = exe_in_cwd exe_name in
  let stdout_r, stdout_w = Unix.pipe () in
  let pid = Unix.create_process exe [| exe; "0" |] Unix.stdin stdout_w Unix.stderr in
  Unix.close stdout_w;
  let ic = Unix.in_channel_of_descr stdout_r in
  let port =
    try
      match String.split_on_char ' ' (input_line ic) with
      | [ "READY"; p ] -> int_of_string p
      | _ -> failwith "bad READY line"
    with End_of_file -> failwith "server did not signal READY"
  in
  Lwt.finalize
    (fun () -> f port)
    (fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      (try close_in ic with _ -> ());
      Lwt.return_unit)

(* ---- Fail policy: never-acking mock, tiny bound → ingest eventually Backpressure. *)
let run_fail () : (bool * bool) Lwt.t =
  with_server "ephemeral_server_noack.exe" (fun port ->
      Zerobus.Io_lwt_for_test.Scope.with_scope (fun scope ->
          let options =
            { Opt.default_stream_options with
              Opt.record_type = Opt.Json;
              max_inflight_requests = 5;
              overflow_policy = Opt.Fail;
              recovery = false }
          in
          let table = { Opt.table_name = "main.default.mock"; descriptor = None } in
          let* stream_r =
            Z.open_stream ~host:"127.0.0.1" ~port ~tls:false ~headers:[] ~options
              ~table scope
          in
          match stream_r with
          | Error _ -> Lwt.return (false, false)
          | Ok stream ->
              (* Push more than the bound; none are acked, so after ~5 the buffer is
                 full and ingest must return Backpressure — never silently drop. *)
              let rec loop i saw_bp =
                if i >= 50 then Lwt.return saw_bp
                else
                  let* r = Z.ingest stream (Bytes.of_string (Printf.sprintf {|{"i":%d}|} i)) in
                  match r with
                  | Error (Zerobus_core.Error.Backpressure _) -> Lwt.return true
                  | Error _ -> Lwt.return saw_bp
                  | Ok _ -> loop (i + 1) saw_bp
              in
              let* saw_bp = loop 0 false in
              let* _ = Z.close stream in
              (* saw_bp = we got a clean Backpressure error; second bool: it happened
                 without the process dying / hanging (we returned). *)
              Lwt.return (saw_bp, true)))

(* ---- Block policy: well-behaved mock acks everything; tiny bound must still let
   ALL records through (throttle, not loss) — flush confirms every one acked. *)
let run_block () : (bool * string option) Lwt.t =
  with_server "ephemeral_server.exe" (fun port ->
      Zerobus.Io_lwt_for_test.Scope.with_scope (fun scope ->
          let options =
            { Opt.default_stream_options with
              Opt.record_type = Opt.Json;
              max_inflight_requests = 8;
              overflow_policy = Opt.Block }
          in
          let table = { Opt.table_name = "main.default.mock"; descriptor = None } in
          let* stream_r =
            Z.open_stream ~host:"127.0.0.1" ~port ~tls:false ~headers:[] ~options
              ~table scope
          in
          match stream_r with
          | Error e -> Lwt.return (false, Some (Zerobus_core.Error.to_string e))
          | Ok stream ->
              let n = 200 in
              let rec loop i =
                if i >= n then Lwt.return (Ok ())
                else
                  let* r = Z.ingest stream (Bytes.of_string (Printf.sprintf {|{"i":%d}|} i)) in
                  match r with Ok _ -> loop (i + 1) | Error _ as e -> Lwt.return e
              in
              let* ing = loop 0 in
              (match ing with
               | Error e -> Lwt.return (false, Some (Zerobus_core.Error.to_string e))
               | Ok () ->
                   let* flush_r = Z.flush stream in
                   let* _ = Z.close stream in
                   Lwt.return
                     (match flush_r with
                      | Ok () -> (true, None)
                      | Error e -> (false, Some (Zerobus_core.Error.to_string e))))))

let bounded label f =
  Lwt.pick
    [ f ();
      (let* () = Lwt_unix.sleep 25.0 in Lwt.fail_with (label ^ ": TIMEOUT")) ]

let fail_result = lazy (Lwt_main.run (bounded "fail" run_fail))
let block_result = lazy (Lwt_main.run (bounded "block" run_block))

let () =
  Alcotest.run "backpressure"
    [
      ( "Fail policy returns Backpressure (no drop)",
        [
          Alcotest.test_case "got a Backpressure error" `Slow (fun () ->
              let saw_bp, _ = Lazy.force fail_result in
              Alcotest.(check bool) "backpressure surfaced" true saw_bp);
          Alcotest.test_case "handled without hang/crash" `Slow (fun () ->
              let _, returned = Lazy.force fail_result in
              Alcotest.(check bool) "returned cleanly" true returned);
        ] );
      ( "Block policy throttles, never loses",
        [
          Alcotest.test_case "no error" `Slow (fun () ->
              let _, err = Lazy.force block_result in
              Alcotest.(check (option string)) "err" None err);
          Alcotest.test_case "flush confirms all 200 acked under a bound of 8" `Slow
            (fun () ->
              let ok, _ = Lazy.force block_result in
              Alcotest.(check bool) "all acked" true ok);
        ] );
    ]
