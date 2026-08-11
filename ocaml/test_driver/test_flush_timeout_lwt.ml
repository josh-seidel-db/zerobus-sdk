(** Flush-timeout acceptance: against a mock that accepts the stream but NEVER
    acks, [flush] with a short [flush_timeout_ms] must return [Error (Timeout _)]
    within roughly that window — not hang forever. This locks in the wired-up
    [flush_timeout_ms] / [server_lack_of_ack_timeout_ms] (a live Azure test found
    flush hung indefinitely when the server never acked). *)

module Z = Zerobus.Driver
module Opt = Zerobus_core.Options

let ( let* ) = Lwt.bind
let port = ref 0

let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "ephemeral_server_noack.exe" in
  if Sys.file_exists cand then cand else "./ephemeral_server_noack.exe"

let with_server (f : unit -> 'a Lwt.t) : 'a Lwt.t =
  let exe = server_exe () in
  let stdout_r, stdout_w = Unix.pipe () in
  let pid = Unix.create_process exe [| exe; "0" |] Unix.stdin stdout_w Unix.stderr in
  Unix.close stdout_w;
  let ic = Unix.in_channel_of_descr stdout_r in
  (try
     match String.split_on_char ' ' (input_line ic) with
     | [ "READY"; p ] -> port := int_of_string p
     | _ -> failwith "bad READY line"
   with End_of_file -> failwith "server did not signal READY");
  Lwt.finalize f (fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      (try close_in ic with _ -> ());
      Lwt.return_unit)

type outcome = { flush_result : (unit, Zerobus_core.Error.t) result; elapsed : float }

let run () : outcome Lwt.t =
  Zerobus.Io_lwt_for_test.Scope.with_scope (fun scope ->
      let options =
        {
          Opt.default_stream_options with
          Opt.record_type = Opt.Json;
          (* short timeouts so the test is quick; recovery off so we don't spend
             retries chasing an ack that never comes. *)
          flush_timeout_ms = 1500;
          server_lack_of_ack_timeout_ms = 1500;
          recovery = false;
        }
      in
      let table = { Opt.table_name = "main.default.noack"; descriptor = None } in
      let* stream_r =
        Z.open_stream ~host:"127.0.0.1" ~port:!port ~tls:false ~headers:[]
          ~options ~table scope
      in
      match stream_r with
      | Error e -> Lwt.return { flush_result = Error e; elapsed = 0. }
      | Ok stream ->
          let* _ = Z.ingest stream (Bytes.of_string {|{"id":1}|}) in
          let t0 = Unix.gettimeofday () in
          let* flush_result = Z.flush stream in
          let elapsed = Unix.gettimeofday () -. t0 in
          let* _ = Z.close stream in
          Lwt.return { flush_result; elapsed })

let result = lazy (Lwt_main.run (with_server run))

let is_timeout = function Error (Zerobus_core.Error.Timeout _) -> true | _ -> false

let () =
  Alcotest.run "flush-timeout"
    [
      ( "no-ack",
        [
          Alcotest.test_case "flush returns Timeout (not hang, not Ok)" `Slow
            (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool)
                (Printf.sprintf "timeout (got %s)"
                   (match o.flush_result with
                    | Ok () -> "Ok"
                    | Error e -> Zerobus_core.Error.to_string e))
                true (is_timeout o.flush_result));
          Alcotest.test_case "returned near the timeout window" `Slow (fun () ->
              let o = Lazy.force result in
              (* ~1.5s configured; allow generous slack but assert it did NOT hang. *)
              Alcotest.(check bool)
                (Printf.sprintf "elapsed %.2fs < 10s" o.elapsed)
                true (o.elapsed < 10.0));
        ] );
    ]
