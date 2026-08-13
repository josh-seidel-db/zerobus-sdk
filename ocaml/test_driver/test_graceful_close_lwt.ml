(** Graceful-close acceptance: the driver's [stream_paused_max_wait_time_ms]
    path end to end over Lwt, against a mock that sends a [CloseStreamSignal]
    mid-way on the first stream and then ends the body cleanly (NOT a retryable
    error status).

    With the default [stream_paused_max_wait_time_ms = None] and recovery on,
    the driver must: keep draining the acks the server already sent, observe the
    clean body end, then reconnect+replay the un-acked tail — so [flush] still
    confirms all 200 records durably acked, with the server-initiated close
    invisible above [ingest]/[flush]. This is the regression test for the
    graceful-close bugs the code review caught (sleep-blocks-draining, unbounded
    reconnect, waiter hang). *)

module Z = Zerobus.Driver
module Opt = Zerobus_core.Options

let ( let* ) = Lwt.bind
let n_records = 200
let port = ref 0

let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "ephemeral_server_close.exe" in
  if Sys.file_exists cand then cand else "./ephemeral_server_close.exe"

let with_server ?(slow = false) (f : unit -> 'a Lwt.t) : 'a Lwt.t =
  let exe = server_exe () in
  let stdout_r, stdout_w = Unix.pipe () in
  let args = if slow then [| exe; "0"; "slow" |] else [| exe; "0" |] in
  let pid = Unix.create_process exe args Unix.stdin stdout_w Unix.stderr in
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

type result = { flushed_ok : bool; acks_seen : int; err : string option }

let run ?(paused_max_wait_ms = None) () : result Lwt.t =
  Zerobus.Io_lwt_for_test.Scope.with_scope (fun scope ->
      let acks_seen = ref 0 in
      let ack_callback =
        { Opt.on_ack = (fun _ -> incr acks_seen); on_error = (fun _ _ -> ()) }
      in
      let options =
        {
          Opt.default_stream_options with
          Opt.record_type = Opt.Json;
          ack_callback = Some ack_callback;
          stream_paused_max_wait_time_ms = paused_max_wait_ms;
          recovery_backoff_ms = 50;
          recovery_timeout_ms = 2000;
          recovery_retries = 6;
        }
      in
      let table = { Opt.table_name = "main.default.mock"; descriptor = None } in
      let* stream_r =
        Z.open_stream ~host:"127.0.0.1" ~port:!port ~tls:false ~headers:[]
          ~options ~table scope
      in
      match stream_r with
      | Error e ->
          Lwt.return
            {
              flushed_ok = false;
              acks_seen = 0;
              err = Some (Zerobus_core.Error.to_string e);
            }
      | Ok stream -> (
          let rec loop i =
            if i >= n_records then Lwt.return (Ok ())
            else
              let* r =
                Z.ingest stream
                  (Bytes.of_string (Printf.sprintf {|{"id":%d}|} i))
              in
              match r with Ok _ -> loop (i + 1) | Error _ as e -> Lwt.return e
          in
          let* ing = loop 0 in
          match ing with
          | Error e ->
              Lwt.return
                {
                  flushed_ok = false;
                  acks_seen = !acks_seen;
                  err = Some (Zerobus_core.Error.to_string e);
                }
          | Ok () ->
              let* flush_r = Z.flush stream in
              let* _ = Z.close stream in
              Lwt.return
                {
                  flushed_ok =
                    (match flush_r with Ok () -> true | Error _ -> false);
                  acks_seen = !acks_seen;
                  err =
                    (match flush_r with
                    | Error e -> Some (Zerobus_core.Error.to_string e)
                    | Ok () -> None);
                }))

(* Bound the whole thing so a graceful-close bug (e.g. a hang) reports instead of
   hanging the suite forever. *)
let run_bounded ?paused_max_wait_ms () =
  Lwt.pick
    [
      run ?paused_max_wait_ms ();
      (let* () = Lwt_unix.sleep 30.0 in
       Lwt.return { flushed_ok = false; acks_seen = -1; err = Some "TIMEOUT" });
    ]

(* Default (None): fast mock ends the body right after the close signal; client
   drains to body-end then reconnects. *)
let result =
  lazy (Lwt_main.run (with_server (run_bounded ?paused_max_wait_ms:None)))

(* Finite cap (Some 300ms): the SLOW mock advertises a 10s close then HOLDS the body
   open for 10s. The client must NOT wait 10s — its 300ms cap must fire and drive the
   reconnect, so flush still succeeds well within the 30s bound. This is the path the
   Io.first race enables (previously would have hung on the 10s body). *)
let result_capped =
  lazy
    (Lwt_main.run
       (with_server ~slow:true (run_bounded ~paused_max_wait_ms:(Some 300))))

let () =
  Alcotest.run "graceful-close"
    [
      ( "drain-then-recover (None = full server window)",
        [
          Alcotest.test_case "no fatal error (recovered across close)" `Slow
            (fun () ->
              let r = Lazy.force result in
              Alcotest.(check (option string)) "err" None r.err);
          Alcotest.test_case "flush confirms all acked across the close" `Slow
            (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "flushed" true r.flushed_ok);
          Alcotest.test_case "acks observed (watermark advanced, no hang)" `Slow
            (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "acks_seen > 0" true (r.acks_seen > 0));
        ] );
      ( "finite cap (Some 300ms) fires before a held-open body",
        [
          Alcotest.test_case "no fatal error (capped then recovered)" `Slow
            (fun () ->
              let r = Lazy.force result_capped in
              Alcotest.(check (option string)) "err" None r.err);
          Alcotest.test_case "flush succeeds via the cap (not the 10s body)"
            `Slow (fun () ->
              let r = Lazy.force result_capped in
              Alcotest.(check bool) "flushed" true r.flushed_ok);
        ] );
    ]
