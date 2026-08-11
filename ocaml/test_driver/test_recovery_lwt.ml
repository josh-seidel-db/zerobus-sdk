(** Phase 6 acceptance: the driver's recovery (§12.3) end to end over Lwt, against
    a mock server that drops the first stream mid-way with a retryable status. The
    driver must reconnect, replay the un-acked tail in order, and ultimately have
    every record durably acked — recovery invisible above [ingest]/[flush].

    Because [ingest] never blocks and the drop happens after 50 acks, the flush at
    the end is what forces the driver through the reconnect+replay before returning. *)

module Z = Zerobus.Driver
module Opt = Zerobus_core.Options

let ( let* ) = Lwt.bind
let n_records = 200
let port = ref 0

let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "ephemeral_server_drop.exe" in
  if Sys.file_exists cand then cand else "./ephemeral_server_drop.exe"

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

type result = { flushed_ok : bool; acks_seen : int; err : string option }

let run () : result Lwt.t =
  Zerobus.Io_lwt_for_test.Scope.with_scope (fun scope ->
      let acks_seen = ref 0 in
      (* count distinct watermark advances via the ack callback *)
      let ack_callback =
        {
          Opt.on_ack = (fun _ -> incr acks_seen);
          on_error = (fun _ _ -> ());
        }
      in
      let options =
        {
          Opt.default_stream_options with
          Opt.record_type = Opt.Json;
          ack_callback = Some ack_callback;
          (* keep the test snappy *)
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
      | Error e -> Lwt.return { flushed_ok = false; acks_seen = 0; err = Some (Zerobus_core.Error.to_string e) }
      | Ok stream ->
          let rec loop i =
            if i >= n_records then Lwt.return (Ok ())
            else
              let* r = Z.ingest stream (Bytes.of_string (Printf.sprintf {|{"id":%d}|} i)) in
              match r with Ok _ -> loop (i + 1) | Error _ as e -> Lwt.return e
          in
          let* ing = loop 0 in
          (match ing with
           | Error e -> Lwt.return { flushed_ok = false; acks_seen = !acks_seen; err = Some (Zerobus_core.Error.to_string e) }
           | Ok () ->
               let* flush_r = Z.flush stream in
               let* _ = Z.close stream in
               Lwt.return
                 {
                   flushed_ok = (match flush_r with Ok () -> true | Error _ -> false);
                   acks_seen = !acks_seen;
                   err = (match flush_r with Error e -> Some (Zerobus_core.Error.to_string e) | Ok () -> None);
                 }))

(* Bound the whole thing so a recovery bug reports instead of hanging. *)
let run_bounded () =
  Lwt.pick
    [ run ();
      (let* () = Lwt_unix.sleep 30.0 in
       Lwt.return { flushed_ok = false; acks_seen = -1; err = Some "TIMEOUT" }) ]

let result = lazy (Lwt_main.run (with_server run_bounded))

let () =
  Alcotest.run "phase6-recovery"
    [
      ( "reconnect-and-replay",
        [
          Alcotest.test_case "no fatal error (recovered)" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check (option string)) "err" None r.err);
          Alcotest.test_case "flush succeeded after recovery" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "flushed" true r.flushed_ok);
          Alcotest.test_case "acks observed (watermark advanced)" `Slow (fun () ->
              let r = Lazy.force result in
              (* at least one ack watermark fired via the callback, and no TIMEOUT
                 sentinel (-1) *)
              Alcotest.(check bool) "acks_seen > 0" true (r.acks_seen > 0));
        ] );
    ]
