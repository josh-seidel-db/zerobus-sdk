(** connection_timeout_ms acceptance: [open_stream] must abort a hung connect after
    [connection_timeout_ms] and return [Timeout], rather than blocking on the OS's
    (minutes-long) TCP timeout. This exercises the {!Io.first}-based hard deadline
    wired into the core driver.

    We connect to a non-routable "black hole" address (TEST-NET / RFC5737-style
    unreachable) so the TCP SYN gets no response and the connect would otherwise
    hang for the OS default; a short connection_timeout_ms must win instead. No mock
    server needed. *)

module Z = Zerobus.Driver
module Opt = Zerobus_core.Options

let ( let* ) = Lwt.bind

(* 192.0.2.0/24 is TEST-NET-1 (RFC 5737): guaranteed non-routable, so a connect
   attempt hangs waiting for a SYN-ACK that never comes. *)
let black_hole_host = "192.0.2.1"
let black_hole_port = 443

type result = { timed_out : bool; ms_elapsed : float; err : string option }

let run () : result Lwt.t =
  Zerobus.Io_lwt_for_test.Scope.with_scope (fun scope ->
      let options =
        { Opt.default_stream_options with
          Opt.record_type = Opt.Json;
          connection_timeout_ms = 500  (* half a second — far below any OS TCP timeout *)
        }
      in
      let table = { Opt.table_name = "main.default.mock"; descriptor = None } in
      let t0 = Unix.gettimeofday () in
      let* stream_r =
        Z.open_stream ~host:black_hole_host ~port:black_hole_port ~tls:false
          ~headers:[] ~options ~table scope
      in
      let ms_elapsed = (Unix.gettimeofday () -. t0) *. 1000. in
      match stream_r with
      | Ok _ ->
          Lwt.return { timed_out = false; ms_elapsed; err = Some "unexpectedly connected" }
      | Error (Zerobus_core.Error.Timeout _) ->
          Lwt.return { timed_out = true; ms_elapsed; err = None }
      | Error e ->
          (* Any other error (e.g. an immediate network-unreachable) is NOT the
             deadline we are testing. *)
          Lwt.return { timed_out = false; ms_elapsed; err = Some (Zerobus_core.Error.to_string e) })

(* Bound the whole thing generously so a broken deadline reports rather than hangs. *)
let run_bounded () =
  Lwt.pick
    [ run ();
      (let* () = Lwt_unix.sleep 20.0 in
       Lwt.return { timed_out = false; ms_elapsed = -1.; err = Some "TEST TIMEOUT (deadline did not fire)" }) ]

let result = lazy (Lwt_main.run (run_bounded ()))

let () =
  Alcotest.run "connect-timeout"
    [
      ( "connection_timeout_ms hard deadline",
        [
          Alcotest.test_case "connect times out (not OS-timeout / not TEST-TIMEOUT)" `Slow
            (fun () ->
              let r = Lazy.force result in
              Alcotest.(check (option string)) "err" None r.err;
              Alcotest.(check bool) "timed_out" true r.timed_out);
          Alcotest.test_case "deadline fired promptly (well under the 20s test bound)" `Slow
            (fun () ->
              let r = Lazy.force result in
              (* 500ms deadline; allow generous slack for scheduling, but it must be
                 far below the OS TCP timeout and the 20s test bound. *)
              Alcotest.(check bool) "elapsed < 5s" true (r.ms_elapsed < 5000.));
        ] );
    ]
