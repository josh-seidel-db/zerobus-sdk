(** Phase 5 acceptance: the real streaming driver ({!Zerobus_core.Make}) over
    the Lwt runtime, against the mock [EphemeralStream] server (separate
    process, cleartext h2c). Proves the loop-then-flush data plane end to end:
    create_stream, ingest N records (queue-only), flush once, assert all acked,
    close.

    Runs on fl414. Separate-process topology (the proven pattern). *)

module Z = Zerobus.Driver
module Opt = Zerobus_core.Options

let ( let* ) = Lwt.bind
let n_records = 200
let port = ref 0

(* --- spawn the mock server, await READY --- *)
let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "ephemeral_server.exe" in
  if Sys.file_exists cand then cand else "./ephemeral_server.exe"

let with_server (f : unit -> 'a Lwt.t) : 'a Lwt.t =
  let exe = server_exe () in
  let stdout_r, stdout_w = Unix.pipe () in
  let pid =
    Unix.create_process exe [| exe; "0" |] Unix.stdin stdout_w Unix.stderr
  in
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

type result = { acked_last : bool; issued : int; err : string option }

let run_driver () : result Lwt.t =
  Zerobus.Io_lwt_for_test.Scope.with_scope (fun scope ->
      let options =
        { Opt.default_stream_options with Opt.record_type = Opt.Json }
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
              acked_last = false;
              issued = 0;
              err = Some (Zerobus_core.Error.to_string e);
            }
      | Ok stream -> (
          (* loop-then-flush: queue N records, do NOT wait per record *)
          let rec loop i last =
            if i >= n_records then Lwt.return (Ok last)
            else
              let* r =
                Z.ingest stream
                  (Bytes.of_string (Printf.sprintf {|{"id":%d}|} i))
              in
              match r with
              | Ok off -> loop (i + 1) (Some off)
              | Error _ as e -> Lwt.return e
          in
          let* ingested = loop 0 None in
          match ingested with
          | Error e ->
              Lwt.return
                {
                  acked_last = false;
                  issued = 0;
                  err = Some (Zerobus_core.Error.to_string e);
                }
          | Ok last_off ->
              (* flush once — wait for all pending acks *)
              let* flush_r = Z.flush stream in
              let* _ = Z.close stream in
              let acked_last =
                match (flush_r, last_off) with
                | Ok (), Some _ -> true
                | _ -> false
              in
              Lwt.return
                {
                  acked_last;
                  issued = n_records;
                  err =
                    (match flush_r with
                    | Error e -> Some (Zerobus_core.Error.to_string e)
                    | Ok () -> None);
                }))

let result =
  lazy
    (Lwt_main.run
       (with_server (fun () ->
            let* r = run_driver () in
            Printf.eprintf
              "ZEROBUS OCAML PHASE 5 DRIVER TEST (Lwt) -- evidence\n\
               ocaml_version : %s\n\
               records_issued: %d\n\
               flush_all_acked: %b\n\
               error         : %s\n\
               %!"
              Sys.ocaml_version r.issued r.acked_last
              (match r.err with Some e -> e | None -> "none");
            Lwt.return r)))

let () =
  Alcotest.run "phase5-driver"
    [
      ( "loop-then-flush",
        [
          Alcotest.test_case "no error" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check (option string)) "err" None r.err);
          Alcotest.test_case "all records issued" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check int) "issued" n_records r.issued);
          Alcotest.test_case "flush confirms all acked" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "acked" true r.acked_last);
        ] );
    ]
