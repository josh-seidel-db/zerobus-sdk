(** Eio Arrow/Flight acceptance: the PUBLIC {!Zerobus_eio} façade auto-selects
    the Arrow Flight [DoPut] driver when [record_type = Arrow], with REAL Arrow
    IPC bodies from {!Zerobus_arrow}, against the Eio Arrow/Flight mock server.
    The Eio counterpart of test_driver_arrow (Lwt) and test_driver_async_arrow —
    closing the gap where Eio had no offline Arrow test.

    Runs on zbeio (OCaml 5.x). Separate-process mock, cleartext h2c (tls:false).
*)

module Zb = Zerobus_eio
module Opt = Zerobus_core.Options

let n_records = 50

let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "flight_arrow_server_eio.exe" in
  if Sys.file_exists cand then cand else "./flight_arrow_server_eio.exe"

(* Spawn the mock + read READY with the blocking stdlib before entering Eio_main. *)
let with_server (f : int -> 'a) : 'a =
  let exe = server_exe () in
  let stdout_r, stdout_w = Unix.pipe () in
  let pid =
    Unix.create_process exe [| exe; "0" |] Unix.stdin stdout_w Unix.stderr
  in
  Unix.close stdout_w;
  let ic = Unix.in_channel_of_descr stdout_r in
  let port =
    try
      match String.split_on_char ' ' (input_line ic) with
      | [ "READY"; p ] -> int_of_string p
      | _ -> failwith "bad READY line"
    with End_of_file -> failwith "server did not signal READY"
  in
  Fun.protect
    ~finally:(fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      try close_in ic with _ -> ())
    (fun () -> f port)

type result = { acked_last : bool; issued : int; err : string option }

let run ~env ~sw ~port : result =
  match
    Zb.create
      ~endpoint:(Printf.sprintf "127.0.0.1:%d" port)
      ~workspace_url:"https://adb-1234567890.11.azuredatabricks.net" ()
  with
  | Error e ->
      {
        acked_last = false;
        issued = 0;
        err = Some (Zerobus_core.Error.to_string e);
      }
  | Ok client -> (
      let schema =
        match Zerobus_arrow.schema_message () with
        | Ok s -> s
        | Error e -> failwith ("arrow schema_message: " ^ e)
      in
      let table =
        {
          Opt.table_name = "main.default.arrow";
          descriptor = Some (Opt.descriptor_of_bytes schema);
        }
      in
      let options =
        { Opt.default_stream_options with Opt.record_type = Opt.Arrow }
      in
      let headers =
        [
          ("authorization", "Bearer mock-token");
          ("x-databricks-zerobus-table-name", table.Opt.table_name);
        ]
      in
      let r =
        Zb.with_stream ~env ~sw ~tls:false client table ~headers ~options
          (fun stream ->
            let rec loop i last =
              if i >= n_records then Ok last
              else
                match
                  Zerobus_arrow.encode
                    [
                      { Zerobus_arrow.id = i; name = Printf.sprintf "row-%d" i };
                    ]
                with
                | Error e ->
                    Error
                      (Zerobus_core.Error.Stream_error ("arrow encode: " ^ e))
                | Ok ipc -> (
                    match Zb.ingest stream ipc with
                    | Ok off -> loop (i + 1) (Some off)
                    | Error _ as e -> e)
            in
            match loop 0 None with
            | Error e -> Error e
            | Ok last_off -> (
                match Zb.flush stream with
                | Error e -> Error e
                | Ok () -> Ok last_off))
      in
      match Result.join r with
      | Ok last_off ->
          {
            acked_last = (match last_off with Some _ -> true | None -> false);
            issued = n_records;
            err = None;
          }
      | Error e ->
          {
            acked_last = false;
            issued = 0;
            err = Some (Zerobus_core.Error.to_string e);
          })

let result =
  lazy
    (with_server (fun port ->
         Eio_main.run @@ fun env ->
         Eio.Switch.run @@ fun sw ->
         let r = run ~env ~sw ~port in
         Printf.eprintf
           "ZEROBUS OCAML — EIO PUBLIC FACADE auto-selects Arrow/Flight -- \
            evidence\n\
            entry          : Zerobus_eio.with_stream (record_type=Arrow)\n\
            driver         : auto-selected Flight DoPut \
            (Zerobus_core.Flight_protocol)\n\
            codec          : zerobus-arrow (real libarrow), caller-side encode\n\
            records_issued : %d\n\
            flush_all_acked: %b\n\
            error          : %s\n\
            %!"
           r.issued r.acked_last
           (match r.err with Some e -> e | None -> "none");
         r))

let () =
  Alcotest.run "eio-facade-arrow"
    [
      ( "public facade auto-selects arrow/flight",
        [
          Alcotest.test_case "no error" `Slow (fun () ->
              Alcotest.(check (option string))
                "err" None (Lazy.force result).err);
          Alcotest.test_case "all records issued" `Slow (fun () ->
              Alcotest.(check int) "issued" n_records (Lazy.force result).issued);
          Alcotest.test_case "flush confirms all acked over flight" `Slow
            (fun () ->
              Alcotest.(check bool) "acked" true (Lazy.force result).acked_last);
        ] );
    ]
