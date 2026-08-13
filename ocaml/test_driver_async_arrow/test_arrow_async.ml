(** Async Arrow acceptance: the PUBLIC {!Zerobus_async} façade auto-selects the
    Arrow Flight [DoPut] driver when [record_type = Arrow], with REAL Arrow IPC
    bodies from the {!Zerobus_arrow} codec, against the shared Arrow/Flight mock
    server. The Async counterpart of [test_driver_arrow/test_driver_arrow.ml]
    (Lwt) and proves Async now has Arrow parity with Lwt/Eio.

    Reuses the SAME separate-process mock ([flight_arrow_server.exe], built by
    [test_driver_arrow/]) — the mock is runtime-agnostic (it just speaks
    Flight), so only the client runtime differs. Cleartext h2c (tls:false). Runs
    on fl414; needs PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig (libarrow, for
    the codec). *)

open! Core
open! Async
module Zb = Zerobus_async
module Opt = Zerobus_core.Options

let n_records = 50
let port = ref 0

let server_exe () =
  (* Built by the sibling test_driver_arrow/ and copied in as a dune dep. *)
  let dir = Filename.dirname (Sys.get_argv ()).(0) in
  let cand = Filename.concat dir "flight_arrow_server.exe" in
  if Sys_unix.file_exists_exn cand then cand else "./flight_arrow_server.exe"

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

let run () : result Deferred.t =
  let%bind client_r =
    Zb.create
      ~endpoint:(Printf.sprintf "127.0.0.1:%d" !port)
      ~workspace_url:"https://adb-1234567890.11.azuredatabricks.net" ()
  in
  match client_r with
  | Error e ->
      return
        {
          acked_last = false;
          issued = 0;
          err = Some (Zerobus_core.Error.to_string e);
        }
  | Ok client -> (
      (* Arrow: carry the schema IPC message in the descriptor (Flight create_frame
         sends it as the first FlightData's data_header). *)
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
      (* record_type = Arrow → the façade auto-selects the Flight DoPut driver. *)
      let options =
        { Opt.default_stream_options with Opt.record_type = Opt.Arrow }
      in
      let headers_provider () =
        return
          (Ok
             [
               ("authorization", "Bearer mock-token");
               ("x-databricks-zerobus-table-name", table.Opt.table_name);
             ])
      in
      let%map r =
        Zb.with_stream ~tls:false client table ~headers_provider ~options
          (fun stream ->
            (* Each record is REAL Arrow IPC (one row/batch) from zerobus-arrow;
               loop-then-flush. *)
            let%bind ingested =
              Deferred.repeat_until_finished (0, None) (fun (i, last) ->
                  if i >= n_records then return (`Finished (Ok last))
                  else
                    match
                      Zerobus_arrow.encode
                        [
                          {
                            Zerobus_arrow.id = i;
                            name = Printf.sprintf "row-%d" i;
                          };
                        ]
                    with
                    | Error e ->
                        return
                          (`Finished
                             (Error
                                (Zerobus_core.Error.Stream_error
                                   ("arrow encode: " ^ e))))
                    | Ok ipc -> (
                        let%map r = Zb.ingest stream ipc in
                        match r with
                        | Ok off -> `Repeat (i + 1, Some off)
                        | Error _ as e -> `Finished e))
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
                let%map flush_r = Zb.flush stream in
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
                })
      in
      match r with
      | Ok res -> res
      | Error e ->
          {
            acked_last = false;
            issued = 0;
            err = Some (Zerobus_core.Error.to_string e);
          })

let result : result = Thread_safe.block_on_async_exn (fun () -> with_server run)

let () =
  Core.eprintf
    "ZEROBUS OCAML — ASYNC PUBLIC FACADE auto-selects Arrow/Flight -- evidence\n\
     entry          : Zerobus_async.with_stream (record_type=Arrow)\n\
     driver         : auto-selected Flight DoPut (Zerobus_core.Flight_protocol)\n\
     codec          : zerobus-arrow (real libarrow), caller-side encode\n\
     records_issued : %d\n\
     flush_all_acked: %b\n\
     error          : %s\n\
     %!"
    result.issued result.acked_last
    (Option.value result.err ~default:"none");
  Alcotest.run "async-facade-arrow"
    [
      ( "public facade auto-selects arrow/flight",
        [
          Alcotest.test_case "no error" `Slow (fun () ->
              Alcotest.(check (option string)) "err" None result.err);
          Alcotest.test_case "all records issued" `Slow (fun () ->
              Alcotest.(check int) "issued" n_records result.issued);
          Alcotest.test_case "flush confirms all acked over flight" `Slow
            (fun () -> Alcotest.(check bool) "acked" true result.acked_last);
        ] );
    ]
