(** Public-API acceptance on Async: drives the ergonomic {!Zerobus_async} façade
    (create → create_stream_with_headers → ingest/flush/close) end to end against
    the mock [EphemeralStream] server (separate process, cleartext h2c). This is
    the counterpart of the raw-driver [test_driver_async]; it proves the public
    surface, not just the internal driver.

    Runs on fl414. Auth is caller-supplied headers (the façade has no built-in
    OAuth on Async — see the module doc). *)

open! Core
open! Async
module Zb = Zerobus_async

let n_records = 200
let port = ref 0

let server_exe () =
  let dir = Filename.dirname (Sys.get_argv ()).(0) in
  let cand = Filename.concat dir "ephemeral_server_async.exe" in
  if Sys_unix.file_exists_exn cand then cand else "./ephemeral_server_async.exe"

let with_server (f : unit -> 'a Deferred.t) : 'a Deferred.t =
  let%bind process = Process.create_exn ~prog:(server_exe ()) ~args:[ "0" ] () in
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
  (* Build a client pointed at the local mock. [endpoint] is explicit host:port,
     so no workspace-URL derivation is exercised here (that is unit-covered by
     Config); [workspace_url] just satisfies create's signature. *)
  let%bind client_r =
    Zb.create ~endpoint:(Printf.sprintf "127.0.0.1:%d" !port)
      ~workspace_url:"https://adb-1234567890.11.azuredatabricks.net" ()
  in
  match client_r with
  | Error e -> return { acked_last = false; issued = 0; err = Some (Zerobus_core.Error.to_string e) }
  | Ok client ->
      let table = { Zerobus_core.Options.table_name = "main.default.mock"; descriptor = None } in
      let options =
        { Zerobus_core.Options.default_stream_options with record_type = Zerobus_core.Options.Json }
      in
      (* Custom-auth: supply a dummy bearer + table-name header (mock ignores them). *)
      let headers_provider () =
        return
          (Ok
             [ ("authorization", "Bearer mock-token");
               ("x-databricks-zerobus-table-name", table.Zerobus_core.Options.table_name) ])
      in
      let%map r =
        Zb.with_stream ~tls:false client table ~headers_provider ~options
          (fun stream ->
            let%bind ingested =
              Deferred.repeat_until_finished (0, None) (fun (i, last) ->
                  if i >= n_records then return (`Finished (Ok last))
                  else
                    let%map r = Zb.ingest stream (Bytes.of_string (Printf.sprintf {|{"id":%d}|} i)) in
                    match r with
                    | Ok off -> `Repeat (i + 1, Some off)
                    | Error _ as e -> `Finished e)
            in
            match ingested with
            | Error e -> return { acked_last = false; issued = 0; err = Some (Zerobus_core.Error.to_string e) }
            | Ok last_off ->
                let%map flush_r = Zb.flush stream in
                let acked_last = match (flush_r, last_off) with Ok (), Some _ -> true | _ -> false in
                { acked_last; issued = n_records;
                  err = (match flush_r with Error e -> Some (Zerobus_core.Error.to_string e) | Ok () -> None) })
      in
      (match r with
       | Ok res -> res
       | Error e -> { acked_last = false; issued = 0; err = Some (Zerobus_core.Error.to_string e) })

let result : result =
  Thread_safe.block_on_async_exn (fun () -> with_server run)

let () =
  Alcotest.run "phase5-facade-async"
    [
      ( "public-api",
        [
          Alcotest.test_case "no error" `Slow (fun () ->
              Alcotest.(check (option string)) "err" None result.err);
          Alcotest.test_case "all records issued" `Slow (fun () ->
              Alcotest.(check int) "issued" n_records result.issued);
          Alcotest.test_case "flush confirms all acked" `Slow (fun () ->
              Alcotest.(check bool) "acked" true result.acked_last);
        ] );
    ]
