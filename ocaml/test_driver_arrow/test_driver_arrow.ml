(** Phase 7b façade acceptance: the PUBLIC {!Zerobus} API auto-selects the Arrow
    Flight [DoPut] driver when [record_type = Arrow], with REAL Arrow IPC bodies
    from the promoted {!Zerobus_arrow} codec, against the Arrow/Flight mock
    server.

    This exercises the promotion end to end:
    - the Flight [DoPut] {!Stream.PROTOCOL} now lives in [zerobus-core]
      ({!Zerobus_core.Flight_protocol}); the Lwt runtime applies it as a second
      driver ({!Zerobus.Io_lwt_for_test.Stream_flight}),
    - the public façade ([Zerobus.create_stream_with_headers]) picks that driver
      purely from [options.record_type = Arrow] — the caller does not choose a
      protocol,
    - the Arrow codec is the separate optional [zerobus-arrow] package (real
      libarrow); the caller encodes rows with it and hands the IPC [bytes] to
      [ingest], which the façade carries opaquely over Flight.

    Runs on fl414. Separate-process mock, cleartext h2c (tls:false). *)

module Zb = Zerobus
module Opt = Zerobus_core.Options

let ( let* ) = Lwt.bind
let n_records = 50
let port = ref 0

let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "flight_arrow_server.exe" in
  if Sys.file_exists cand then cand else "./flight_arrow_server.exe"

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

let run () : result Lwt.t =
  (* Public client pointed at the local mock (explicit host:port endpoint). *)
  let* client_r =
    Zb.create
      ~endpoint:(Printf.sprintf "127.0.0.1:%d" !port)
      ~workspace_url:"https://adb-1234567890.11.azuredatabricks.net" ()
  in
  match client_r with
  | Error e ->
      Lwt.return
        {
          acked_last = false;
          issued = 0;
          err = Some (Zerobus_core.Error.to_string e);
        }
  | Ok client -> (
      (* Arrow: carry the schema IPC message in descriptor (Flight create_frame
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
      (* Custom-auth (tls:false mock): a dummy bearer + table-name header. *)
      let headers_provider () =
        Lwt.return
          (Ok
             [
               ("authorization", "Bearer mock-token");
               ("x-databricks-zerobus-table-name", table.Opt.table_name);
             ])
      in
      let* stream_r =
        Zb.create_stream_with_headers ~tls:false client table ~headers_provider
          ~options ()
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
          (* Each record is REAL Arrow IPC (one row/batch) from the zerobus-arrow
              codec; loop-then-flush. *)
          let rec loop i last =
            if i >= n_records then Lwt.return (Ok last)
            else
              match
                Zerobus_arrow.encode
                  [ { Zerobus_arrow.id = i; name = Printf.sprintf "row-%d" i } ]
              with
              | Error e ->
                  Lwt.return
                    (Error
                       (Zerobus_core.Error.Stream_error ("arrow encode: " ^ e)))
              | Ok ipc -> (
                  let* r = Zb.ingest stream ipc in
                  match r with
                  | Ok off -> loop (i + 1) (Some off)
                  | Error _ as e -> Lwt.return e)
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
              let* flush_r = Zb.flush stream in
              let* _ = Zb.close stream in
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
            let* r = run () in
            Printf.eprintf
              "ZEROBUS OCAML PHASE 7b — PUBLIC FACADE auto-selects \
               Arrow/Flight -- evidence\n\
               ocaml_version : %s\n\
               entry         : Zerobus.create_stream_with_headers \
               (record_type=Arrow)\n\
               driver        : auto-selected Flight DoPut \
               (Zerobus_core.Flight_protocol)\n\
               codec         : zerobus-arrow (real libarrow), caller-side encode\n\
               records_issued: %d\n\
               flush_all_acked: %b\n\
               error         : %s\n\
               %!"
              Sys.ocaml_version r.issued r.acked_last
              (match r.err with Some e -> e | None -> "none");
            Lwt.return r)))

let () =
  Alcotest.run "phase7b-facade-arrow"
    [
      ( "public facade auto-selects arrow/flight",
        [
          Alcotest.test_case "no error" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check (option string)) "err" None r.err);
          Alcotest.test_case "all records issued" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check int) "issued" n_records r.issued);
          Alcotest.test_case "flush confirms all acked over flight" `Slow
            (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "acked" true r.acked_last);
        ] );
    ]
