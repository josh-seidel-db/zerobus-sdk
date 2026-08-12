(** Async OTLP acceptance: {!Zerobus_otlp_async} (the OTLP logs/metrics exporter,
    Async runtime) against a mock OTLP collector (separate process, cleartext h2c)
    + an in-process cohttp-async OIDC token endpoint.

    The Async counterpart of test_otlp/test_otlp_lwt.ml — same coverage, Async
    idioms. Proves the full otlp/grpc flow end-to-end without a live workspace:
    - the exporter mints a table-scoped token (client-credentials grant) and opens
      a TLS-off h2c connection to the collector via the Async H2 client;
    - a batch of OTel logs is framed as a valid ExportLogsServiceRequest, the
      collector DECODES it (proving the wire types + framing), and returns OK;
    - likewise for metrics via MetricsService/Export;
    - a partial-success response (rejected > 0) is surfaced to the caller;
    - an empty batch is a no-op (no RPC), and the token is cached (one mint).

    Cleartext h2c, loopback, ephemeral ports. Runs on a switch with cohttp-async. *)

open! Core
open! Async
module Otlp = Zerobus_otlp_async
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

let table = "main.default.otlp_mock"
let token_mints = ref 0

(* --- in-process cohttp-async OIDC token endpoint (counts mints) --- *)
let start_token_server () : int Deferred.t =
  let handler ~body:_ _addr (req : Cohttp.Request.t) =
    let path = Uri.path (Cohttp.Request.uri req) in
    if String.equal path "/oidc/v1/token" then begin
      Int.incr token_mints;
      Cohttp_async.Server.respond_string ~status:`OK
        {|{"access_token":"tok-otlp-1","token_type":"Bearer","expires_in":3600}|}
    end
    else Cohttp_async.Server.respond_string ~status:`Not_found ""
  in
  let%map server =
    Cohttp_async.Server.create ~on_handler_error:`Ignore
      (Tcp.Where_to_listen.bind_to Tcp.Bind_to_address.Localhost
         Tcp.Bind_to_port.On_port_chosen_by_os)
      handler
  in
  Cohttp_async.Server.listening_on server

(* --- spawn the mock OTLP collector (separate process), await READY --- *)
let collector_exe () =
  let dir = Filename.dirname (Sys.get_argv ()).(0) in
  let cand = Filename.concat dir "otlp_collector_async.exe" in
  if Sys_unix.file_exists_exn cand then cand else "./otlp_collector_async.exe"

(* Spawn the collector via Async's [Process], read its READY line, run [f] with
   the bound port, then kill it. Mirrors test_driver_async's with_server. *)
let with_collector (f : int -> 'a Deferred.t) : 'a Deferred.t =
  let%bind process =
    Process.create_exn ~prog:(collector_exe ()) ~args:[ "0" ] ()
  in
  let stdout_pipe = Reader.lines (Process.stdout process) in
  let%bind ready_line = Pipe.read stdout_pipe in
  let port =
    match ready_line with
    | `Ok line -> (
        match String.split ~on:' ' line with
        | [ "READY"; p ] -> Int.of_string p
        | _ -> failwithf "unexpected collector line: %s" line ())
    | `Eof -> failwith "collector closed before READY"
  in
  Monitor.protect
    (fun () -> f port)
    ~finally:(fun () ->
      Process.send_signal process Signal.kill;
      let%bind _ = Process.wait process in
      return ())

(* --- build a few OTel records --- *)
let sample_logs ?(schema_url = "") () : OLogs.resource_logs list =
  let lr1 = OLogs.make_log_record ~time_unix_nano:1L ~severity_text:"INFO" () in
  let lr2 = OLogs.make_log_record ~time_unix_nano:2L ~severity_text:"WARN" () in
  [
    OLogs.make_resource_logs
      ~scope_logs:[ OLogs.make_scope_logs ~log_records:[ lr1; lr2 ] () ]
      ~schema_url ();
  ]

let sample_metrics ?(schema_url = "") () : OMetrics.resource_metrics list =
  let m1 = OMetrics.make_metric ~name:"req_count" ~unit_:"1" () in
  [
    OMetrics.make_resource_metrics
      ~scope_metrics:[ OMetrics.make_scope_metrics ~metrics:[ m1 ] () ]
      ~schema_url ();
  ]

type outcome = {
  logs_ok : bool;
  logs_rejected : int64;
  metrics_ok : bool;
  metrics_rejected : int64;
  partial_rejected : int64;
  empty_ok : bool;
  mints : int;
}

let run () : outcome Deferred.t =
  let%bind token_port = start_token_server () in
  let token_url = Printf.sprintf "http://127.0.0.1:%d" token_port in
  with_collector (fun port ->
      let endpoint = Printf.sprintf "127.0.0.1:%d" port in
      match
        Otlp.create ~tls:false ~endpoint ~workspace_url:token_url ~table
          ~client_id:"sp-app-id" ~client_secret:"sp-secret" ()
      with
      | Error e -> failwith (Zerobus_core.Error.to_string e)
      | Ok client ->
          let%bind r_logs = Otlp.export_logs client (sample_logs ()) in
          let%bind r_metrics = Otlp.export_metrics client (sample_metrics ()) in
          let%bind r_partial =
            Otlp.export_logs client (sample_logs ~schema_url:"REJECT1" ())
          in
          let%bind r_empty = Otlp.export_logs client [] in
          let ok = function Ok _ -> true | Error _ -> false in
          let rej = function
            | Ok (r : Otlp.export_result) -> r.rejected
            | Error _ -> -1L
          in
          return
            {
              logs_ok = ok r_logs;
              logs_rejected = rej r_logs;
              metrics_ok = ok r_metrics;
              metrics_rejected = rej r_metrics;
              partial_rejected = rej r_partial;
              empty_ok =
                (match r_empty with
                | Ok { rejected = 0L; error_message = "" } -> true
                | _ -> false);
              mints = !token_mints;
            })

let result : outcome = Thread_safe.block_on_async_exn run

let () =
  Core.eprintf
    "ZEROBUS OCAML — ASYNC OTLP TEST -- evidence\n\
     logs_export   : ok=%b rejected=%Ld\n\
     metrics_export: ok=%b rejected=%Ld\n\
     partial_success (REJECT1): rejected=%Ld (expect 1)\n\
     empty_is_noop : %b\n\
     token_mints   : %d (expect 1 — cached across exports)\n%!"
    result.logs_ok result.logs_rejected result.metrics_ok result.metrics_rejected
    result.partial_rejected result.empty_ok result.mints;
  Alcotest.run "async-otlp"
    [
      ( "otlp-export-async",
        [
          Alcotest.test_case "logs Export accepted" `Slow (fun () ->
              Alcotest.(check bool) "logs ok" true result.logs_ok);
          Alcotest.test_case "logs fully accepted (0 rejected)" `Slow (fun () ->
              Alcotest.(check int64) "logs rejected" 0L result.logs_rejected);
          Alcotest.test_case "metrics Export accepted" `Slow (fun () ->
              Alcotest.(check bool) "metrics ok" true result.metrics_ok);
          Alcotest.test_case "metrics fully accepted (0 rejected)" `Slow
            (fun () ->
              Alcotest.(check int64) "metrics rejected" 0L result.metrics_rejected);
          Alcotest.test_case "partial success surfaced (1 rejected)" `Slow
            (fun () ->
              Alcotest.(check int64) "partial" 1L result.partial_rejected);
          Alcotest.test_case "empty batch is a no-op" `Slow (fun () ->
              Alcotest.(check bool) "empty" true result.empty_ok);
          Alcotest.test_case "token cached (mints once)" `Slow (fun () ->
              Alcotest.(check int) "mints" 1 result.mints);
        ] );
    ]
