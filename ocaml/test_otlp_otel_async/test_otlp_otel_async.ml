(** Cross-implementation OTLP acceptance (Async): our {!Zerobus_otlp_async}
    exporter (vendored [Zerobus_otlp_proto]) against a mock collector that
    decodes with the canonical upstream [opentelemetry.proto] types
    (otel_collector_async). The Async counterpart of
    test_otlp_otel/test_otlp_otel_lwt.ml — proves genuine OTLP
    wire-compatibility for the Async exporter (independent encoder/decoder).

    The mock collector is the SAME separate-process canonical-decoding binary
    the Lwt test uses ([otel_collector_lwt.exe]) — the collector's runtime is
    irrelevant to what's under test (the Async client + our OTLP framing), and
    the combined async-live switch carries grpc-async 0.1.0 (no
    [Grpc_async.Server]), whereas grpc-lwt 0.1.0 does provide a server. So we
    reuse the Lwt collector.

    Cleartext h2c, loopback, ephemeral ports. Runs on a switch with cohttp-async
    \+ tls-async + opentelemetry. *)

open! Core
open! Async
module Otlp = Zerobus_otlp_async
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

let table = "main.default.otlp_otel_mock"
let token_mints = ref 0

let start_token_server () : int Deferred.t =
  let handler ~body:_ _addr (req : Cohttp.Request.t) =
    let path = Uri.path (Cohttp.Request.uri req) in
    if String.equal path "/oidc/v1/token" then begin
      Int.incr token_mints;
      Cohttp_async.Server.respond_string ~status:`OK
        {|{"access_token":"tok-otel-1","token_type":"Bearer","expires_in":3600}|}
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

(* Reuse the Lwt canonical-decoding collector (see the module doc). Look for it
   next to this exe, then in the sibling test_otlp_otel build dir. *)
let collector_exe () =
  let dir = Filename.dirname (Sys.get_argv ()).(0) in
  let candidates =
    [
      Filename.concat dir "otel_collector_lwt.exe";
      Filename.concat dir "../test_otlp_otel/otel_collector_lwt.exe";
      "./otel_collector_lwt.exe";
    ]
  in
  match List.find candidates ~f:(fun p -> Sys_unix.file_exists_exn p) with
  | Some p -> p
  | None -> "./otel_collector_lwt.exe"

let with_collector ~log_path (f : int -> 'a Deferred.t) : 'a Deferred.t =
  let%bind process =
    Process.create_exn ~prog:(collector_exe ()) ~args:[ "0" ]
      ~env:(`Extend [ ("ZEROBUS_OTEL_COLLECTOR_LOG", log_path) ])
      ()
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

let sample_logs ?(schema_url = "") () : OLogs.resource_logs list =
  let lr1 =
    OLogs.make_log_record ~time_unix_nano:111L
      ~severity_number:OLogs.Severity_number_info ~severity_text:"INFO" ()
  in
  let lr2 =
    OLogs.make_log_record ~time_unix_nano:222L
      ~severity_number:OLogs.Severity_number_warn ~severity_text:"WARN" ()
  in
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
  partial_rejected : int64;
  mints : int;
  collector_saw_2_logs : bool;
  collector_saw_info : bool;
}

let read_file p = try In_channel.read_all p with _ -> ""

let run () : outcome Deferred.t =
  let%bind token_port = start_token_server () in
  let token_url = Printf.sprintf "http://127.0.0.1:%d" token_port in
  let log_path = Filename_unix.temp_file "otel_collector_async" ".log" in
  with_collector ~log_path (fun port ->
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
          let%bind () = Clock_ns.after (Time_ns.Span.of_sec 0.2) in
          let clog = read_file log_path in
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
              partial_rejected = rej r_partial;
              mints = !token_mints;
              collector_saw_2_logs =
                String.is_substring clog ~substring:"canonical-decoded 2 record";
              collector_saw_info =
                String.is_substring clog ~substring:"sev0=INFO";
            })

let result : outcome = Thread_safe.block_on_async_exn run

let () =
  Core.eprintf
    "ZEROBUS OCAML — OTLP CROSS-IMPL TEST (Async, canonical opentelemetry.proto)\n\
     logs ok=%b rejected=%Ld ; metrics ok=%b ; partial rejected=%Ld\n\
     token_mints=%d ; canonical-decoded 2 logs=%b ; saw INFO=%b\n\
     %!"
    result.logs_ok result.logs_rejected result.metrics_ok
    result.partial_rejected result.mints result.collector_saw_2_logs
    result.collector_saw_info;
  Alcotest.run "otlp-otel-async"
    [
      ( "cross-impl-export-async",
        [
          Alcotest.test_case "logs Export accepted" `Slow (fun () ->
              Alcotest.(check bool) "ok" true result.logs_ok);
          Alcotest.test_case "logs fully accepted (0 rejected)" `Slow (fun () ->
              Alcotest.(check int64) "rej" 0L result.logs_rejected);
          Alcotest.test_case "metrics Export accepted" `Slow (fun () ->
              Alcotest.(check bool) "ok" true result.metrics_ok);
          Alcotest.test_case "partial success surfaced (1 rejected)" `Slow
            (fun () -> Alcotest.(check int64) "rej" 1L result.partial_rejected);
          Alcotest.test_case "token cached (mints once)" `Slow (fun () ->
              Alcotest.(check int) "mints" 1 result.mints);
          Alcotest.test_case "CANONICAL decoder read 2 log records" `Slow
            (fun () ->
              Alcotest.(check bool) "2 logs" true result.collector_saw_2_logs);
          Alcotest.test_case "CANONICAL decoder read severity INFO" `Slow
            (fun () ->
              Alcotest.(check bool) "sev" true result.collector_saw_info);
        ] );
    ]
