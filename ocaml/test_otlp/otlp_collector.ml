(** Mock OTLP collector for the Phase 7d exporter test — a standalone process
    (the proven separate-process topology), cleartext h2c on loopback. Speaks
    the REAL vendored OpenTelemetry collector protos: two unary [Export] RPCs
    - opentelemetry.proto.collector.logs.v1.LogsService/Export
    - opentelemetry.proto.collector.metrics.v1.MetricsService/Export

    It DECODES each request (proving the exporter framed valid OTLP protobuf),
    counts the log records / data points it received, and replies with an
    Export*ServiceResponse. To exercise the partial-success path, a request
    whose first resource carries the schema_url "REJECT1" comes back reporting 1
    rejected record.

    The received counts are echoed on stderr so the test can eyeball them; the
    exporter asserts on the decoded response (rejected count). *)

module LSvc = Zerobus_otlp_proto.Logs_service
module MSvc = Zerobus_otlp_proto.Metrics_service
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

let ( let* ) = Lwt.bind

(* Optional decode-evidence log: if ZEROBUS_OTLP_COLLECTOR_LOG is set, append the
   per-Export decode counts there (stderr is captured/redirected under alcotest,
   so a file gives durable round-trip evidence). *)
let log_line s =
  match Sys.getenv_opt "ZEROBUS_OTLP_COLLECTOR_LOG" with
  | Some path -> (
      try
        let oc = open_out_gen [ Open_append; Open_creat ] 0o644 path in
        output_string oc (s ^ "\n");
        close_out oc
      with _ -> ())
  | None -> ()

(* count the log records across all resource/scope groupings *)
let count_logs (req : LSvc.export_logs_service_request) : int * bool =
  let reject =
    match req.LSvc.resource_logs with
    | rl :: _ -> rl.OLogs.schema_url = "REJECT1"
    | [] -> false
  in
  let n =
    List.fold_left
      (fun acc (rl : OLogs.resource_logs) ->
        List.fold_left
          (fun acc (sl : OLogs.scope_logs) ->
            acc + List.length sl.OLogs.log_records)
          acc rl.OLogs.scope_logs)
      0 req.LSvc.resource_logs
  in
  (n, reject)

let count_metrics (req : MSvc.export_metrics_service_request) : int * bool =
  let reject =
    match req.MSvc.resource_metrics with
    | rm :: _ -> rm.OMetrics.schema_url = "REJECT1"
    | [] -> false
  in
  let n =
    List.fold_left
      (fun acc (rm : OMetrics.resource_metrics) ->
        List.fold_left
          (fun acc (sm : OMetrics.scope_metrics) ->
            acc + List.length sm.OMetrics.metrics)
          acc rm.OMetrics.scope_metrics)
      0 req.MSvc.resource_metrics
  in
  (n, reject)

let logs_export : Grpc_lwt.Server.Rpc.unary =
 fun raw ->
  let req =
    LSvc.decode_pb_export_logs_service_request (Pbrt.Decoder.of_string raw)
  in
  let n, reject = count_logs req in
  log_line
    (Printf.sprintf "LogsService/Export decoded %d log record(s)%s" n
       (if reject then " (REJECT1)" else ""));
  let resp =
    if reject then
      LSvc.make_export_logs_service_response
        ~partial_success:
          (LSvc.make_export_logs_partial_success ~rejected_log_records:1L
             ~error_message:"1 rejected (mock)" ())
        ()
    else LSvc.make_export_logs_service_response ()
  in
  let enc = Pbrt.Encoder.create () in
  LSvc.encode_pb_export_logs_service_response resp enc;
  Lwt.return (Grpc.Status.v Grpc.Status.OK, Some (Pbrt.Encoder.to_string enc))

let metrics_export : Grpc_lwt.Server.Rpc.unary =
 fun raw ->
  let req =
    MSvc.decode_pb_export_metrics_service_request (Pbrt.Decoder.of_string raw)
  in
  let n, reject = count_metrics req in
  log_line
    (Printf.sprintf "MetricsService/Export decoded %d metric(s)%s" n
       (if reject then " (REJECT1)" else ""));
  let resp =
    if reject then
      MSvc.make_export_metrics_service_response
        ~partial_success:
          (MSvc.make_export_metrics_partial_success ~rejected_data_points:1L
             ~error_message:"1 rejected (mock)" ())
        ()
    else MSvc.make_export_metrics_service_response ()
  in
  let enc = Pbrt.Encoder.create () in
  MSvc.encode_pb_export_metrics_service_response resp enc;
  Lwt.return (Grpc.Status.v Grpc.Status.OK, Some (Pbrt.Encoder.to_string enc))

let grpc_server () =
  let logs_service =
    Grpc_lwt.Server.Service.(
      v ()
      |> add_rpc ~name:"Export" ~rpc:(Grpc_lwt.Server.Rpc.Unary logs_export)
      |> handle_request)
  in
  let metrics_service =
    Grpc_lwt.Server.Service.(
      v ()
      |> add_rpc ~name:"Export" ~rpc:(Grpc_lwt.Server.Rpc.Unary metrics_export)
      |> handle_request)
  in
  Grpc_lwt.Server.(
    v ()
    |> add_service ~name:"opentelemetry.proto.collector.logs.v1.LogsService"
         ~service:logs_service
    |> add_service
         ~name:"opentelemetry.proto.collector.metrics.v1.MetricsService"
         ~service:metrics_service)

let () =
  let requested =
    if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 0
  in
  Lwt_main.run
    (let server = grpc_server () in
     let listen_fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
     Lwt_unix.setsockopt listen_fd Unix.SO_REUSEADDR true;
     let* () =
       Lwt_unix.bind listen_fd Unix.(ADDR_INET (inet_addr_loopback, requested))
     in
     Lwt_unix.listen listen_fd 128;
     let actual_port =
       match Lwt_unix.getsockname listen_fd with
       | Unix.ADDR_INET (_, p) -> p
       | _ -> requested
     in
     let handler =
       H2_lwt_unix.Server.create_connection_handler ?config:None
         ~request_handler:(fun _ reqd ->
           Grpc_lwt.Server.handle_request server reqd)
         ~error_handler:(fun _ ?request:_ _ _ -> ())
     in
     Printf.printf "READY %d\n%!" actual_port;
     let rec accept_loop () =
       let* client_fd, client_addr = Lwt_unix.accept listen_fd in
       Lwt.async (fun () -> handler client_addr client_fd);
       accept_loop ()
     in
     accept_loop ())
