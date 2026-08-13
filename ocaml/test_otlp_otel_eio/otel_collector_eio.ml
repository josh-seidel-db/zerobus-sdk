(** Cross-implementation OTLP mock collector (Eio) — decodes our exporter's
    Export requests with the {b canonical} upstream [opentelemetry.proto] wire
    types, NOT our vendored [Zerobus_otlp_proto]. The Eio counterpart of
    test_otlp_otel/otel_collector_lwt.ml, on the grpc-eio / h2-eio stack.
    Standalone process, cleartext h2c on loopback.

    Proves genuine OTLP wire-compatibility for the Eio exporter: request encoded
    by {!Zerobus_otlp_eio} (our protos), decoded here by the independent
    canonical protos. A first-resource schema_url of "REJECT1" drives the
    partial-success path (1 rejected). Decode counts go to
    $ZEROBUS_OTEL_COLLECTOR_LOG. *)

module LSvc = Opentelemetry_proto.Logs_service
module MSvc = Opentelemetry_proto.Metrics_service
module OLogs = Opentelemetry_proto.Logs
module OMetrics = Opentelemetry_proto.Metrics

let log_line s =
  match Sys.getenv_opt "ZEROBUS_OTEL_COLLECTOR_LOG" with
  | Some path -> (
      try
        let oc = open_out_gen [ Open_append; Open_creat ] 0o644 path in
        output_string oc (s ^ "\n");
        close_out oc
      with _ -> ())
  | None -> ()

let logs_export : Grpc_eio.Server.Rpc.unary =
 fun raw ->
  let req =
    LSvc.decode_pb_export_logs_service_request (Pbrt.Decoder.of_string raw)
  in
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
  let first_sev =
    match req.LSvc.resource_logs with
    | rl :: _ -> (
        match rl.OLogs.scope_logs with
        | sl :: _ -> (
            match sl.OLogs.log_records with
            | lr :: _ -> lr.OLogs.severity_text
            | [] -> "")
        | [] -> "")
    | [] -> ""
  in
  log_line
    (Printf.sprintf
       "LogsService/Export canonical-decoded %d record(s) sev0=%s%s" n first_sev
       (if reject then " (REJECT1)" else ""));
  let resp =
    if reject then
      LSvc.make_export_logs_service_response
        ~partial_success:
          (LSvc.make_export_logs_partial_success ~rejected_log_records:1L
             ~error_message:"1 rejected (canonical mock)" ())
        ()
    else LSvc.make_export_logs_service_response ()
  in
  let enc = Pbrt.Encoder.create () in
  LSvc.encode_pb_export_logs_service_response resp enc;
  (Grpc.Status.v Grpc.Status.OK, Some (Pbrt.Encoder.to_string enc))

let metrics_export : Grpc_eio.Server.Rpc.unary =
 fun raw ->
  let req =
    MSvc.decode_pb_export_metrics_service_request (Pbrt.Decoder.of_string raw)
  in
  let reject =
    match req.MSvc.resource_metrics with
    | rm :: _ -> rm.OMetrics.schema_url = "REJECT1"
    | [] -> false
  in
  let resp =
    if reject then
      MSvc.make_export_metrics_service_response
        ~partial_success:
          (MSvc.make_export_metrics_partial_success ~rejected_data_points:1L
             ~error_message:"1 rejected (canonical mock)" ())
        ()
    else MSvc.make_export_metrics_service_response ()
  in
  let enc = Pbrt.Encoder.create () in
  MSvc.encode_pb_export_metrics_service_response resp enc;
  (Grpc.Status.v Grpc.Status.OK, Some (Pbrt.Encoder.to_string enc))

let grpc_server () =
  let logs_service =
    Grpc_eio.Server.Service.(
      v ()
      |> add_rpc ~name:"Export" ~rpc:(Grpc_eio.Server.Rpc.Unary logs_export)
      |> handle_request)
  in
  let metrics_service =
    Grpc_eio.Server.Service.(
      v ()
      |> add_rpc ~name:"Export" ~rpc:(Grpc_eio.Server.Rpc.Unary metrics_export)
      |> handle_request)
  in
  Grpc_eio.Server.(
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
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let server = grpc_server () in
  let listening =
    Eio.Net.listen ~sw ~backlog:128 ~reuse_addr:true net
      (`Tcp (Eio.Net.Ipaddr.V4.loopback, requested))
  in
  let bound_port =
    match Eio.Net.listening_addr listening with
    | `Tcp (_, p) -> p
    | `Unix _ -> requested
  in
  Printf.printf "READY %d\n%!" bound_port;
  let rec loop () =
    Eio.Net.accept_fork ~sw listening
      ~on_error:(fun _ -> ())
      (fun socket addr ->
        H2_eio.Server.create_connection_handler ?config:None
          ~request_handler:(fun _addr reqd ->
            Eio.Fiber.fork ~sw (fun () ->
                Grpc_eio.Server.handle_request server reqd))
          ~error_handler:(fun _addr ?request:_ _err _start -> ())
          ~sw addr socket);
    loop ()
  in
  loop ()
