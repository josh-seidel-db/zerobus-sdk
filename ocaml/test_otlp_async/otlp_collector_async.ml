(** Mock OTLP collector for the Async exporter test — a standalone process (the
    proven separate-process topology), cleartext h2c on loopback. The Async
    counterpart of test_otlp/otlp_collector.ml: same two unary [Export] RPCs
      - opentelemetry.proto.collector.logs.v1.LogsService/Export
      - opentelemetry.proto.collector.metrics.v1.MetricsService/Export
    on the grpc-async / h2-async stack (standard on fl414, like the async driver
    mock).

    It DECODES each request (proving the exporter framed valid OTLP protobuf) and
    replies with an Export*ServiceResponse. A request whose first resource carries
    the schema_url "REJECT1" comes back reporting 1 rejected record (exercising
    the partial-success path). *)

open! Core
open! Async
module LSvc = Zerobus_otlp_proto.Logs_service
module MSvc = Zerobus_otlp_proto.Metrics_service
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

let logs_export : Grpc_async.Server.Rpc.unary =
 fun raw ->
  let req =
    LSvc.decode_pb_export_logs_service_request (Pbrt.Decoder.of_string raw)
  in
  let reject =
    match req.LSvc.resource_logs with
    | rl :: _ -> String.equal rl.OLogs.schema_url "REJECT1"
    | [] -> false
  in
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
  return (Grpc.Status.v Grpc.Status.OK, Some (Pbrt.Encoder.to_string enc))

let metrics_export : Grpc_async.Server.Rpc.unary =
 fun raw ->
  let req =
    MSvc.decode_pb_export_metrics_service_request (Pbrt.Decoder.of_string raw)
  in
  let reject =
    match req.MSvc.resource_metrics with
    | rm :: _ -> String.equal rm.OMetrics.schema_url "REJECT1"
    | [] -> false
  in
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
  return (Grpc.Status.v Grpc.Status.OK, Some (Pbrt.Encoder.to_string enc))

let grpc_server () =
  let logs_service =
    Grpc_async.Server.Service.(
      v ()
      |> add_rpc ~name:"Export" ~rpc:(Grpc_async.Server.Rpc.Unary logs_export)
      |> handle_request)
  in
  let metrics_service =
    Grpc_async.Server.Service.(
      v ()
      |> add_rpc ~name:"Export" ~rpc:(Grpc_async.Server.Rpc.Unary metrics_export)
      |> handle_request)
  in
  Grpc_async.Server.(
    v ()
    |> add_service ~name:"opentelemetry.proto.collector.logs.v1.LogsService"
         ~service:logs_service
    |> add_service ~name:"opentelemetry.proto.collector.metrics.v1.MetricsService"
         ~service:metrics_service)

let main () =
  let requested_port =
    if Array.length (Sys.get_argv ()) > 1 then Int.of_string (Sys.get_argv ()).(1)
    else 0
  in
  let server = grpc_server () in
  let request_handler _addr reqd = Grpc_async.Server.handle_request server reqd in
  let error_handler _addr ?request:_ _err start_response =
    let body = start_response H2.Headers.empty in
    H2.Body.Writer.close body
  in
  let bind_port =
    if requested_port = 0 then Tcp.Bind_to_port.On_port_chosen_by_os
    else Tcp.Bind_to_port.On_port requested_port
  in
  let where =
    Tcp.Where_to_listen.bind_to Tcp.Bind_to_address.Localhost bind_port
  in
  let%bind server_sock =
    Tcp.Server.create_sock ~on_handler_error:`Ignore where (fun addr sock ->
        H2_async.Server.create_connection_handler ?config:None ~request_handler
          ~error_handler addr sock)
  in
  let bound_port = Tcp.Server.listening_on server_sock in
  printf "READY %d\n%!" bound_port;
  Deferred.never ()

let () =
  don't_wait_for (main ());
  never_returns (Scheduler.go ())
