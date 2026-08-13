(** Mock OTLP collector for the Eio exporter test — a standalone process (the
    proven separate-process topology), cleartext h2c on loopback. The Eio
    counterpart of test_otlp/otlp_collector.ml: same two unary [Export] RPCs
    - opentelemetry.proto.collector.logs.v1.LogsService/Export
    - opentelemetry.proto.collector.metrics.v1.MetricsService/Export on the
      grpc-eio / h2-eio stack so it links on the OCaml-5 [zbeio] switch.

    It DECODES each request (proving the exporter framed valid OTLP protobuf)
    and replies with an Export*ServiceResponse. A request whose first resource
    carries the schema_url "REJECT1" comes back reporting 1 rejected record
    (exercising the partial-success path). *)

module LSvc = Zerobus_otlp_proto.Logs_service
module MSvc = Zerobus_otlp_proto.Metrics_service
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

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
             ~error_message:"1 rejected (mock)" ())
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
