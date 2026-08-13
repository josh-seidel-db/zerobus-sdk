(** Cross-implementation OTLP mock collector (Lwt) — decodes our exporter's
    Export requests with the {b canonical} [opentelemetry.proto] wire types (the
    upstream OCaml OpenTelemetry package's generated protos), NOT our vendored
    [Zerobus_otlp_proto]. A standalone process, cleartext h2c on loopback.

    This is the wire-compatibility proof the plain [test_otlp] mock cannot give:
    there, both sides use our own [Zerobus_otlp_proto], so a symmetric-codec bug
    would pass. Here the request is encoded by {!Zerobus_otlp} (our protos) and
    decoded by [Opentelemetry_proto] (the independent, upstream-generated
    protos). If the canonical decoder reads our bytes correctly — right
    log-record count, severity, body — our OTLP framing is genuinely
    interoperable.

    The response is likewise built + encoded with the canonical types, so our
    exporter's decoder is validated against canonical-encoded bytes too. A
    request whose first resource carries schema_url "REJECT1" comes back
    reporting 1 rejected record (the partial-success path).

    Received counts are written to $ZEROBUS_OTEL_COLLECTOR_LOG (if set) for
    durable round-trip evidence. *)

module LSvc = Opentelemetry_proto.Logs_service
module MSvc = Opentelemetry_proto.Metrics_service
module OLogs = Opentelemetry_proto.Logs
module OMetrics = Opentelemetry_proto.Metrics

let ( let* ) = Lwt.bind

let log_line s =
  match Sys.getenv_opt "ZEROBUS_OTEL_COLLECTOR_LOG" with
  | Some path -> (
      try
        let oc = open_out_gen [ Open_append; Open_creat ] 0o644 path in
        output_string oc (s ^ "\n");
        close_out oc
      with _ -> ())
  | None -> ()

(* Count log records across all resource/scope groupings, and whether the first
   resource requests rejection. Reads the CANONICAL decoded structure. *)
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
  (* DECODE with the canonical opentelemetry.proto types. *)
  let req =
    LSvc.decode_pb_export_logs_service_request (Pbrt.Decoder.of_string raw)
  in
  let n, reject = count_logs req in
  (* Also read a field's content to prove structural decode (severity_text of the
     first record), not just the count. *)
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
  Lwt.return (Grpc.Status.v Grpc.Status.OK, Some (Pbrt.Encoder.to_string enc))

let metrics_export : Grpc_lwt.Server.Rpc.unary =
 fun raw ->
  let req =
    MSvc.decode_pb_export_metrics_service_request (Pbrt.Decoder.of_string raw)
  in
  let n, reject = count_metrics req in
  log_line
    (Printf.sprintf "MetricsService/Export canonical-decoded %d metric(s)%s" n
       (if reject then " (REJECT1)" else ""));
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
