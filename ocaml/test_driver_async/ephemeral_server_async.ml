(** Mock Zerobus [EphemeralStream] server for the Async driver test — a standalone
    process (the proven separate-process topology), cleartext h2c on loopback.

    Identical wire behaviour to [ephemeral_server.ml] (the Lwt mock) and
    [ephemeral_server_eio.ml], just on the Async grpc/h2 stack: speaks the REAL
    vendored proto — on [CreateIngestStreamRequest] it replies with a
    [CreateIngestStreamResponse] (a stream_id); for each [IngestRecordRequest] it
    advances a durability watermark and replies with an [IngestRecordResponse]
    carrying [durability_ack_up_to_offset].

    This is the backend the runtime-agnostic driver ({!Zerobus_core.Make}) runs
    against on Async, exercising create/ingest/flush end to end without a live
    service. *)

open! Core
open! Async
module Z = Zerobus_proto.Zerobus_service

let decode_req (s : string) : Z.ephemeral_stream_request =
  Z.decode_pb_ephemeral_stream_request (Pbrt.Decoder.of_string s)

let encode_resp (r : Z.ephemeral_stream_response) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_response r e;
  Pbrt.Encoder.to_string e

let handle_stream (requests : string Pipe.Reader.t)
    (responses : string Pipe.Writer.t) : Grpc.Status.t Deferred.t =
  let watermark = ref (-1L) in
  let%bind () =
    Pipe.iter requests ~f:(fun raw ->
        match decode_req raw with
        | Z.Create_stream (_ : Z.create_ingest_stream_request) ->
            let resp =
              Z.Create_stream_response
                (Z.make_create_ingest_stream_response
                   ~stream_id:"mock-stream-0001" ())
            in
            Pipe.write responses (encode_resp resp)
        | Z.Ingest_record r ->
            if Int64.( > ) r.Z.offset_id !watermark then watermark := r.Z.offset_id;
            let resp =
              Z.Ingest_record_response
                (Z.make_ingest_record_response
                   ~durability_ack_up_to_offset:!watermark ())
            in
            Pipe.write responses (encode_resp resp)
        | Z.Ingest_record_batch _ -> return ())
  in
  Pipe.close responses;
  return (Grpc.Status.v Grpc.Status.OK)

let grpc_server () =
  let service =
    Grpc_async.Server.Service.(
      v ()
      |> add_rpc ~name:"EphemeralStream"
           ~rpc:(Grpc_async.Server.Rpc.Bidirectional_streaming handle_stream)
      |> handle_request)
  in
  Grpc_async.Server.(
    v () |> add_service ~name:"databricks.zerobus.Zerobus" ~service)

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
