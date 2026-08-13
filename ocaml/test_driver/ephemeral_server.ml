(** Mock Zerobus [EphemeralStream] server for the Phase 5 driver test — a
    standalone process (the proven separate-process topology), cleartext h2c on
    loopback. Speaks the REAL vendored proto: on [CreateIngestStreamRequest] it
    replies with a [CreateIngestStreamResponse] (a stream_id); for each
    [IngestRecordRequest] it advances a durability watermark and replies with an
    [IngestRecordResponse] carrying [durability_ack_up_to_offset].

    This is the backend the runtime-agnostic driver ({!Zerobus_core.Make}) runs
    against, exercising create/ingest/flush/wait end to end without a live
    service. *)

module Z = Zerobus_proto.Zerobus_service

let decode_req (s : string) : Z.ephemeral_stream_request =
  Z.decode_pb_ephemeral_stream_request (Pbrt.Decoder.of_string s)

let encode_resp (r : Z.ephemeral_stream_response) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_response r e;
  Pbrt.Encoder.to_string e

let handle (requests : string Lwt_stream.t) (respond : string -> unit) :
    Grpc.Status.t Lwt.t =
  let open Lwt.Syntax in
  let watermark = ref (-1L) in
  let* () =
    Lwt_stream.iter
      (fun raw ->
        match decode_req raw with
        | Z.Create_stream (_ : Z.create_ingest_stream_request) ->
            let resp =
              Z.Create_stream_response
                (Z.make_create_ingest_stream_response
                   ~stream_id:"mock-stream-0001" ())
            in
            respond (encode_resp resp)
        | Z.Ingest_record r ->
            (* advance the watermark to this record's offset and ack it *)
            if r.Z.offset_id > !watermark then watermark := r.Z.offset_id;
            let resp =
              Z.Ingest_record_response
                (Z.make_ingest_record_response
                   ~durability_ack_up_to_offset:!watermark ())
            in
            respond (encode_resp resp)
        | Z.Ingest_record_batch _ -> ())
      requests
  in
  Lwt.return (Grpc.Status.v Grpc.Status.OK)

let grpc_server () =
  let service =
    Grpc_lwt.Server.Service.(
      v ()
      |> add_rpc ~name:"EphemeralStream"
           ~rpc:(Grpc_lwt.Server.Rpc.Bidirectional_streaming handle)
      |> handle_request)
  in
  Grpc_lwt.Server.(
    v () |> add_service ~name:"databricks.zerobus.Zerobus" ~service)

let () =
  (* argv.(1) = requested port; 0 (or absent) means OS-assigned ephemeral, which
     avoids EADDRINUSE flakiness across repeated runs. We report the ACTUAL bound
     port on the READY line. *)
  let requested =
    if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 0
  in
  Lwt_main.run
    (let open Lwt.Syntax in
     let server = grpc_server () in
     (* Bind the listening socket manually so we can read back the OS-assigned
        port via getsockname before announcing READY. *)
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
