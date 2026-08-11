(** Mock Zerobus [EphemeralStream] server that DROPS the first stream mid-way, to
    exercise the driver's recovery (§12.3). Separate process, cleartext h2c.

    Behavior (mirrors the proven recovery spike, now against the real driver):
    - On the FIRST bidi stream: reply to CreateIngestStream, ack records by their
      offset_id, but after [drop_after_n] acked records end the stream with a
      RETRYABLE gRPC status (UNAVAILABLE=14) instead of OK — a recoverable break.
    - A process-global flag ensures only the first stream drops; the reconnected
      stream is served to completion (acks every replayed + new record).

    Acks carry [durability_ack_up_to_offset] = the record's own offset_id, so the
    driver's monotonic watermark advances correctly across the reconnect and the
    un-acked replay set shrinks. *)

module Z = Zerobus_proto.Zerobus_service

let drop_after_n = 50
let already_dropped = ref false

let decode_req (s : string) : Z.ephemeral_stream_request =
  Z.decode_pb_ephemeral_stream_request (Pbrt.Decoder.of_string s)

let encode_resp (r : Z.ephemeral_stream_response) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_response r e;
  Pbrt.Encoder.to_string e

let handle (requests : string Lwt_stream.t) (respond : string -> unit) :
    Grpc.Status.t Lwt.t =
  let open Lwt.Syntax in
  let will_drop = not !already_dropped in
  let acked_this_conn = ref 0 in
  let status = ref (Grpc.Status.v Grpc.Status.OK) in
  let* () =
    Lwt.catch
      (fun () ->
        Lwt_stream.iter
          (fun raw ->
            match decode_req raw with
            | Z.Create_stream _ ->
                respond
                  (encode_resp
                     (Z.Create_stream_response
                        (Z.make_create_ingest_stream_response
                           ~stream_id:"mock-drop-0001" ())))
            | Z.Ingest_record r ->
                if will_drop && !acked_this_conn >= drop_after_n then begin
                  already_dropped := true;
                  status := Grpc.Status.v Grpc.Status.Unavailable;
                  raise Exit
                end;
                respond
                  (encode_resp
                     (Z.Ingest_record_response
                        (Z.make_ingest_record_response
                           ~durability_ack_up_to_offset:r.Z.offset_id ())));
                incr acked_this_conn
            | Z.Ingest_record_batch _ -> ())
          requests)
      (function Exit -> Lwt.return_unit | e -> Lwt.fail e)
  in
  Lwt.return !status

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
  let requested =
    if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 0
  in
  Lwt_main.run
    (let open Lwt.Syntax in
     let server = grpc_server () in
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
         ~request_handler:(fun _ reqd -> Grpc_lwt.Server.handle_request server reqd)
         ~error_handler:(fun _ ?request:_ _ _ -> ())
     in
     Printf.printf "READY %d\n%!" actual_port;
     let rec accept_loop () =
       let* client_fd, client_addr = Lwt_unix.accept listen_fd in
       Lwt.async (fun () -> handler client_addr client_fd);
       accept_loop ()
     in
     accept_loop ())
