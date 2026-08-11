(** Mock Zerobus [EphemeralStream] server that accepts the stream but NEVER acks —
    it replies to [CreateStream] with a stream_id, then silently drops every
    [IngestRecord] (no [durability_ack_up_to_offset] ever sent). Used to prove the
    driver's flush/ack-watchdog TIMEOUT: with a short [flush_timeout_ms], [flush]
    must return [Error (Timeout _)] rather than hanging forever.

    Separate-process, cleartext h2c on loopback (the proven topology). *)

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
  let* () =
    Lwt_stream.iter
      (fun raw ->
        match decode_req raw with
        | Z.Create_stream (_ : Z.create_ingest_stream_request) ->
            (* accept the stream so open_stream succeeds ... *)
            respond
              (encode_resp
                 (Z.Create_stream_response
                    (Z.make_create_ingest_stream_response
                       ~stream_id:"mock-noack-0001" ())))
        (* ... but NEVER ack any record: swallow ingests silently. *)
        | Z.Ingest_record _ -> ()
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
