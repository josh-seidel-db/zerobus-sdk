(** Mock Zerobus [EphemeralStream] server for the Eio driver test — a standalone
    process (the proven separate-process topology, required by the grpc-eio 0.2.0
    client-flush finding), cleartext h2c on loopback.

    Identical wire behaviour to [ephemeral_server.ml] (the Lwt mock), just on the
    Eio grpc/h2 stack so it links on the OCaml-5 [zbeio] switch (which has no
    grpc-lwt): speaks the REAL vendored proto — on [CreateIngestStreamRequest] it
    replies with a [CreateIngestStreamResponse] (a stream_id); for each
    [IngestRecordRequest] it advances a durability watermark and replies with an
    [IngestRecordResponse] carrying [durability_ack_up_to_offset].

    This is the backend the runtime-agnostic driver ({!Zerobus_core.Make}) runs
    against on Eio, exercising create/ingest/flush end to end without a live
    service. *)

module Z = Zerobus_proto.Zerobus_service

let decode_req (s : string) : Z.ephemeral_stream_request =
  Z.decode_pb_ephemeral_stream_request (Pbrt.Decoder.of_string s)

let encode_resp (r : Z.ephemeral_stream_response) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_response r e;
  Pbrt.Encoder.to_string e

let handle (requests : string Seq.t) (respond : string -> unit) : Grpc.Status.t =
  let watermark = ref (-1L) in
  Seq.iter
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
          if r.Z.offset_id > !watermark then watermark := r.Z.offset_id;
          let resp =
            Z.Ingest_record_response
              (Z.make_ingest_record_response
                 ~durability_ack_up_to_offset:!watermark ())
          in
          respond (encode_resp resp)
      | Z.Ingest_record_batch _ -> ())
    requests;
  Grpc.Status.v Grpc.Status.OK

let grpc_server () =
  let service =
    Grpc_eio.Server.Service.(
      v ()
      |> add_rpc ~name:"EphemeralStream"
           ~rpc:(Grpc_eio.Server.Rpc.Bidirectional_streaming handle)
      |> handle_request)
  in
  Grpc_eio.Server.(
    v () |> add_service ~name:"databricks.zerobus.Zerobus" ~service)

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
