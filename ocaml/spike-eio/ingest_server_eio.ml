(** Mock Zerobus ingest server for the Eio spike -- a standalone process.

    Runs its own Eio event loop and grpc-eio bidi handler: for each incoming
    IngestRequest it emits one IngestResponse with a monotonic offset. The spike
    test driver (bidi_spike_eio.ml) spawns this as a subprocess, because
    grpc-eio 0.2.0 client+server only interoperate across separate processes
    (see README "Eio finding"). Listen port is argv.(1). *)

module Ingest = Ingest_proto.Ingest

let decode_request (s : string) : Ingest.ingest_request =
  Ingest.decode_pb_ingest_request (Pbrt.Decoder.of_string s)

let encode_response (r : Ingest.ingest_response) : string =
  let enc = Pbrt.Encoder.create () in
  Ingest.encode_pb_ingest_response r enc;
  Pbrt.Encoder.to_string enc

let handle_ingest_stream (requests : string Seq.t) (respond : string -> unit) :
    Grpc.Status.t =
  let offset = ref 0L in
  Seq.iter
    (fun raw_req ->
      let (_ : Ingest.ingest_request) = decode_request raw_req in
      let resp = Ingest.make_ingest_response ~offset:!offset ~durable:true () in
      offset := Int64.add !offset 1L;
      respond (encode_response resp))
    requests;
  Grpc.Status.v Grpc.Status.OK

let grpc_server () =
  let service =
    Grpc_eio.Server.Service.(
      v ()
      |> add_rpc ~name:"IngestStream"
           ~rpc:(Grpc_eio.Server.Rpc.Bidirectional_streaming handle_ingest_stream)
      |> handle_request)
  in
  Grpc_eio.Server.(v () |> add_service ~name:"ingest.Ingest" ~service)

let () =
  (* Port from argv.(1); 0 (default) asks the OS for a free port, which we then
     report back so the client connects to the right one -- no collisions. *)
  let requested_port =
    if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 0
  in
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let server = grpc_server () in
  let listening =
    Eio.Net.listen ~sw ~backlog:10 ~reuse_addr:true net
      (`Tcp (Eio.Net.Ipaddr.V4.loopback, requested_port))
  in
  (* Discover the actually-bound port and signal readiness to the parent. *)
  let bound_port =
    match Eio.Net.listening_addr listening with
    | `Tcp (_, p) -> p
    | `Unix _ -> requested_port
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
