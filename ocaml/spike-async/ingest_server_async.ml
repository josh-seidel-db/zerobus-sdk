(** Mock Zerobus ingest server for the Async spike -- a standalone process.

    Runs its own Async scheduler and a grpc-async bidi handler: for each incoming
    IngestRequest it emits one IngestResponse with a monotonic offset. Spawned as
    a subprocess by bidi_spike_async.ml (separate-process topology, per the
    grpc-eio in-process finding). Listen port is argv.(1); 0 => OS-assigned, and
    the bound port is printed as "READY <port>". *)

open! Core
open! Async

module Ingest = Ingest_proto.Ingest

let decode_request (s : string) : Ingest.ingest_request =
  Ingest.decode_pb_ingest_request (Pbrt.Decoder.of_string s)

let encode_response (r : Ingest.ingest_response) : string =
  let enc = Pbrt.Encoder.create () in
  Ingest.encode_pb_ingest_response r enc;
  Pbrt.Encoder.to_string enc

(* bidi handler: Pipe.Reader (requests) -> Pipe.Writer (responses) -> status *)
let handle_ingest_stream (requests : string Pipe.Reader.t)
    (responses : string Pipe.Writer.t) : Grpc.Status.t Deferred.t =
  let offset = ref 0L in
  let%bind () =
    Pipe.iter requests ~f:(fun raw_req ->
        let (_ : Ingest.ingest_request) = decode_request raw_req in
        let resp =
          Ingest.make_ingest_response ~offset:!offset ~durable:true ()
        in
        offset := Int64.( + ) !offset 1L;
        Pipe.write responses (encode_response resp))
  in
  Pipe.close responses;
  return (Grpc.Status.v Grpc.Status.OK)

let grpc_server () =
  let service =
    Grpc_async.Server.Service.(
      v ()
      |> add_rpc ~name:"IngestStream"
           ~rpc:
             (Grpc_async.Server.Rpc.Bidirectional_streaming handle_ingest_stream)
      |> handle_request)
  in
  Grpc_async.Server.(v () |> add_service ~name:"ingest.Ingest" ~service)

let main () =
  let requested_port =
    if Array.length (Sys.get_argv ()) > 1 then
      Int.of_string (Sys.get_argv ()).(1)
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
        H2_async.Server.create_connection_handler ?config:None
          ~request_handler ~error_handler addr sock)
  in
  let bound_port = Tcp.Server.listening_on server_sock in
  printf "READY %d\n%!" bound_port;
  Deferred.never ()

let () =
  don't_wait_for (main ());
  never_returns (Scheduler.go ())
