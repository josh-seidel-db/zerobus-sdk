(** Mock Zerobus-shaped ingest server for the recovery spike — a standalone
    process (the proven separate-process topology from spike-flight / spike-eio).

    It implements [ingest.Ingest/IngestStream] as a bidi gRPC stream and
    deliberately breaks the FIRST stream mid-way to force client recovery:

    - It acks every record by the record's own [client_seq] (NOT a per-connection
      counter). This is the crucial property real recovery depends on: a replayed
      record must be acked under its original offset, so the client's un-acked set
      shrinks monotonically across reconnects. (The original in-process draft acked
      by a per-connection counter reset to 0 each reconnect, so the un-acked tail
      never shrank — an infinite reconnect loop. That was the hang.)
    - On the first connection, after acking [drop_after_n] records, it ends the
      stream with a RETRYABLE gRPC status (Unavailable) rather than a clean OK — a
      real server-initiated recoverable break, exactly what Zerobus recovery is for.
    - It remembers (process-global, across connections) that it has already dropped
      once, so the client's reconnected stream is served to completion.

    The client must therefore detect the non-OK stream end, reconnect, and replay
    only the un-acked tail — ending with every offset acked exactly once, in order. *)

module Ingest = Ingest_proto.Ingest

let drop_after_n = 50

(* Process-global: survives across the sequential client connections so only the
   first stream drops. *)
let already_dropped = ref false

let decode_request (s : string) : Ingest.ingest_request =
  Ingest.decode_pb_ingest_request (Pbrt.Decoder.of_string s)

let encode_response (r : Ingest.ingest_response) : string =
  let enc = Pbrt.Encoder.create () in
  Ingest.encode_pb_ingest_response r enc;
  Pbrt.Encoder.to_string enc

let handle_ingest_stream (requests : string Lwt_stream.t)
    (respond : string -> unit) : Grpc.Status.t Lwt.t =
  let open Lwt.Syntax in
  (* Whether THIS connection is the one that will drop: only if we have not
     already dropped on an earlier connection. *)
  let will_drop = not !already_dropped in
  let acked_this_conn = ref 0 in
  let status = ref (Grpc.Status.v Grpc.Status.OK) in
  let* () =
    Lwt.catch
      (fun () ->
        Lwt_stream.iter_s
          (fun raw_req ->
            let req = decode_request raw_req in
            (* Ack by the record's own client_seq — this is the durable offset. *)
            let offset = req.Ingest.client_seq in
            let resp = Ingest.make_ingest_response ~offset ~durable:true () in
            respond (encode_response resp);
            incr acked_this_conn;
            if will_drop && !acked_this_conn >= drop_after_n then begin
              already_dropped := true;
              status := Grpc.Status.v Grpc.Status.Unavailable;
              (* Abort the stream: stop consuming and surface a retryable status.
                 [Exit] unwinds out of the iter; the catch below swallows it. *)
              Lwt.fail Exit
            end
            else Lwt.return_unit)
          requests)
      (function Exit -> Lwt.return_unit | exn -> Lwt.fail exn)
  in
  Lwt.return !status

let grpc_server () =
  let service =
    Grpc_lwt.Server.Service.(
      v ()
      |> add_rpc ~name:"IngestStream"
           ~rpc:(Grpc_lwt.Server.Rpc.Bidirectional_streaming handle_ingest_stream)
      |> handle_request)
  in
  Grpc_lwt.Server.(v () |> add_service ~name:"ingest.Ingest" ~service)

let () =
  let port =
    if Array.length Sys.argv > 1 && Sys.argv.(1) <> "0" then
      int_of_string Sys.argv.(1)
    else 50561
  in
  Lwt_main.run
    (let open Lwt.Syntax in
     let server = grpc_server () in
     let listen_address = Unix.(ADDR_INET (inet_addr_loopback, port)) in
     let* _server_socket =
       Lwt_io.establish_server_with_client_socket
         ~prepare_listening_fd:(fun fd ->
           Lwt_unix.setsockopt fd Unix.SO_REUSEADDR true)
         listen_address
         (fun _addr socket ->
           H2_lwt_unix.Server.create_connection_handler ?config:None
             ~request_handler:(fun _ reqd ->
               Grpc_lwt.Server.handle_request server reqd)
             ~error_handler:(fun _ ?request:_ _ _ -> ())
             _addr socket)
     in
     Printf.printf "READY %d\n%!" port;
     let forever, _ = Lwt.wait () in
     forever)
