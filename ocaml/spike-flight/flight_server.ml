(** Mock Zerobus Arrow-Flight/DoPut server for the spike — a standalone process.

    Implements arrow.flight.protocol.FlightService/DoPut as a bidi gRPC stream:
    consumes FlightData messages and, for each record-batch message (one that
    carries a data_body), returns a PutResult whose app_metadata holds the
    durable offset — exactly the shape rust/sdk/src/stream/arrow uses (offset in
    FlightData.app_metadata on the way in, ack in PutResult.app_metadata on the
    way out). The first FlightData is the schema message (flight_descriptor +
    data_header, no data_body) and is not acked, mirroring Arrow IPC framing.

    The data_body is treated as OPAQUE bytes — no Arrow library is linked. *)

module F = Flight_proto.Flight

let decode_flight_data (s : string) : F.flight_data =
  F.decode_pb_flight_data (Pbrt.Decoder.of_string s)

let encode_put_result (r : F.put_result) : string =
  let enc = Pbrt.Encoder.create () in
  F.encode_pb_put_result r enc;
  Pbrt.Encoder.to_string enc

(* offset is carried as decimal text in app_metadata (JSON in real Zerobus; a
   plain int here keeps the spike free of a JSON dep — the mechanism is identical). *)
let offset_of_metadata (b : bytes) : int64 option =
  match Bytes.to_string b with
  | "" -> None
  | s -> ( try Some (Int64.of_string (String.trim s)) with _ -> None)

let handle_do_put (requests : string Lwt_stream.t) (respond : string -> unit) :
    Grpc.Status.t Lwt.t =
  let open Lwt.Syntax in
  let batches = ref 0 in
  let* () =
    Lwt_stream.iter
      (fun raw ->
        let fd = decode_flight_data raw in
        (* Schema message (has data_header, empty data_body) is not acked. *)
        if Bytes.length fd.F.data_body > 0 then begin
          incr batches;
          let off =
            match offset_of_metadata fd.F.app_metadata with
            | Some o -> o
            | None -> Int64.of_int (!batches - 1)
          in
          (* durable ack: echo "<offset>:<body-len>:<body-sum>" so the client can
             verify the opaque data_body arrived byte-exact through the proto. A
             per-byte checksum over the full data_body detects any truncation or
             corruption of the (unmodeled) Arrow IPC payload. *)
          let sum = ref 0 in
          Bytes.iter (fun c -> sum := (!sum + Char.code c) land 0xffffff) fd.F.data_body;
          let ack =
            Printf.sprintf "%Ld:%d:%d" off (Bytes.length fd.F.data_body) !sum
          in
          let pr = F.make_put_result ~app_metadata:(Bytes.of_string ack) () in
          respond (encode_put_result pr)
        end)
      requests
  in
  Lwt.return (Grpc.Status.v Grpc.Status.OK)

let grpc_server () =
  let service =
    Grpc_lwt.Server.Service.(
      v ()
      |> add_rpc ~name:"DoPut"
           ~rpc:(Grpc_lwt.Server.Rpc.Bidirectional_streaming handle_do_put)
      |> handle_request)
  in
  Grpc_lwt.Server.(
    v ()
    |> add_service ~name:"arrow.flight.protocol.FlightService" ~service)

let () =
  (* Fixed loopback port (overridable via argv.(1)); SO_REUSEADDR avoids stale-
     bind flakiness. The driver reads the port from the READY line regardless. *)
  let port =
    if Array.length Sys.argv > 1 && Sys.argv.(1) <> "0" then
      int_of_string Sys.argv.(1)
    else 50560
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
