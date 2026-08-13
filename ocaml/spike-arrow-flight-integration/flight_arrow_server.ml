(** Integration mock server: Arrow-Flight/DoPut + REAL Arrow IPC decode.

    Unlike spike-flight/ (which treats data_body as opaque bytes), this server
    LINKS libarrow and actually decodes each FlightData.data_body back into rows
    via {!Arrow_ipc.decode}. It then acks with "<offset>:<nrows>:<idsum>" so the
    client can confirm the server reconstructed the exact rows the client Arrow-
    encoded. This closes the full seam: OCaml columns -> Arrow IPC bytes (client)
    -> FlightData.data_body -> DoPut wire -> Arrow IPC decode -> rows (server).

    Separate-process topology (the proven pattern). Runs on the Lwt switch. *)

module F = Flight_proto.Flight

let decode_flight_data (s : string) : F.flight_data =
  F.decode_pb_flight_data (Pbrt.Decoder.of_string s)

let encode_put_result (r : F.put_result) : string =
  let enc = Pbrt.Encoder.create () in
  F.encode_pb_put_result r enc;
  Pbrt.Encoder.to_string enc

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
        (* Schema message (data_header, empty data_body) is not acked. *)
        if Bytes.length fd.F.data_body > 0 then begin
          incr batches;
          let off =
            match offset_of_metadata fd.F.app_metadata with
            | Some o -> o
            | None -> Int64.of_int (!batches - 1)
          in
          (* REAL Arrow IPC decode of the opaque data_body. If the client sent
             genuine Arrow IPC bytes, this yields the original rows; a corrupt or
             non-Arrow body fails to decode and we ack an error marker instead. *)
          let ack =
            match Arrow_ipc.decode fd.F.data_body with
            | Ok rows ->
                let idsum =
                  List.fold_left (fun a (r : Arrow_ipc.row) -> a + r.id) 0 rows
                in
                Printf.sprintf "%Ld:%d:%d" off (List.length rows) idsum
            | Error e -> Printf.sprintf "%Ld:ARROW_DECODE_ERR:%s" off e
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
    v () |> add_service ~name:"arrow.flight.protocol.FlightService" ~service)

let () =
  (* argv.(1) = requested port; 0 => OS-assigned ephemeral, reported on READY. *)
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
