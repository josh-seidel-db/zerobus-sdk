(** Driver-level Arrow-Flight/DoPut mock server. Links REAL libarrow and decodes
    each FlightData.data_body back to rows via {!Zerobus_arrow.decode} (so a
    corrupt or non-Arrow body is caught), then acks the DURABLE OFFSET as a
    plain int64 in PutResult.app_metadata — the watermark the
    {!Zerobus_core.Make_with_protocol} driver's Flight PROTOCOL decodes. This is
    what lets the runtime-generic driver (offset/ack/flush/recovery) run
    unmodified over Flight DoPut.

    Offset in on the way (FlightData.app_metadata, decimal int64) → offset out
    as the watermark (PutResult.app_metadata, decimal int64), monotonic per
    stream. Separate-process topology; cleartext h2c. *)

module F = Zerobus_proto.Flight

let decode_flight_data (s : string) : F.flight_data =
  F.decode_pb_flight_data (Pbrt.Decoder.of_string s)

let encode_put_result (r : F.put_result) : string =
  let enc = Pbrt.Encoder.create () in
  F.encode_pb_put_result r enc;
  Pbrt.Encoder.to_string enc

(* offset_id from app_metadata JSON {"offset_id":N} (or a bare int64). *)
let offset_of_metadata (b : bytes) : int64 option =
  let s = String.trim (Bytes.to_string b) in
  if s = "" then None
  else if s.[0] = '{' then
    match String.index_opt s ':' with
    | Some i ->
        let rest = String.sub s (i + 1) (String.length s - i - 1) in
        let digits =
          String.trim
            (String.map (fun c -> if c = '}' || c = '"' then ' ' else c) rest)
        in
        Int64.of_string_opt (String.trim digits)
    | None -> None
  else Int64.of_string_opt s

(* Re-pack (data_header, data_body) into the [hdrlen][header][body] blob the
   zerobus-arrow codec's decode expects — the inverse of Flight_protocol's split. *)
let repack (header : bytes) (body : bytes) : bytes =
  let hlen = Bytes.length header in
  let out = Bytes.create (4 + hlen + Bytes.length body) in
  Bytes.set out 0 (Char.chr ((hlen lsr 24) land 0xff));
  Bytes.set out 1 (Char.chr ((hlen lsr 16) land 0xff));
  Bytes.set out 2 (Char.chr ((hlen lsr 8) land 0xff));
  Bytes.set out 3 (Char.chr (hlen land 0xff));
  Bytes.blit header 0 out 4 hlen;
  Bytes.blit body 0 out (4 + hlen) (Bytes.length body);
  out

let handle_do_put (requests : string Lwt_stream.t) (respond : string -> unit) :
    Grpc.Status.t Lwt.t =
  let open Lwt.Syntax in
  let watermark = ref (-1L) in
  let seq = ref (-1L) in
  (* fallback offset if app_metadata is absent *)
  let schema = ref Bytes.empty in
  let* () =
    Lwt_stream.iter
      (fun raw ->
        let fd = decode_flight_data raw in
        if Bytes.length fd.F.data_body = 0 then
          (* Schema message: data_header carries the Arrow schema metadata, no
             body. Capture it (not acked); needed to decode later batches. *)
          schema := fd.F.data_header
        else begin
          seq := Int64.add !seq 1L;
          let off =
            match offset_of_metadata fd.F.app_metadata with
            | Some o -> o
            | None -> !seq
          in
          (* REAL Arrow IPC decode from (schema, header+body) — proves the driver
             sent genuine Arrow the server reconstructs. Failure -> ack -1, which
             can never satisfy a flush target >= 0, so the client sees a failure. *)
          let packed = repack fd.F.data_header fd.F.data_body in
          match Zerobus_arrow.decode ~schema_message:!schema packed with
          | Ok _rows ->
              if off > !watermark then watermark := off;
              (* ack the durable watermark as JSON {"offset_id":N} (as the real
                 Zerobus Arrow endpoint does); the Flight PROTOCOL reads it. *)
              let pr =
                F.make_put_result
                  ~app_metadata:
                    (Bytes.of_string
                       (Printf.sprintf
                          {|{"ack_up_to_offset":%Ld,"ack_up_to_records":%Ld}|}
                          !watermark (Int64.add !watermark 1L)))
                  ()
              in
              respond (encode_put_result pr)
          | Error _ ->
              let pr =
                F.make_put_result
                  ~app_metadata:
                    (Bytes.of_string
                       {|{"ack_up_to_offset":-1,"ack_up_to_records":0}|})
                  ()
              in
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
