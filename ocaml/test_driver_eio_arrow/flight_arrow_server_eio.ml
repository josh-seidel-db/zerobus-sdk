(** Driver-level Arrow-Flight/DoPut mock server — Eio. The Eio counterpart of
    test_driver_arrow/flight_arrow_server.ml, on the grpc-eio / h2-eio stack.
    Links REAL libarrow and decodes each FlightData.data_body via
    {!Zerobus_arrow.decode} (so a corrupt / non-Arrow body is caught), then acks
    the durable offset as JSON in PutResult.app_metadata — the watermark the
    Flight PROTOCOL reads.

    Separate-process topology; cleartext h2c. *)

module F = Zerobus_proto.Flight

let decode_flight_data (s : string) : F.flight_data =
  F.decode_pb_flight_data (Pbrt.Decoder.of_string s)

let encode_put_result (r : F.put_result) : string =
  let enc = Pbrt.Encoder.create () in
  F.encode_pb_put_result r enc;
  Pbrt.Encoder.to_string enc

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

let handle_do_put (requests : string Seq.t) (respond : string -> unit) :
    Grpc.Status.t =
  let watermark = ref (-1L) in
  let seq = ref (-1L) in
  let schema = ref Bytes.empty in
  Seq.iter
    (fun raw ->
      let fd = decode_flight_data raw in
      if Bytes.length fd.F.data_body = 0 then schema := fd.F.data_header
      else begin
        seq := Int64.add !seq 1L;
        let off =
          match offset_of_metadata fd.F.app_metadata with
          | Some o -> o
          | None -> !seq
        in
        let packed = repack fd.F.data_header fd.F.data_body in
        match Zerobus_arrow.decode ~schema_message:!schema packed with
        | Ok _rows ->
            if off > !watermark then watermark := off;
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
    requests;
  Grpc.Status.v Grpc.Status.OK

let grpc_server () =
  let service =
    Grpc_eio.Server.Service.(
      v ()
      |> add_rpc ~name:"DoPut"
           ~rpc:(Grpc_eio.Server.Rpc.Bidirectional_streaming handle_do_put)
      |> handle_request)
  in
  Grpc_eio.Server.(
    v () |> add_service ~name:"arrow.flight.protocol.FlightService" ~service)

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
