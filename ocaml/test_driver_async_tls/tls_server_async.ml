(** Self-signed TLS h2 mock Zerobus [EphemeralStream] server for the Async
    live-TLS test — a standalone process. Identical proto behaviour to
    [ephemeral_server_async.ml] (the cleartext Async mock), but each accepted
    socket is wrapped in a server-side TLS 1.3 handshake (ALPN h2) using a
    freshly generated self-signed certificate. On startup it prints
    [READY <port> <cert-sha256-b64>] so the client can pin that exact cert (it
    can't use the system trust store for a self-signed cert). This is what
    actually exercises the Async transport's real TLS + ALPN path (the cleartext
    mocks never do).

    {b Why hand-rolled.} On the tls-async-equipped switch, [h2] is pinned to
    0.12 (so the SDK lib compiles), which forces async v0.15 and drags
    [grpc-async] down to 0.1.0 — a version with NO [Grpc_async.Server] module.
    So this mock cannot use the grpc-async server helpers the cleartext mock
    uses; instead it drives the runtime-agnostic {!H2.Server_connection} core
    over the [tls-async] [Reader]/[Writer] duplex itself (the server-side mirror
    of the client's [h2_pump.ml]) and does gRPC length-prefix framing by hand —
    exactly the wire format {!Zerobus_io_async} produces. Runs only where
    tls-async is present. *)

open! Core
open! Async
module Z = Zerobus_proto.Zerobus_service

let decode_req (s : string) : Z.ephemeral_stream_request =
  Z.decode_pb_ephemeral_stream_request (Pbrt.Decoder.of_string s)

let encode_resp (r : Z.ephemeral_stream_response) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_response r e;
  Pbrt.Encoder.to_string e

(* gRPC length-prefix framing (1 compressed-flag byte + 4-byte BE length +
   payload) — the exact wire format {!Zerobus_io_async.grpc_frame} produces. *)
let grpc_frame (msg : string) : string =
  let len = String.length msg in
  let b = Buffer.create (5 + len) in
  Buffer.add_char b '\000';
  Buffer.add_char b (Char.of_int_exn ((len lsr 24) land 0xff));
  Buffer.add_char b (Char.of_int_exn ((len lsr 16) land 0xff));
  Buffer.add_char b (Char.of_int_exn ((len lsr 8) land 0xff));
  Buffer.add_char b (Char.of_int_exn (len land 0xff));
  Buffer.add_string b msg;
  Buffer.contents b

let grpc_deframe (acc : string) : string list * string =
  let rec loop acc msgs =
    if String.length acc < 5 then (List.rev msgs, acc)
    else
      let len =
        (Char.to_int acc.[1] lsl 24)
        lor (Char.to_int acc.[2] lsl 16)
        lor (Char.to_int acc.[3] lsl 8)
        lor Char.to_int acc.[4]
      in
      if String.length acc < 5 + len then (List.rev msgs, acc)
      else
        loop
          (String.sub acc ~pos:(5 + len) ~len:(String.length acc - 5 - len))
          (String.sub acc ~pos:5 ~len :: msgs)
  in
  loop acc []

(* Per-request handler: decode each framed EphemeralStream request, advance the
   durability watermark, and stream back framed responses, then grpc-status:0
   trailers. Identical semantics to [ephemeral_server_async.ml]. *)
let handle_reqd (reqd : H2.Reqd.t) : unit =
  let resp =
    H2.Response.create
      ~headers:
        (H2.Headers.of_list [ ("content-type", "application/grpc+proto") ])
      `OK
  in
  let out = H2.Reqd.respond_with_streaming reqd resp in
  let watermark = ref (-1L) in
  let acc = ref "" in
  let on_eof () =
    H2.Reqd.schedule_trailers reqd (H2.Headers.of_list [ ("grpc-status", "0") ]);
    H2.Body.Writer.close out
  in
  let rec on_read bs ~off ~len =
    acc := !acc ^ Bigstringaf.substring bs ~off ~len;
    let msgs, rest = grpc_deframe !acc in
    acc := rest;
    List.iter msgs ~f:(fun raw ->
        let reply =
          match decode_req raw with
          | Z.Create_stream (_ : Z.create_ingest_stream_request) ->
              Z.Create_stream_response
                (Z.make_create_ingest_stream_response
                   ~stream_id:"mock-stream-tls-async-0001" ())
          | Z.Ingest_record r ->
              if Int64.( > ) r.Z.offset_id !watermark then
                watermark := r.Z.offset_id;
              Z.Ingest_record_response
                (Z.make_ingest_record_response
                   ~durability_ack_up_to_offset:!watermark ())
          | Z.Ingest_record_batch _ ->
              Z.Ingest_record_response
                (Z.make_ingest_record_response
                   ~durability_ack_up_to_offset:!watermark ())
        in
        H2.Body.Writer.write_string out (grpc_frame (encode_resp reply));
        H2.Body.Writer.flush out (fun () -> ()));
    H2.Body.Reader.schedule_read (H2.Reqd.request_body reqd) ~on_read ~on_eof
  in
  H2.Body.Reader.schedule_read (H2.Reqd.request_body reqd) ~on_read ~on_eof

(* Server-side h2 pump: drive [H2.Server_connection] over the cleartext
   Reader/Writer that tls-async hands us post-handshake. Mirror of the client
   [H2_pump] (write loop drains IOVecs to the Writer; read loop feeds chunks in). *)
let serve_connection (reader : Reader.t) (writer : Writer.t) : unit Deferred.t =
  let error_handler ?request:_ _err start_response =
    let body = start_response H2.Headers.empty in
    H2.Body.Writer.close body
  in
  let conn =
    H2.Server_connection.create ?config:None ~error_handler (fun reqd ->
        handle_reqd reqd)
  in
  let rec run_writer () =
    match H2.Server_connection.next_write_operation conn with
    | `Write iovecs ->
        let n =
          List.fold iovecs ~init:0 ~f:(fun acc { Faraday.buffer; off; len } ->
              Writer.write_bigstring writer buffer ~pos:off ~len;
              acc + len)
        in
        H2.Server_connection.report_write_result conn (`Ok n);
        run_writer ()
    | `Yield -> H2.Server_connection.yield_writer conn run_writer
    | `Close _ -> ()
  in
  let read_done =
    Reader.read_one_chunk_at_a_time reader ~handle_chunk:(fun buf ~pos ~len ->
        let consumed =
          match H2.Server_connection.next_read_operation conn with
          | `Read -> H2.Server_connection.read conn buf ~off:pos ~len
          | `Close -> 0
        in
        return (`Consumed (consumed, `Need_unknown)))
  in
  run_writer ();
  let%map _reason = read_done in
  ignore
    (H2.Server_connection.read_eof conn Bigstringaf.empty ~off:0 ~len:0 : int)

(* Generate a fresh self-signed cert + key; return (own_cert, fingerprint-b64). *)
let make_self_signed () : Tls.Config.own_cert * string =
  let key = X509.Private_key.generate `RSA in
  let dn =
    [
      X509.Distinguished_name.(
        Relative_distinguished_name.singleton (CN "localhost"));
    ]
  in
  let csr =
    match X509.Signing_request.create dn key with
    | Ok c -> c
    | Error (`Msg m) -> failwith ("csr: " ^ m)
  in
  let valid_from = Ptime.epoch in
  let valid_until =
    match Ptime.of_float_s (Core_unix.time () +. 86400.) with
    | Some t -> t
    | None -> failwith "ptime"
  in
  let cert =
    match X509.Signing_request.sign csr ~valid_from ~valid_until key dn with
    | Ok c -> c
    | Error _ -> failwith "sign"
  in
  let fp = X509.Certificate.fingerprint `SHA256 cert in
  let fp_b64 = Base64.encode_string (Cstruct.to_string fp) in
  (`Single ([ cert ], key), fp_b64)

let main () =
  let requested_port =
    if Array.length (Sys.get_argv ()) > 1 then
      Int.of_string (Sys.get_argv ()).(1)
    else 0
  in
  Mirage_crypto_rng_unix.initialize (module Mirage_crypto_rng.Fortuna);
  let certificates, fp_b64 = make_self_signed () in
  let tls_cfg = Tls.Config.server ~certificates ~alpn_protocols:[ "h2" ] () in
  let where =
    Tcp.Where_to_listen.bind_to Tcp.Bind_to_address.Localhost
      (if requested_port = 0 then Tcp.Bind_to_port.On_port_chosen_by_os
       else Tcp.Bind_to_port.On_port requested_port)
  in
  let%bind server =
    Tls_async.listen ~on_handler_error:`Ignore tls_cfg where
      (fun _addr _session reader writer -> serve_connection reader writer)
  in
  let bound_port = Tcp.Server.listening_on server in
  printf "READY %d %s\n%!" bound_port fp_b64;
  Deferred.never ()

let () =
  don't_wait_for (main ());
  never_returns (Scheduler.go ())
