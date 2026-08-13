(** Self-signed TLS h2 mock Zerobus [EphemeralStream] server for the Eio
    live-TLS test — a standalone process. Identical proto behaviour to
    [ephemeral_server_eio.ml], but each accepted socket is wrapped in a
    server-side TLS 1.3 handshake (ALPN h2) using a freshly generated
    self-signed certificate. On startup it prints
    [READY <port> <cert-sha256-b64>] so the client can pin that exact cert (the
    client can't use the system trust store for a self-signed cert). This is
    what actually exercises the Eio transport's real TLS+ALPN path (the
    cleartext mocks never do). *)

module Z = Zerobus_proto.Zerobus_service

let decode_req (s : string) : Z.ephemeral_stream_request =
  Z.decode_pb_ephemeral_stream_request (Pbrt.Decoder.of_string s)

let encode_resp (r : Z.ephemeral_stream_response) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_response r e;
  Pbrt.Encoder.to_string e

let handle (requests : string Seq.t) (respond : string -> unit) : Grpc.Status.t
    =
  let watermark = ref (-1L) in
  Seq.iter
    (fun raw ->
      match decode_req raw with
      | Z.Create_stream (_ : Z.create_ingest_stream_request) ->
          respond
            (encode_resp
               (Z.Create_stream_response
                  (Z.make_create_ingest_stream_response
                     ~stream_id:"mock-stream-tls-0001" ())))
      | Z.Ingest_record r ->
          if r.Z.offset_id > !watermark then watermark := r.Z.offset_id;
          respond
            (encode_resp
               (Z.Ingest_record_response
                  (Z.make_ingest_record_response
                     ~durability_ack_up_to_offset:!watermark ())))
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
    match Ptime.of_float_s (Unix.time () +. 86400.) with
    | Some t -> t
    | None -> failwith "ptime"
  in
  let cert =
    match X509.Signing_request.sign csr ~valid_from ~valid_until key dn with
    | Ok c -> c
    | Error _ -> failwith "sign"
  in
  let fp = X509.Certificate.fingerprint `SHA256 cert in
  let fp_b64 = Base64.encode_string fp in
  (`Single ([ cert ], key), fp_b64)

let () =
  let requested =
    if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 0
  in
  Mirage_crypto_rng_unix.use_default ();
  let certificates, fp_b64 = make_self_signed () in
  let tls_cfg =
    match Tls.Config.server ~certificates ~alpn_protocols:[ "h2" ] () with
    | Ok c -> c
    | Error (`Msg m) -> failwith ("tls server cfg: " ^ m)
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
  Printf.printf "READY %d %s\n%!" bound_port fp_b64;
  let rec loop () =
    Eio.Net.accept_fork ~sw listening
      ~on_error:(fun _ -> ())
      (fun socket addr ->
        (* Server-side TLS handshake, then hand the TLS flow to h2 via the same
           stream_socket shim the client uses. *)
        let tls_flow = Tls_eio.server_of_flow tls_cfg socket in
        let module S = struct
          type t = Tls_eio.t
          type tag = [ `Generic ]

          let single_read t buf = Eio.Flow.single_read t buf
          let read_methods = []
          let single_write t bufs = Eio.Flow.single_write t bufs
          let copy t ~src = Eio.Flow.copy src t
          let shutdown t cmd = Eio.Flow.shutdown t cmd
          let close t = Eio.Resource.close t
        end in
        let sock : [ `Generic ] Eio.Net.stream_socket_ty Eio.Resource.t =
          Eio.Resource.T (tls_flow, Eio.Net.Pi.stream_socket (module S))
        in
        H2_eio.Server.create_connection_handler ?config:None
          ~request_handler:(fun _addr reqd ->
            Eio.Fiber.fork ~sw (fun () ->
                Grpc_eio.Server.handle_request server reqd))
          ~error_handler:(fun _addr ?request:_ _err _start -> ())
          ~sw addr sock);
    loop ()
  in
  loop ()
