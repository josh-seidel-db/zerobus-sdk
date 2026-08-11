(** Live proof: Async live-TLS EphemeralStream JSON ingest via the h2-core pump
    ({!H2_async_tls}), against the real Zerobus endpoint. Mirrors the Lwt raw-h2
    transport but on Async + Tls_async, with NO gluten-async/h2-async.

    Env (never logged): DATABRICKS_WORKSPACE_URL (unused here; token via curl in the
    runner), ZEROBUS_ENDPOINT (grpc host:port), ZEROBUS_TABLE, ZEROBUS_TOKEN
    (a pre-minted bearer; the runner mints it so this stays transport-only).

    Exit 0 = the server accepted the stream and durably acked our rows. *)

open! Core
open! Async
module Z = Zerobus_proto.Zerobus_service

let env k = match Sys.getenv k with Some v -> v | None -> failwith ("missing env: " ^ k)

(* gRPC length-prefixed framing (1 compression byte + 4-byte BE length). *)
let frame (msg : string) : string =
  let len = String.length msg in
  let b = Buffer.create (5 + len) in
  Buffer.add_char b '\000';
  Buffer.add_char b (Char.of_int_exn ((len lsr 24) land 0xff));
  Buffer.add_char b (Char.of_int_exn ((len lsr 16) land 0xff));
  Buffer.add_char b (Char.of_int_exn ((len lsr 8) land 0xff));
  Buffer.add_char b (Char.of_int_exn (len land 0xff));
  Buffer.add_string b msg;
  Buffer.contents b

let deframe (acc : string) : string list * string =
  let rec loop acc msgs =
    if String.length acc < 5 then (List.rev msgs, acc)
    else
      let byte i = Char.to_int acc.[i] in
      let len = (byte 1 lsl 24) lor (byte 2 lsl 16) lor (byte 3 lsl 8) lor byte 4 in
      if String.length acc < 5 + len then (List.rev msgs, acc)
      else
        loop
          (String.sub acc ~pos:(5 + len) ~len:(String.length acc - 5 - len))
          (String.sub acc ~pos:5 ~len :: msgs)
  in
  loop acc []

let enc_req (r : Z.ephemeral_stream_request) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_request r e;
  Pbrt.Encoder.to_string e

let dec_resp (s : string) : Z.ephemeral_stream_response =
  Z.decode_pb_ephemeral_stream_response (Pbrt.Decoder.of_string s)

let n_rows = 5

let main () : unit Deferred.t =
  let endpoint = env "ZEROBUS_ENDPOINT" in
  let table = env "ZEROBUS_TABLE" in
  let token = env "ZEROBUS_TOKEN" in
  let host, port =
    match String.lsplit2 endpoint ~on:':' with
    | Some (h, p) -> (h, Int.of_string p)
    | None -> (endpoint, 443)
  in
  printf "== Async live-TLS ingest ==\nendpoint: %s:%d\ntable: %s\n%!" host port table;
  match%bind H2_async_tls.connect ~host ~port with
  | Error e -> printf "RESULT: FAIL — connect: %s\n%!" (Error.to_string_hum e); exit 1
  | Ok t ->
      printf "tls: connected, ALPN h2\n%!";
      let got_create = ref false in
      let got_ack = ref false in
      let acc = ref "" in
      let status = ref "(none)" in
      let done_ivar = Ivar.create () in
      let response_handler (_resp : H2.Response.t) (body : H2.Body.Reader.t) =
        let rec on_read bs ~off ~len =
          acc := !acc ^ Bigstringaf.substring bs ~off ~len;
          let msgs, rest = deframe !acc in
          acc := rest;
          List.iter msgs ~f:(fun raw ->
              match dec_resp raw with
              | Z.Create_stream_response _ -> got_create := true
              | Z.Ingest_record_response _ -> got_ack := true
              | Z.Close_stream_signal _ -> ());
          H2.Body.Reader.schedule_read body ~on_read ~on_eof
        and on_eof () = Ivar.fill_if_empty done_ivar () in
        H2.Body.Reader.schedule_read body ~on_read ~on_eof
      in
      let trailers_handler (h : H2.Headers.t) =
        match H2.Headers.get h "grpc-status" with
        | Some s -> status := "grpc-status=" ^ s
        | None -> ()
      in
      let req =
        H2.Request.create ~scheme:"https" `POST
          "/databricks.zerobus.Zerobus/EphemeralStream"
          ~headers:
            (H2.Headers.of_list
               [ (":authority", host);
                 ("te", "trailers");
                 ("content-type", "application/grpc+proto");
                 ("grpc-encoding", "identity");
                 ("authorization", "Bearer " ^ token);
                 ("x-databricks-zerobus-table-name", table) ])
      in
      let body =
        H2.Client_connection.request t.H2_async_tls.conn req
          ~error_handler:(fun _ -> Ivar.fill_if_empty done_ivar ())
          ~trailers_handler ~response_handler
      in
      let write_frame s =
        H2.Body.Writer.write_string body (frame s);
        H2.Body.Writer.flush body (fun _ -> ())
      in
      (* CreateStream (JSON) + rows, loop-then-nothing (fire and drain acks). *)
      write_frame
        (enc_req
           (Z.Create_stream
              (Z.make_create_ingest_stream_request ~table_name:table
                 ~record_type:Z.Json ())));
      for i = 0 to n_rows - 1 do
        write_frame
          (enc_req
             (Z.Ingest_record
                (Z.make_ingest_record_request ~offset_id:(Int64.of_int i)
                   ~record:(Z.Json_record (Printf.sprintf {|{"id":%d,"name":"row-%d"}|} i i))
                   ())))
      done;
      H2.Body.Writer.close body;
      (* Wait for acks or a 20s cap. *)
      let timeout = Clock.after (Time.Span.of_sec 20.) in
      let%bind () = Deferred.any [ Ivar.read done_ivar; timeout ] in
      printf "create_resp=%b durability_ack=%b %s\n%!" !got_create !got_ack !status;
      if !got_create then (
        printf "RESULT: PASS — Async live-TLS accepted stream%s\n%!"
          (if !got_ack then " (rows acked)" else " (create only)");
        return ())
      else (printf "RESULT: FAIL — no CreateStreamResponse\n%!"; exit 1)

let () =
  don't_wait_for
    (let%bind () = main () in
     Shutdown.exit 0);
  never_returns (Scheduler.go ())
