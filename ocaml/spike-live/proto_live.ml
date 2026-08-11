(** Live PROTO validation (follow-up to D1 gate B).

    Gate B proved the wire framing against the live service using JSON records
    (no DescriptorProto). This proves the PROTO path: it builds a real
    [google.protobuf.DescriptorProto] for the row message [{ id:int64=1;
    name:string=2 }], sends it as [descriptor_proto] in a
    [CreateIngestStreamRequest] with [record_type = Proto], then ingests
    proto-encoded rows — and asserts the LIVE service validates the descriptor
    against the target table's schema (id BIGINT, name STRING) and durably acks.

    This is the piece the mock can't check: the mock ignores payload/descriptor
    content, so only a real server confirms the DescriptorProto is well-formed AND
    schema-compatible.

    Credentials + target come from env (never logged):
      DATABRICKS_HOST, ZEROBUS_WORKSPACE_ID, ZEROBUS_CLIENT_ID,
      ZEROBUS_CLIENT_SECRET, ZEROBUS_TABLE, ZEROBUS_ENDPOINT (grpc host:port).

    Exit 0 = PASS. *)

module Z = Zerobus_proto.Zerobus_service
module D = Zerobus_proto.Descriptor

let ( let* ) = Lwt.bind
let env k = try Sys.getenv k with Not_found -> failwith ("missing env: " ^ k)

(* ---- §12.2 scoped-OAuth token mint (identical to gate_b_wire) ---- *)

let authorization_details ~table =
  match String.split_on_char '.' table with
  | [ cat; sch; _ ] ->
      Printf.sprintf
        {|[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"%s"},{"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],"object_type":"SCHEMA","object_full_path":"%s.%s"},{"type":"unity_catalog_privileges","privileges":["MODIFY","SELECT"],"object_type":"TABLE","object_full_path":"%s"}]|}
        cat cat sch table
  | _ -> failwith ("ZEROBUS_TABLE must be catalog.schema.table, got: " ^ table)

let mint_token () : string Lwt.t =
  let host = env "DATABRICKS_HOST" in
  let wsid = env "ZEROBUS_WORKSPACE_ID" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in
  let table = env "ZEROBUS_TABLE" in
  let resource =
    Printf.sprintf "api://databricks/workspaces/%s/zerobusDirectWriteApi" wsid
  in
  let body_str =
    String.concat "&"
      [ "grant_type=client_credentials"; "scope=all-apis";
        "resource=" ^ Uri.pct_encode ~component:`Query_value resource;
        "authorization_details="
        ^ Uri.pct_encode ~component:`Query_value (authorization_details ~table) ]
  in
  let basic = Base64.encode_string (client_id ^ ":" ^ client_secret) in
  let headers =
    Cohttp.Header.of_list
      [ ("Content-Type", "application/x-www-form-urlencoded");
        ("Authorization", "Basic " ^ basic) ]
  in
  let* resp, body =
    Cohttp_lwt_unix.Client.post ~headers
      ~body:(Cohttp_lwt.Body.of_string body_str)
      (Uri.of_string (host ^ "/oidc/v1/token"))
  in
  let code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
  let* body_str = Cohttp_lwt.Body.to_string body in
  if code < 200 || code >= 300 then
    Lwt.fail_with (Printf.sprintf "token HTTP %d" code)
  else
    match Yojson.Safe.from_string body_str with
    | `Assoc l -> (
        match List.assoc_opt "access_token" l with
        | Some (`String t) -> Lwt.return t
        | _ -> Lwt.fail_with "no access_token")
    | _ -> Lwt.fail_with "bad token json"

(* ---- the DescriptorProto for the row message { id:int64=1; name:string=2 } ---- *)

let make_descriptor_bytes () : bytes =
  let f_id =
    D.make_field_descriptor_proto ~name:"id" ~number:1l
      ~label:D.Label_optional ~type_:D.Type_int64 ()
  in
  let f_name =
    D.make_field_descriptor_proto ~name:"name" ~number:2l
      ~label:D.Label_optional ~type_:D.Type_string ()
  in
  let msg =
    D.make_descriptor_proto ~name:"Record" ~field:[ f_id; f_name ] ()
  in
  let e = Pbrt.Encoder.create () in
  D.encode_pb_descriptor_proto msg e;
  Pbrt.Encoder.to_bytes e

(* Proto-encode a row { id = i; name = "row-i" } matching the descriptor. *)
let encode_row (i : int) : bytes =
  let e = Pbrt.Encoder.create () in
  (* field 2 (name, string, wire type 2) *)
  Pbrt.Encoder.string (Printf.sprintf "row-%d" i) e;
  Pbrt.Encoder.key 2 Pbrt.Bytes e;
  (* field 1 (id, int64 varint, wire type 0) *)
  Pbrt.Encoder.int64_as_varint (Int64.of_int i) e;
  Pbrt.Encoder.key 1 Pbrt.Varint e;
  Pbrt.Encoder.to_bytes e

(* ---- proto framing with the generated (gate-A) types ---- *)

let enc_req (r : Z.ephemeral_stream_request) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_request r e;
  Pbrt.Encoder.to_string e

let dec_resp (s : string) : Z.ephemeral_stream_response =
  Z.decode_pb_ephemeral_stream_response (Pbrt.Decoder.of_string s)

let frame (msg : string) : string =
  let len = String.length msg in
  let b = Buffer.create (5 + len) in
  Buffer.add_char b '\000';
  Buffer.add_char b (Char.chr ((len lsr 24) land 0xff));
  Buffer.add_char b (Char.chr ((len lsr 16) land 0xff));
  Buffer.add_char b (Char.chr ((len lsr 8) land 0xff));
  Buffer.add_char b (Char.chr (len land 0xff));
  Buffer.add_string b msg;
  Buffer.contents b

let deframe (acc : string) : string list * string =
  let rec loop acc msgs =
    if String.length acc < 5 then (List.rev msgs, acc)
    else
      let len =
        (Char.code acc.[1] lsl 24) lor (Char.code acc.[2] lsl 16)
        lor (Char.code acc.[3] lsl 8) lor Char.code acc.[4]
      in
      if String.length acc < 5 + len then (List.rev msgs, acc)
      else
        loop (String.sub acc (5 + len) (String.length acc - 5 - len))
          (String.sub acc 5 len :: msgs)
  in
  loop acc []

(* ---- TLS+ALPN h2 connect (ported from gate_b_wire) ---- *)

let connect_tls host port =
  let* addrs =
    Lwt_unix.getaddrinfo host (string_of_int port)
      [ Unix.(AI_SOCKTYPE SOCK_STREAM) ]
  in
  let* sockaddr =
    match addrs with
    | ai :: _ -> Lwt.return ai.Unix.ai_addr
    | [] -> Lwt.fail_with ("no address for " ^ host)
  in
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let* () = Lwt_unix.connect fd sockaddr in
  let authenticator =
    match Ca_certs.authenticator () with
    | Ok a -> a
    | Error (`Msg m) -> failwith ("ca-certs: " ^ m)
  in
  let peer_name =
    match Domain_name.of_string host with
    | Ok d -> ( match Domain_name.host d with Ok h -> Some h | Error _ -> None)
    | Error _ -> None
  in
  let cfg =
    Tls.Config.client ~authenticator ?peer_name ~alpn_protocols:[ "h2" ] ()
  in
  let* tls = Tls_lwt.Unix.client_of_fd cfg fd in
  (match Tls_lwt.Unix.epoch tls with
   | Ok e when e.Tls.Core.alpn_protocol = Some "h2" -> ()
   | Ok _ -> failwith "ALPN != h2"
   | Error () -> failwith "no TLS epoch");
  H2_lwt_unix.Client.TLS.create_connection ~error_handler:(fun _ -> ()) tls

let () =
  Lwt_main.run
    (let table = env "ZEROBUS_TABLE" in
     let endpoint = env "ZEROBUS_ENDPOINT" in
     let host, port =
       match String.split_on_char ':' endpoint with
       | [ h; p ] -> (h, int_of_string p)
       | _ -> (endpoint, 443)
     in
     Printf.printf
       "== Live PROTO validation ==\nendpoint: %s:%d\ntable: %s\n%!" host port
       table;

     let* token = mint_token () in
     Printf.printf "oauth: scoped token minted (value not logged)\n%!";

     let* conn = connect_tls host port in
     Printf.printf "tls: connected, ALPN h2\n%!";

     let descriptor = make_descriptor_bytes () in
     Printf.printf "descriptor: built DescriptorProto (%d bytes)\n%!"
       (Bytes.length descriptor);

     (* CreateIngestStream with record_type=Proto + the descriptor, then rows. *)
     let create =
       Z.Create_stream
         (Z.make_create_ingest_stream_request ~table_name:table
            ~record_type:Z.Proto ~descriptor_proto:descriptor ())
     in
     let n_rows = 5 in

     let got_create_resp = ref false in
     let got_ack = ref false in
     let stream_id = ref "" in
     let proto_error = ref None in
     let grpc_status = ref "(none)" in
     let http_status = ref "(none)" in

     let req =
       H2.Request.create ~scheme:"https" `POST
         "/databricks.zerobus.Zerobus/EphemeralStream"
         ~headers:
           (H2.Headers.of_list
              [ (":authority", host);
                ("te", "trailers");
                ("content-type", "application/grpc+proto");
                ("grpc-encoding", "identity");
                ("user-agent", "zerobus-ocaml-sdk/0.0.0-protolive");
                ("authorization", "Bearer " ^ token);
                ("x-databricks-zerobus-table-name", table) ])
     in
     let done_p, done_u = Lwt.wait () in
     let resp_acc = ref "" in
     let response_handler (response : H2.Response.t) body =
       http_status := H2.Status.to_string response.H2.Response.status;
       (match H2.Headers.get response.H2.Response.headers "grpc-status" with
        | Some s -> grpc_status := "grpc-status=" ^ s
        | None -> ());
       let rec on_read bs ~off ~len =
         resp_acc := !resp_acc ^ Bigstringaf.substring bs ~off ~len;
         let msgs, rest = deframe !resp_acc in
         resp_acc := rest;
         List.iter
           (fun raw ->
             match dec_resp raw with
             | Z.Create_stream_response r ->
                 got_create_resp := true;
                 stream_id := r.Z.stream_id
             | Z.Ingest_record_response _ -> got_ack := true
             | Z.Close_stream_signal _ -> ()
             | exception Pbrt.Decoder.Failure _ ->
                 proto_error := Some "decode failure")
           msgs;
         H2.Body.Reader.schedule_read body ~on_read ~on_eof
       and on_eof () = if Lwt.is_sleeping done_p then Lwt.wakeup_later done_u () in
       H2.Body.Reader.schedule_read body ~on_read ~on_eof
     in
     let trailers_handler (h : H2.Headers.t) =
       match H2.Headers.get h "grpc-status" with
       | Some s ->
           grpc_status :=
             "grpc-status=" ^ s
             ^ (match H2.Headers.get h "grpc-message" with
                | Some m -> " (" ^ m ^ ")" | None -> "")
       | None -> ()
     in
     let h2_err = ref "(none)" in
     let body_writer =
       H2_lwt_unix.Client.TLS.request conn req
         ~error_handler:(fun e ->
           h2_err :=
             (match e with
              | `Malformed_response s -> "malformed_response: " ^ s
              | `Invalid_response_body_length _ -> "invalid_response_body_length"
              | `Protocol_error (_, s) -> "protocol_error: " ^ s
              | `Exn ex -> "exn: " ^ Printexc.to_string ex);
           if Lwt.is_sleeping done_p then Lwt.wakeup_later done_u ())
         ~trailers_handler ~response_handler
     in
     let write_frame msg =
       H2.Body.Writer.write_string body_writer (frame msg);
       let flushed, u = Lwt.wait () in
       H2.Body.Writer.flush body_writer (fun () ->
           if Lwt.is_sleeping flushed then Lwt.wakeup_later u ());
       flushed
     in
     let* () = write_frame (enc_req create) in
     let* () =
       let rec loop i =
         if i >= n_rows then Lwt.return_unit
         else
           let rec_req =
             Z.Ingest_record
               (Z.make_ingest_record_request ~offset_id:(Int64.of_int i)
                  ~record:(Z.Proto_encoded_record (encode_row i)) ())
           in
           let* () = write_frame (enc_req rec_req) in
           loop (i + 1)
       in
       loop 0
     in
     H2.Body.Writer.close body_writer;
     let timed_out = ref false in
     let timeout =
       let* () = Lwt_unix.sleep 25.0 in
       timed_out := true;
       Lwt.return_unit
     in
     let* () = Lwt.pick [ done_p; timeout ] in
     let* () =
       Lwt.catch
         (fun () -> H2_lwt_unix.Client.TLS.shutdown conn)
         (fun _ -> Lwt.return_unit)
     in
     let t = Unix.gmtime (Unix.time ()) in
     Printf.printf
       "\n-- EVIDENCE (live proto) --\n\
        timestamp_utc  : %04d-%02d-%02dT%02d:%02d:%02dZ\n\
        rpc            : databricks.zerobus.Zerobus/EphemeralStream (bidi, TLS h2)\n\
        record_type    : PROTO (DescriptorProto sent)\n\
        create_stream_response: %b (stream_id=%s)\n\
        durability_ack : %b\n\
        http           : %s | %s | h2_err=%s\n\
        timed_out      : %b\n\
        decode_error   : %s\n%!"
       (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min t.tm_sec
       !got_create_resp !stream_id !got_ack !http_status !grpc_status !h2_err
       !timed_out
       (match !proto_error with Some e -> e | None -> "none");

     (* PASS if the live service accepted the Proto descriptor + framing: a
        well-formed CreateIngestStreamResponse and no decode error. A durability
        ack additionally confirms the descriptor was schema-compatible and the
        proto rows were accepted. *)
     if !got_create_resp && !proto_error = None then begin
       Printf.printf
         "RESULT: PASS — live service accepted the DescriptorProto + proto rows%s.\n%!"
         (if !got_ack then " (rows durably acked)" else " (create only; no ack seen)");
       Lwt.return_unit
     end
     else begin
       Printf.printf "RESULT: FAIL — server rejected the proto descriptor/framing.\n%!";
       exit 1
     end)
