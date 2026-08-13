(** Live SDK test — Lwt runtime, via the REAL public {!Zerobus} API.

    Unlike spike-live/ (hand-rolled h2), this drives the shipped SDK end to end:
    [Zerobus.create] -> [Zerobus.create_stream] (built-in client-credentials
    OAuth + TLS 1.3 + ALPN h2) -> loop [ingest] -> [flush] -> [close].

    Mode via argv.(1): "json" | "proto" | "arrow". Each targets its own table so
    the row shapes don't collide. Credentials + targets come from env (never
    logged):
      DATABRICKS_WORKSPACE_URL, ZEROBUS_ENDPOINT (grpc host:port),
      ZEROBUS_CLIENT_ID, ZEROBUS_CLIENT_SECRET,
      ZEROBUS_TABLE_JSON, ZEROBUS_TABLE_PROTO, ZEROBUS_TABLE_ARROW

    Exit 0 = the SDK ingested + flushed durably (Ok from flush). Row persistence
    is verified out of band by the driver script (SQL query). *)

module Opt = Zerobus_core.Options
module D = Zerobus_proto.Descriptor

let ( let* ) = Lwt.bind
let env k = try Sys.getenv k with Not_found -> failwith ("missing env: " ^ k)
let n_rows = 5

(* Proto DescriptorProto for { id:int64=1; name:string=2 } (matches the table). *)
let descriptor_bytes () : bytes =
  let f_id =
    D.make_field_descriptor_proto ~name:"id" ~number:1l ~label:D.Label_optional
      ~type_:D.Type_int64 ()
  in
  let f_name =
    D.make_field_descriptor_proto ~name:"name" ~number:2l
      ~label:D.Label_optional ~type_:D.Type_string ()
  in
  let msg = D.make_descriptor_proto ~name:"Record" ~field:[ f_id; f_name ] () in
  let e = Pbrt.Encoder.create () in
  D.encode_pb_descriptor_proto msg e;
  Pbrt.Encoder.to_bytes e

(* Proto-encode a row { id = i; name = "row-i" } matching the descriptor. *)
let proto_row (i : int) : bytes =
  let e = Pbrt.Encoder.create () in
  Pbrt.Encoder.string (Printf.sprintf "row-%d" i) e;
  Pbrt.Encoder.key 2 Pbrt.Bytes e;
  Pbrt.Encoder.int64_as_varint (Int64.of_int i) e;
  Pbrt.Encoder.key 1 Pbrt.Varint e;
  Pbrt.Encoder.to_bytes e

let run mode =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let endpoint = env "ZEROBUS_ENDPOINT" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in
  let table_name, record_type, descriptor, mk_row =
    match mode with
    | "json" ->
        ( env "ZEROBUS_TABLE_JSON", Opt.Json, None,
          fun i -> Bytes.of_string (Printf.sprintf {|{"id": %d, "name": "row-%d"}|} i i) )
    | "proto" ->
        ( env "ZEROBUS_TABLE_PROTO", Opt.Proto,
          Some (Opt.descriptor_of_bytes (descriptor_bytes ())), proto_row )
    | "arrow" ->
        let schema =
          match Zerobus_arrow.schema_message () with
          | Ok s -> s
          | Error m -> failwith ("arrow schema_message: " ^ m)
        in
        ( env "ZEROBUS_TABLE_ARROW", Opt.Arrow,
          (* carry the Arrow schema message in descriptor; Flight create_frame
             sends it as the first FlightData's data_header. *)
          Some (Opt.descriptor_of_bytes schema),
          fun i ->
            match Zerobus_arrow.encode [ { Zerobus_arrow.id = i; name = Printf.sprintf "row-%d" i } ] with
            | Ok packed -> packed
            | Error m -> failwith ("arrow encode: " ^ m) )
    | m -> failwith ("unknown mode: " ^ m)
  in
  Printf.printf "== live SDK test (Lwt, %s) ==\nendpoint: %s\ntable: %s\n%!"
    mode endpoint table_name;

  (* endpoint passed explicitly: Azure host, not the AWS-style derivation. *)
  let* client_r = Zerobus.create ~endpoint ~workspace_url () in
  match client_r with
  | Error e -> Lwt.return (Error e)
  | Ok client ->
      let table = { Opt.table_name; descriptor } in
      let ack_callback =
        {
          Opt.on_ack =
            (fun o -> Printf.eprintf "[ack] durable up to offset %Ld\n%!" (Opt.int64_of_offset o));
          on_error = (fun o m -> Printf.eprintf "[ack-err] offset %Ld: %s\n%!" (Opt.int64_of_offset o) m);
        }
      in
      let options = { Opt.default_stream_options with record_type; ack_callback = Some ack_callback } in
      Printf.eprintf "[stage] client created; opening stream...\n%!";
      let* stream_r =
        Zerobus.create_stream client table ~client_id ~client_secret ~options ()
      in
      (match stream_r with
      | Error e -> Lwt.return (Error e)
      | Ok stream ->
          Printf.eprintf "[stage] stream opened; ingesting %d rows...\n%!" n_rows;
          (* loop-then-flush: queue all rows, then wait once *)
          let* last =
            Lwt_list.fold_left_s
              (fun acc i ->
                match acc with
                | Error _ -> Lwt.return acc
                | Ok _ ->
                    let* r = Zerobus.ingest stream (mk_row i) in
                    Lwt.return (Result.map (fun o -> Some o) r))
              (Ok None)
              (List.init n_rows Fun.id)
          in
          (match last with
          | Error e -> Lwt.return (Error e)
          | Ok _ ->
              Printf.eprintf "[stage] all rows queued; flushing...\n%!";
              let* flush_r = Zerobus.flush stream in
              Printf.eprintf "[stage] flush returned; closing...\n%!";
              let* _ = Zerobus.close stream in
              Lwt.return flush_r))

let () =
  let mode = if Array.length Sys.argv > 1 then Sys.argv.(1) else "json" in
  match Lwt_main.run (run mode) with
  | Ok () ->
      Printf.printf "RESULT: PASS — SDK ingested %d %s rows and flushed durably\n%!"
        n_rows mode
  | Error e ->
      Printf.printf "RESULT: FAIL — %s\n%!" (Zerobus_core.Error.to_string e);
      exit 1
