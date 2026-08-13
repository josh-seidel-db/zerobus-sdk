(** Live SDK test — Eio runtime, via the REAL public {!Zerobus_eio} API.

    Drives the shipped Eio SDK end to end: [Zerobus_eio.create] ->
    [with_stream_oauth] (built-in client-credentials OAuth + TLS 1.3 + ALPN h2,
    the Eio live-verified path) -> loop [ingest] -> [flush], bracket-closed.

    Mode via argv.(1): "json" | "proto" | "arrow" — proving the Eio façade selects
    the record type / driver exactly like Lwt. Env (never logged):
    DATABRICKS_WORKSPACE_URL, ZEROBUS_ENDPOINT, ZEROBUS_CLIENT_ID,
    ZEROBUS_CLIENT_SECRET, and ZEROBUS_TABLE_{JSON_EIO,PROTO_EIO,ARROW_EIO}. *)

module Opt = Zerobus_core.Options
module D = Zerobus_proto.Descriptor

let getenv k = try Sys.getenv k with Not_found -> failwith ("missing env: " ^ k)
let n_rows = 5

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

let proto_row (i : int) : bytes =
  let e = Pbrt.Encoder.create () in
  Pbrt.Encoder.string (Printf.sprintf "row-%d" i) e;
  Pbrt.Encoder.key 2 Pbrt.Bytes e;
  Pbrt.Encoder.int64_as_varint (Int64.of_int i) e;
  Pbrt.Encoder.key 1 Pbrt.Varint e;
  Pbrt.Encoder.to_bytes e

let run ~env ~sw mode : (unit, Zerobus_core.Error.t) result =
  let workspace_url = getenv "DATABRICKS_WORKSPACE_URL" in
  let endpoint = getenv "ZEROBUS_ENDPOINT" in
  let client_id = getenv "ZEROBUS_CLIENT_ID" in
  let client_secret = getenv "ZEROBUS_CLIENT_SECRET" in
  let table_name, record_type, descriptor, mk_row =
    match mode with
    | "json" ->
        ( getenv "ZEROBUS_TABLE_JSON_EIO", Opt.Json, None,
          fun i -> Bytes.of_string (Printf.sprintf {|{"id": %d, "name": "row-%d"}|} i i) )
    | "proto" ->
        ( getenv "ZEROBUS_TABLE_PROTO_EIO", Opt.Proto,
          Some (Opt.descriptor_of_bytes (descriptor_bytes ())), proto_row )
    | "arrow" ->
        let schema =
          match Zerobus_arrow.schema_message () with
          | Ok s -> s
          | Error m -> failwith ("arrow schema_message: " ^ m)
        in
        ( getenv "ZEROBUS_TABLE_ARROW_EIO", Opt.Arrow,
          Some (Opt.descriptor_of_bytes schema),
          fun i ->
            match Zerobus_arrow.encode [ { Zerobus_arrow.id = i; name = Printf.sprintf "row-%d" i } ] with
            | Ok packed -> packed
            | Error m -> failwith ("arrow encode: " ^ m) )
    | m -> failwith ("unknown mode: " ^ m)
  in
  Printf.printf "== live SDK test (Eio, %s) ==\nendpoint: %s\ntable: %s\n%!" mode
    endpoint table_name;
  match Zerobus_eio.create ~endpoint ~workspace_url () with
  | Error e -> Error e
  | Ok client ->
      let table = { Opt.table_name; descriptor } in
      let options = { Opt.default_stream_options with record_type } in
      let outer =
        Zerobus_eio.with_stream_oauth ~env ~sw client table ~client_id
          ~client_secret ~options (fun stream ->
            List.iter
              (fun i ->
                match Zerobus_eio.ingest stream (mk_row i) with
                | Ok _ -> ()
                | Error e -> failwith (Zerobus_core.Error.to_string e))
              (List.init n_rows Fun.id);
            Zerobus_eio.flush stream)
      in
      (match outer with Ok inner -> inner | Error e -> Error e)

let () =
  let mode = if Array.length Sys.argv > 1 then Sys.argv.(1) else "json" in
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  match run ~env:(env :> Eio_unix.Stdenv.base) ~sw mode with
  | Ok () ->
      Printf.printf "RESULT: PASS — SDK ingested %d %s rows and flushed durably (Eio)\n%!"
        n_rows mode
  | Error e ->
      Printf.printf "RESULT: FAIL — %s\n%!" (Zerobus_core.Error.to_string e);
      exit 1
