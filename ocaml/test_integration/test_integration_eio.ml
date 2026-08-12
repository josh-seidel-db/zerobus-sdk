(** Env-gated live integration suite (DESIGN.md §12.4) — Eio runtime.

    The Eio counterpart of test_integration_lwt.ml. Exercises the real stack
    against a live Databricks workspace across the Eio interfaces:
      - gRPC streaming JSON ingest  ({!Zerobus_eio.with_stream_oauth})
      - gRPC streaming Proto ingest (DescriptorProto for {{id:int64; name:string}})
      - REST insert                 ({!Zerobus_rest_eio.insert})
      - OTLP logs export            ({!Zerobus_otlp_eio.export_logs})

    Same env gating as the Lwt suite (DATABRICKS_HOST / DATABRICKS_CLIENT_ID /
    DATABRICKS_CLIENT_SECRET / ZEROBUS_TEST_TABLE, optional ZEROBUS_ENDPOINT and
    per-interface *_PROTO/_REST/_OTLP overrides). When unset EVERY case is a clean
    no-op success, so CI and offline dev stay green. Runs on zbeio (OCaml 5.x). *)

module Opt = Zerobus_core.Options
module D = Zerobus_proto.Descriptor

let n_rows = 5

(* --- env gating --- *)
let getenv_opt k = match Sys.getenv_opt k with Some "" -> None | v -> v

type cfg = {
  workspace_url : string;
  endpoint : string;
  client_id : string;
  client_secret : string;
  table_json : string;
  table_proto : string;
  table_rest : string;
  table_otlp : string;
}

let cfg : cfg option =
  match
    ( getenv_opt "DATABRICKS_HOST",
      getenv_opt "DATABRICKS_CLIENT_ID",
      getenv_opt "DATABRICKS_CLIENT_SECRET",
      getenv_opt "ZEROBUS_TEST_TABLE" )
  with
  | Some workspace_url, Some client_id, Some client_secret, Some table ->
      Some
        {
          workspace_url;
          endpoint = Option.value (getenv_opt "ZEROBUS_ENDPOINT") ~default:"";
          client_id;
          client_secret;
          table_json = table;
          table_proto =
            Option.value (getenv_opt "ZEROBUS_TEST_TABLE_PROTO") ~default:table;
          table_rest =
            Option.value (getenv_opt "ZEROBUS_TEST_TABLE_REST") ~default:table;
          table_otlp =
            Option.value (getenv_opt "ZEROBUS_TEST_TABLE_OTLP") ~default:table;
        }
  | _ -> None

let skip_note =
  "SKIPPED (live integration): set DATABRICKS_HOST, DATABRICKS_CLIENT_ID, \
   DATABRICKS_CLIENT_SECRET, ZEROBUS_TEST_TABLE to run"

(* --- proto helpers (mirror ocaml/live/live_lwt.ml) --- *)
let descriptor_bytes () : bytes =
  let f_id =
    D.make_field_descriptor_proto ~name:"id" ~number:1l ~label:D.Label_optional
      ~type_:D.Type_int64 ()
  in
  let f_name =
    D.make_field_descriptor_proto ~name:"name" ~number:2l ~label:D.Label_optional
      ~type_:D.Type_string ()
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

let json_row (i : int) : bytes =
  Bytes.of_string (Printf.sprintf {|{"id": %d, "name": "row-%d"}|} i i)

(* --- gRPC streaming ingest (JSON or Proto) via the Eio OAuth façade --- *)
let grpc_ingest ~env ~sw (c : cfg) ~table_name ~record_type ~descriptor ~mk_row :
    (unit, Zerobus_core.Error.t) result =
  match Zerobus_eio.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url () with
  | Error e -> Error e
  | Ok client ->
      let table = { Opt.table_name; descriptor } in
      let options = { Opt.default_stream_options with record_type } in
      Zerobus_eio.with_stream_oauth ~env ~sw client table ~client_id:c.client_id
        ~client_secret:c.client_secret ~options (fun stream ->
          (* loop-then-flush: queue all rows, then wait once at the end *)
          let rec loop i last =
            if i >= n_rows then last
            else
              match Zerobus_eio.ingest stream (mk_row i) with
              | Error _ as e -> e
              | Ok o -> loop (i + 1) (Ok o)
          in
          match loop 0 (Ok (Opt.offset_of_int64 0L)) with
          | Error e -> Error e
          | Ok _ -> Zerobus_eio.flush stream)
      |> Result.join

(* --- REST insert --- *)
let rest_insert ~env ~sw (c : cfg) : (unit, Zerobus_core.Error.t) result =
  match
    Zerobus_rest_eio.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url
      ~client_id:c.client_id ~client_secret:c.client_secret ()
  with
  | Error e -> Error e
  | Ok client ->
      let records =
        List.init n_rows (fun i ->
            `Assoc [ ("id", `Int i); ("name", `String (Printf.sprintf "row-%d" i)) ])
      in
      Zerobus_rest_eio.insert ~env ~sw client ~table:c.table_rest records

(* --- OTLP logs export --- *)
let otlp_export ~env ~sw (c : cfg) : (unit, Zerobus_core.Error.t) result =
  match
    Zerobus_otlp_eio.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url
      ~table:c.table_otlp ~client_id:c.client_id ~client_secret:c.client_secret ()
  with
  | Error e -> Error e
  | Ok client ->
      let module OLogs = Zerobus_otlp_proto.Logs in
      let recs =
        [
          OLogs.make_resource_logs
            ~scope_logs:
              [
                OLogs.make_scope_logs
                  ~log_records:
                    (List.init n_rows (fun i ->
                         OLogs.make_log_record
                           ~time_unix_nano:(Int64.of_int (i + 1))
                           ~severity_text:"INFO" ()))
                  ();
              ]
            ();
        ]
      in
      Zerobus_otlp_eio.export_logs ~env ~sw client recs
      |> Result.map (fun (_ : Zerobus_otlp_eio.export_result) -> ())

(* Run a live action, returning true on Ok. When unconfigured, returns true
   without touching the network (clean skip). Each case gets its own Eio env/sw. *)
let run_live label (f : env:_ -> sw:Eio.Switch.t -> cfg -> (unit, Zerobus_core.Error.t) result) : bool =
  match cfg with
  | None ->
      Printf.eprintf "[%s] %s\n%!" label skip_note;
      true
  | Some c ->
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw -> (
        match f ~env ~sw c with
        | Ok () ->
            Printf.eprintf "[%s] PASS (live)\n%!" label;
            true
        | Error e ->
            Printf.eprintf "[%s] FAIL (live): %s\n%!" label
              (Zerobus_core.Error.to_string e);
            false)

let () =
  Printf.eprintf
    "ZEROBUS OCAML — LIVE INTEGRATION SUITE (§12.4, Eio)\nconfigured: %b\n%!"
    (Option.is_some cfg);
  Alcotest.run "integration-eio"
    [
      ( "live",
        [
          Alcotest.test_case "gRPC JSON ingest + flush" `Slow (fun () ->
              Alcotest.(check bool) "ok" true
                (run_live "grpc-json" (fun ~env ~sw c ->
                     grpc_ingest ~env ~sw c ~table_name:c.table_json
                       ~record_type:Opt.Json ~descriptor:None ~mk_row:json_row)));
          Alcotest.test_case "gRPC Proto ingest + flush" `Slow (fun () ->
              Alcotest.(check bool) "ok" true
                (run_live "grpc-proto" (fun ~env ~sw c ->
                     grpc_ingest ~env ~sw c ~table_name:c.table_proto
                       ~record_type:Opt.Proto
                       ~descriptor:(Some (Opt.descriptor_of_bytes (descriptor_bytes ())))
                       ~mk_row:proto_row)));
          Alcotest.test_case "REST insert" `Slow (fun () ->
              Alcotest.(check bool) "ok" true
                (run_live "rest" (fun ~env ~sw c -> rest_insert ~env ~sw c)));
          Alcotest.test_case "OTLP logs export" `Slow (fun () ->
              Alcotest.(check bool) "ok" true
                (run_live "otlp" (fun ~env ~sw c -> otlp_export ~env ~sw c)));
        ] );
    ]
