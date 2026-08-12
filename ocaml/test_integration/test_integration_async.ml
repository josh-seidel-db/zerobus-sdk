(** Env-gated live integration suite (DESIGN.md §12.4) — Async runtime.

    The Async counterpart of test_integration_lwt.ml. Exercises the real stack
    against a live Databricks workspace across the Async interfaces:
      - gRPC streaming JSON ingest  ({!Zerobus_async.with_stream_oauth})
      - gRPC streaming Proto ingest (DescriptorProto for {{id:int64; name:string}})
      - REST insert                 ({!Zerobus_rest_async.insert})
      - OTLP logs export            ({!Zerobus_otlp_async.export_logs})

    Same env gating as the Lwt suite. When unset EVERY case is a clean no-op
    success. The gRPC + OTLP cases need live TLS (tls-async) and OAuth
    (cohttp-async); this whole suite is an [optional] executable that builds only
    on a switch with both installed (see the dune + doc/arch/tls_async_status.md).
    NOT in CI. *)

open! Core
open! Async
module Opt = Zerobus_core.Options
module D = Zerobus_proto.Descriptor

let n_rows = 5

(* --- env gating --- *)
let getenv_opt k =
  match Sys.getenv k with Some "" -> None | v -> v

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

(* --- gRPC streaming ingest (JSON or Proto) via the Async OAuth façade --- *)
let grpc_ingest (c : cfg) ~table_name ~record_type ~descriptor ~mk_row :
    (unit, Zerobus_core.Error.t) result Deferred.t =
  match%bind Zerobus_async.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url () with
  | Error e -> return (Error e)
  | Ok client ->
      let table = { Opt.table_name; descriptor } in
      let options = { Opt.default_stream_options with record_type } in
      let%map r =
        Zerobus_async.with_stream_oauth client table ~client_id:c.client_id
          ~client_secret:c.client_secret ~options (fun stream ->
            (* loop-then-flush: queue all rows, then wait once at the end *)
            let rec loop i last =
              if i >= n_rows then return last
              else
                match%bind Zerobus_async.ingest stream (mk_row i) with
                | Error _ as e -> return e
                | Ok o -> loop (i + 1) (Ok o)
            in
            match%bind loop 0 (Ok (Opt.offset_of_int64 0L)) with
            | Error e -> return (Error e)
            | Ok _ -> Zerobus_async.flush stream)
      in
      Result.join r

(* --- REST insert --- *)
let rest_insert (c : cfg) : (unit, Zerobus_core.Error.t) result Deferred.t =
  match
    Zerobus_rest_async.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url
      ~client_id:c.client_id ~client_secret:c.client_secret ()
  with
  | Error e -> return (Error e)
  | Ok client ->
      let records =
        List.init n_rows ~f:(fun i ->
            `Assoc [ ("id", `Int i); ("name", `String (Printf.sprintf "row-%d" i)) ])
      in
      Zerobus_rest_async.insert client ~table:c.table_rest records

(* --- OTLP logs export --- *)
let otlp_export (c : cfg) : (unit, Zerobus_core.Error.t) result Deferred.t =
  match
    Zerobus_otlp_async.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url
      ~table:c.table_otlp ~client_id:c.client_id ~client_secret:c.client_secret ()
  with
  | Error e -> return (Error e)
  | Ok client ->
      let module OLogs = Zerobus_otlp_proto.Logs in
      let recs =
        [
          OLogs.make_resource_logs
            ~scope_logs:
              [
                OLogs.make_scope_logs
                  ~log_records:
                    (List.init n_rows ~f:(fun i ->
                         OLogs.make_log_record
                           ~time_unix_nano:(Int64.of_int (i + 1))
                           ~severity_text:"INFO" ()))
                  ();
              ]
            ();
        ]
      in
      let%map r = Zerobus_otlp_async.export_logs client recs in
      Result.map r ~f:(fun (_ : Zerobus_otlp_async.export_result) -> ())

(* Run a live action, returning true on Ok. When unconfigured, returns true
   without touching the network (clean skip). *)
let run_live label (f : cfg -> (unit, Zerobus_core.Error.t) result Deferred.t) : bool =
  match cfg with
  | None ->
      Core.eprintf "[%s] %s\n%!" label skip_note;
      true
  | Some c -> (
      match Thread_safe.block_on_async_exn (fun () -> f c) with
      | Ok () ->
          Core.eprintf "[%s] PASS (live)\n%!" label;
          true
      | Error e ->
          Core.eprintf "[%s] FAIL (live): %s\n%!" label
            (Zerobus_core.Error.to_string e);
          false)

let () =
  Core.eprintf
    "ZEROBUS OCAML — LIVE INTEGRATION SUITE (§12.4, Async)\nconfigured: %b\n%!"
    (Option.is_some cfg);
  Alcotest.run "integration-async"
    [
      ( "live",
        [
          Alcotest.test_case "gRPC JSON ingest + flush" `Slow (fun () ->
              Alcotest.(check bool) "ok" true
                (run_live "grpc-json" (fun c ->
                     grpc_ingest c ~table_name:c.table_json ~record_type:Opt.Json
                       ~descriptor:None ~mk_row:json_row)));
          Alcotest.test_case "gRPC Proto ingest + flush" `Slow (fun () ->
              Alcotest.(check bool) "ok" true
                (run_live "grpc-proto" (fun c ->
                     grpc_ingest c ~table_name:c.table_proto ~record_type:Opt.Proto
                       ~descriptor:(Some (Opt.descriptor_of_bytes (descriptor_bytes ())))
                       ~mk_row:proto_row)));
          Alcotest.test_case "REST insert" `Slow (fun () ->
              Alcotest.(check bool) "ok" true (run_live "rest" rest_insert));
          Alcotest.test_case "OTLP logs export" `Slow (fun () ->
              Alcotest.(check bool) "ok" true (run_live "otlp" otlp_export));
        ] );
    ]
