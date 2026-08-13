(** Env-gated live integration suite (DESIGN.md §12.4) — Lwt runtime.

    Exercises the {b real} stack the loopback mocks cannot: the TLS 1.3 + ALPN-h2
    handshake (§12.1), scoped-token acquisition (§12.2), and each of the SDK's
    interfaces against a live Databricks workspace, across {b both} of the SDK's
    authentication mechanisms:
      - gRPC streaming JSON / Proto / Arrow ingest — built-in scoped OAuth
        ({!Zerobus.create_stream} -> ingest -> flush)
      - gRPC streaming JSON / Proto ingest — caller-supplied bearer headers
        ({!Zerobus.create_stream_with_headers}, bearer minted via
        {!Zerobus_core.Config.oauth_token_request_body})
      - REST insert                 ({!Zerobus_rest.insert}, built-in OAuth)
      - OTLP logs export            ({!Zerobus_otlp.export_logs}, built-in OAuth)

    {2 Gating}

    The suite runs only when ALL of these env vars are set; otherwise EVERY case is
    a clean no-op success (so CI and offline dev stay green — nothing to configure):
      - DATABRICKS_HOST          the workspace URL (e.g. https://ws.azuredatabricks.net)
      - DATABRICKS_CLIENT_ID     service-principal application id
      - DATABRICKS_CLIENT_SECRET service-principal secret
      - ZEROBUS_TEST_TABLE       a PRE-CREATED UC table (catalog.schema.table) with
                                 columns {{ id BIGINT, name STRING }} — Zerobus never
                                 auto-creates tables

    Optional:
      - ZEROBUS_ENDPOINT         explicit gRPC host:port (e.g.
                                 <wsid>.zerobus.<region>.<cloud>:443). Recommended on
                                 Azure/GCP where the endpoint is not derivable from
                                 the console URL (see Config.endpoint_of_workspace).
                                 Falls back to derivation from DATABRICKS_HOST.
      - ZEROBUS_TEST_TABLE_PROTO / _REST / _OTLP  per-interface table overrides;
                                 each defaults to ZEROBUS_TEST_TABLE.

    Row persistence is asserted indirectly (a durable [flush]/2xx from the live
    service). A stronger check (SQL SELECT) is done out of band by the operator, as
    with the ocaml/live/ harnesses. NOT part of the normal CI matrix. *)

module Opt = Zerobus_core.Options
module D = Zerobus_proto.Descriptor

let ( let* ) = Lwt.bind
let n_rows = 5

(* --- env gating --- *)
let getenv_opt k = match Sys.getenv_opt k with Some "" -> None | v -> v

type cfg = {
  workspace_url : string;
  endpoint : string; (* "" => derive from workspace_url *)
  client_id : string;
  client_secret : string;
  table_json : string;
  table_proto : string;
  table_arrow : string;
  table_rest : string;
  table_otlp : string;
}

(* Present only when the four required vars are all set. *)
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
          table_arrow =
            Option.value (getenv_opt "ZEROBUS_TEST_TABLE_ARROW") ~default:table;
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

let json_row (i : int) : bytes =
  Bytes.of_string (Printf.sprintf {|{"id": %d, "name": "row-%d"}|} i i)

(* --- Arrow helpers (real zerobus-arrow IPC codec; {id:int64; name:string}) --- *)
let arrow_schema () : bytes =
  match Zerobus_arrow.schema_message () with
  | Ok s -> s
  | Error e -> failwith ("arrow schema_message: " ^ e)

let arrow_row (i : int) : bytes =
  match
    Zerobus_arrow.encode
      [ { Zerobus_arrow.id = i; name = Printf.sprintf "row-%d" i } ]
  with
  | Ok ipc -> ipc
  | Error e -> failwith ("arrow encode: " ^ e)

(* --- standalone token mint (for the bring-your-own-headers auth pathway) ---
   The built-in-OAuth path (create_stream) mints internally; to exercise the
   *custom-headers* path (create_stream_with_headers) we mint a table-scoped
   bearer here — the same client-credentials grant, via the shared
   Config.oauth_token_request_body — and supply it as the [authorization] header.
   This proves the second, caller-driven authentication mechanism end to end. *)
let mint_bearer (c : cfg) ~table : (string, Zerobus_core.Error.t) result Lwt.t =
  match Zerobus_core.Config.workspace_id_of_url c.workspace_url with
  | Error e -> Lwt.return (Error e)
  | Ok workspace_id -> (
      match
        Zerobus_core.Config.oauth_token_request_body ~workspace_id ~table
      with
      | Error e -> Lwt.return (Error e)
      | Ok body ->
          let token_url = c.workspace_url ^ "/oidc/v1/token" in
          let basic =
            Base64.encode_string (c.client_id ^ ":" ^ c.client_secret)
          in
          let headers =
            Cohttp.Header.of_list
              [
                ("Content-Type", "application/x-www-form-urlencoded");
                ("Authorization", "Basic " ^ basic);
              ]
          in
          Lwt.catch
            (fun () ->
              let* resp, resp_body =
                Cohttp_lwt_unix.Client.post ~headers
                  ~body:(Cohttp_lwt.Body.of_string body)
                  (Uri.of_string token_url)
              in
              let status =
                Cohttp.Response.status resp |> Cohttp.Code.code_of_status
              in
              let* resp_str = Cohttp_lwt.Body.to_string resp_body in
              if status < 200 || status >= 300 then
                Lwt.return
                  (Error
                     (Zerobus_core.Error.Auth_error
                        (Printf.sprintf "token endpoint HTTP %d" status)))
              else
                match Yojson.Safe.from_string resp_str with
                | `Assoc l -> (
                    match List.assoc_opt "access_token" l with
                    | Some (`String tok) -> Lwt.return (Ok tok)
                    | _ ->
                        Lwt.return
                          (Error
                             (Zerobus_core.Error.Auth_error
                                "no access_token in token response")))
                | _ ->
                    Lwt.return
                      (Error
                         (Zerobus_core.Error.Auth_error
                            "malformed token response")))
            (fun exn ->
              Lwt.return
                (Error
                   (Zerobus_core.Error.Transport_error (Printexc.to_string exn))))
      )

(* Shared loop-then-flush driver: queue all rows, wait once, close. *)
let drive_stream stream ~mk_row : (unit, Zerobus_core.Error.t) result Lwt.t =
  let* last =
    Lwt_list.fold_left_s
      (fun acc i ->
        match acc with
        | Error _ -> Lwt.return acc
        | Ok _ ->
            let* r = Zerobus.ingest stream (mk_row i) in
            Lwt.return (Result.map (fun o -> Some o) r))
      (Ok None) (List.init n_rows Fun.id)
  in
  let* flush_r =
    match last with
    | Error e -> Lwt.return (Error e)
    | Ok _ -> Zerobus.flush stream
  in
  let* _ = Zerobus.close stream in
  Lwt.return flush_r

(* --- gRPC streaming ingest, BUILT-IN OAuth (create_stream, JSON or Proto) --- *)
let grpc_ingest (c : cfg) ~table_name ~record_type ~descriptor ~mk_row :
    (unit, Zerobus_core.Error.t) result Lwt.t =
  let* client_r =
    Zerobus.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url ()
  in
  match client_r with
  | Error e -> Lwt.return (Error e)
  | Ok client -> (
      let table = { Opt.table_name; descriptor } in
      let options = { Opt.default_stream_options with record_type } in
      let* stream_r =
        Zerobus.create_stream client table ~client_id:c.client_id
          ~client_secret:c.client_secret ~options ()
      in
      match stream_r with
      | Error e -> Lwt.return (Error e)
      | Ok stream -> drive_stream stream ~mk_row)

(* --- gRPC streaming ingest, CUSTOM HEADERS auth (create_stream_with_headers) ---
   Mints a bearer ourselves (mint_bearer) and supplies it via headers_provider,
   proving the second authentication mechanism drives the full stream. *)
let grpc_ingest_headers (c : cfg) ~table_name ~record_type ~descriptor ~mk_row :
    (unit, Zerobus_core.Error.t) result Lwt.t =
  let* client_r =
    Zerobus.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url ()
  in
  match client_r with
  | Error e -> Lwt.return (Error e)
  | Ok client -> (
      let table = { Opt.table_name; descriptor } in
      let options = { Opt.default_stream_options with record_type } in
      let headers_provider () =
        let* tok = mint_bearer c ~table:table_name in
        Lwt.return
          (Result.map
             (fun tok ->
               [
                 ("authorization", "Bearer " ^ tok);
                 ("x-databricks-zerobus-table-name", table_name);
               ])
             tok)
      in
      let* stream_r =
        Zerobus.create_stream_with_headers client table ~headers_provider
          ~options ()
      in
      match stream_r with
      | Error e -> Lwt.return (Error e)
      | Ok stream -> drive_stream stream ~mk_row)

(* --- REST insert --- *)
let rest_insert (c : cfg) : (unit, Zerobus_core.Error.t) result Lwt.t =
  let* client_r =
    Zerobus_rest.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url
      ~client_id:c.client_id ~client_secret:c.client_secret ()
  in
  match client_r with
  | Error e -> Lwt.return (Error e)
  | Ok client ->
      let records =
        List.init n_rows (fun i ->
            `Assoc
              [ ("id", `Int i); ("name", `String (Printf.sprintf "row-%d" i)) ])
      in
      Zerobus_rest.insert client ~table:c.table_rest records

(* --- OTLP logs export --- *)
let otlp_export (c : cfg) : (unit, Zerobus_core.Error.t) result Lwt.t =
  let* client_r =
    Zerobus_otlp.create ~endpoint:c.endpoint ~workspace_url:c.workspace_url
      ~table:c.table_otlp ~client_id:c.client_id ~client_secret:c.client_secret
      ()
  in
  match client_r with
  | Error e -> Lwt.return (Error e)
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
      let* r = Zerobus_otlp.export_logs client recs in
      Lwt.return (Result.map (fun (_ : Zerobus_otlp.export_result) -> ()) r)

(* Run a live action, returning true on Ok. When unconfigured, returns true
   without touching the network (clean skip). *)
let run_live label (f : cfg -> (unit, Zerobus_core.Error.t) result Lwt.t) : bool
    =
  match cfg with
  | None ->
      Printf.eprintf "[%s] %s\n%!" label skip_note;
      true
  | Some c -> (
      match Lwt_main.run (f c) with
      | Ok () ->
          Printf.eprintf "[%s] PASS (live)\n%!" label;
          true
      | Error e ->
          Printf.eprintf "[%s] FAIL (live): %s\n%!" label
            (Zerobus_core.Error.to_string e);
          false)

let () =
  Printf.eprintf
    "ZEROBUS OCAML — LIVE INTEGRATION SUITE (§12.4)\nconfigured: %b\n%!"
    (Option.is_some cfg);
  Alcotest.run "integration-lwt"
    [
      ( "live",
        [
          Alcotest.test_case "gRPC JSON ingest + flush" `Slow (fun () ->
              Alcotest.(check bool)
                "ok" true
                (run_live "grpc-json" (fun c ->
                     grpc_ingest c ~table_name:c.table_json
                       ~record_type:Opt.Json ~descriptor:None ~mk_row:json_row)));
          Alcotest.test_case "gRPC Proto ingest + flush" `Slow (fun () ->
              Alcotest.(check bool)
                "ok" true
                (run_live "grpc-proto" (fun c ->
                     grpc_ingest c ~table_name:c.table_proto
                       ~record_type:Opt.Proto
                       ~descriptor:
                         (Some (Opt.descriptor_of_bytes (descriptor_bytes ())))
                       ~mk_row:proto_row)));
          Alcotest.test_case "gRPC Arrow ingest + flush" `Slow (fun () ->
              Alcotest.(check bool)
                "ok" true
                (run_live "grpc-arrow" (fun c ->
                     grpc_ingest c ~table_name:c.table_arrow
                       ~record_type:Opt.Arrow
                       ~descriptor:
                         (Some (Opt.descriptor_of_bytes (arrow_schema ())))
                       ~mk_row:arrow_row)));
          (* Second auth mechanism: caller-supplied bearer via
             create_stream_with_headers (JSON + Proto). *)
          Alcotest.test_case "gRPC JSON ingest + flush (headers auth)" `Slow
            (fun () ->
              Alcotest.(check bool)
                "ok" true
                (run_live "grpc-json-headers" (fun c ->
                     grpc_ingest_headers c ~table_name:c.table_json
                       ~record_type:Opt.Json ~descriptor:None ~mk_row:json_row)));
          Alcotest.test_case "gRPC Proto ingest + flush (headers auth)" `Slow
            (fun () ->
              Alcotest.(check bool)
                "ok" true
                (run_live "grpc-proto-headers" (fun c ->
                     grpc_ingest_headers c ~table_name:c.table_proto
                       ~record_type:Opt.Proto
                       ~descriptor:
                         (Some (Opt.descriptor_of_bytes (descriptor_bytes ())))
                       ~mk_row:proto_row)));
          Alcotest.test_case "REST insert" `Slow (fun () ->
              Alcotest.(check bool) "ok" true (run_live "rest" rest_insert));
          Alcotest.test_case "OTLP logs export" `Slow (fun () ->
              Alcotest.(check bool) "ok" true (run_live "otlp" otlp_export));
        ] );
    ]
