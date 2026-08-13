(** Example — the OTLP interface: export OpenTelemetry logs (Async).

    The Async counterpart of {!otlp_export_lwt}. {!Zerobus_otlp_async} is for
    callers already emitting OpenTelemetry data. It ships OTLP logs and metrics
    to the Zerobus OTLP endpoint over the same TLS+h2 transport as the gRPC SDK,
    via the unary [LogsService/Export] / [MetricsService/Export] RPCs. Build the
    records with the generated OpenTelemetry types in [Zerobus_otlp_proto]. *)

open! Core
open! Async
module OLogs = Zerobus_otlp_proto.Logs

let env k =
  match Sys.getenv k with Some v -> v | None -> failwith ("set env " ^ k)

let main () : (unit, Zerobus_core.Error.t) result Deferred.t =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let table = env "ZEROBUS_TABLE" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in

  match
    Zerobus_otlp_async.create ~endpoint:"" ~workspace_url ~table ~client_id
      ~client_secret ()
  with
  | Error e -> return (Error e)
  | Ok client -> (
      (* A minimal batch of two log records under one resource/scope. *)
      let log_records =
        List.map
          ~f:(fun (ts, sev, _body) ->
            OLogs.make_log_record ~time_unix_nano:ts ~severity_text:sev ())
          [ (1L, "INFO", "started"); (2L, "WARN", "slow") ]
      in
      let resource_logs =
        [
          OLogs.make_resource_logs
            ~scope_logs:[ OLogs.make_scope_logs ~log_records () ]
            ();
        ]
      in
      Zerobus_otlp_async.export_logs client resource_logs >>| function
      | Ok (res : Zerobus_otlp_async.export_result) ->
          Core.printf "exported logs (rejected=%Ld)\n%!" res.rejected;
          Ok ()
      | Error e -> Error e)

let () =
  don't_wait_for
    ( main () >>| fun r ->
      (match r with
      | Ok () -> print_endline "OK — OTLP logs exported (Async)"
      | Error e ->
          prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
          Core.exit 1);
      Shutdown.shutdown 0 );
  never_returns (Scheduler.go ())
