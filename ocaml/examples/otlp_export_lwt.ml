(** Example 5 — the OTLP interface: export OpenTelemetry logs/metrics (Lwt).

    {!Zerobus_otlp} is for callers already emitting OpenTelemetry data. It ships
    OTLP logs and metrics to the Zerobus OTLP endpoint over the same TLS+h2
    transport as the gRPC SDK, via the unary [LogsService/Export] /
    [MetricsService/Export] RPCs. Build the records with the generated
    OpenTelemetry types in [Zerobus_otlp_proto]. *)

let ( let* ) = Lwt.bind
let env k = try Sys.getenv k with Not_found -> failwith ("set env " ^ k)

module OLogs = Zerobus_otlp_proto.Logs

let main () : (unit, Zerobus_core.Error.t) result Lwt.t =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let table = env "ZEROBUS_TABLE" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in

  let* client_r =
    Zerobus_otlp.create ~endpoint:"" ~workspace_url ~table ~client_id
      ~client_secret ()
  in
  match client_r with
  | Error e -> Lwt.return (Error e)
  | Ok client -> (
      (* A minimal batch of two log records under one resource/scope. *)
      let log_records =
        List.map
          (fun (ts, sev, _body) ->
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
      let* r = Zerobus_otlp.export_logs client resource_logs in
      match r with
      | Ok res ->
          Printf.printf "exported logs (rejected=%Ld)\n%!" res.rejected;
          Lwt.return (Ok ())
      | Error e -> Lwt.return (Error e))

let () =
  match Lwt_main.run (main ()) with
  | Ok () -> print_endline "OK — OTLP logs exported"
  | Error e ->
      prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
      exit 1
