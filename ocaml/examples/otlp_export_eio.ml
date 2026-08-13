(** Example — the OTLP interface: export OpenTelemetry logs (Eio).

    The Eio counterpart of {!otlp_export_lwt}. {!Zerobus_otlp_eio} is for
    callers already emitting OpenTelemetry data. It ships OTLP logs and metrics
    to the Zerobus OTLP endpoint over the same TLS+h2 transport as the gRPC SDK,
    via the unary [LogsService/Export] / [MetricsService/Export] RPCs. Build the
    records with the generated OpenTelemetry types in [Zerobus_otlp_proto].

    [env]/[sw] come from the caller's [Eio_main.run] / [Switch.run]. *)

module OLogs = Zerobus_otlp_proto.Logs

let getenv k = try Sys.getenv k with Not_found -> failwith ("set env " ^ k)

let run ~env ~sw : (unit, Zerobus_core.Error.t) result =
  let workspace_url = getenv "DATABRICKS_WORKSPACE_URL" in
  let table = getenv "ZEROBUS_TABLE" in
  let client_id = getenv "ZEROBUS_CLIENT_ID" in
  let client_secret = getenv "ZEROBUS_CLIENT_SECRET" in

  match
    Zerobus_otlp_eio.create ~endpoint:"" ~workspace_url ~table ~client_id
      ~client_secret ()
  with
  | Error e -> Error e
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
      match Zerobus_otlp_eio.export_logs ~env ~sw client resource_logs with
      | Ok (res : Zerobus_otlp_eio.export_result) ->
          Printf.printf "exported logs (rejected=%Ld)\n%!" res.rejected;
          Ok ()
      | Error e -> Error e)

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  match run ~env:(env :> Eio_unix.Stdenv.base) ~sw with
  | Ok () -> print_endline "OK — OTLP logs exported (Eio)"
  | Error e ->
      prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
      exit 1
