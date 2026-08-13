(** Cross-implementation OTLP acceptance (Lwt): our {!Zerobus_otlp} exporter
    (which encodes with our vendored [Zerobus_otlp_proto]) against a mock
    collector that decodes with the {b canonical} upstream [opentelemetry.proto]
    types (otel_collector_lwt). This proves genuine OTLP wire-compatibility — a
    symmetric-codec bug that [test_otlp] (both sides our protos) would miss is
    caught here, because the encoder and decoder are independent
    implementations.

    Flow: mint token (in-process cohttp OIDC mock) -> h2c to the collector ->
    LogsService/Export + MetricsService/Export -> the collector
    CANONICAL-decodes and echoes counts to a log file -> asserts on our decoded
    response + the collector's decode log.

    Cleartext h2c, loopback, ephemeral ports. Runs on fl414. *)

module Otlp = Zerobus_otlp
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

let ( let* ) = Lwt.bind
let table = "main.default.otlp_otel_mock"

(* --- in-process OIDC token endpoint --- *)
let token_mints = ref 0

let token_handler _conn (req : Cohttp.Request.t) (_body : Cohttp_lwt.Body.t) =
  let path = Uri.path (Cohttp.Request.uri req) in
  if path = "/oidc/v1/token" then begin
    incr token_mints;
    Cohttp_lwt_unix.Server.respond_string ~status:`OK
      ~body:
        {|{"access_token":"tok-otel-1","token_type":"Bearer","expires_in":3600}|}
      ()
  end
  else Cohttp_lwt_unix.Server.respond_string ~status:`Not_found ~body:"" ()

let start_token_server () : string Lwt.t =
  let ctx = Cohttp_lwt_unix.Net.init () in
  let server = Cohttp_lwt_unix.Server.make ~callback:token_handler () in
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  let* () = Lwt_unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0)) in
  Lwt_unix.listen sock 16;
  let port =
    match Lwt_unix.getsockname sock with
    | Unix.ADDR_INET (_, p) -> p
    | _ -> failwith "no port"
  in
  Lwt.async (fun () ->
      Cohttp_lwt_unix.Server.create ~ctx ~mode:(`TCP (`Socket sock)) server);
  Lwt.return (Printf.sprintf "http://127.0.0.1:%d" port)

(* --- spawn the canonical-decoding collector (separate process) --- *)
let collector_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "otel_collector_lwt.exe" in
  if Sys.file_exists cand then cand else "./otel_collector_lwt.exe"

let with_collector ~log_path (f : int -> 'a Lwt.t) : 'a Lwt.t =
  let exe = collector_exe () in
  let env =
    Array.append (Unix.environment ())
      [| "ZEROBUS_OTEL_COLLECTOR_LOG=" ^ log_path |]
  in
  let stdout_r, stdout_w = Unix.pipe () in
  let pid =
    Unix.create_process_env exe [| exe; "0" |] env Unix.stdin stdout_w
      Unix.stderr
  in
  Unix.close stdout_w;
  let ic = Unix.in_channel_of_descr stdout_r in
  let port =
    try
      match String.split_on_char ' ' (input_line ic) with
      | [ "READY"; p ] -> int_of_string p
      | _ -> failwith "bad READY line"
    with End_of_file -> failwith "collector did not signal READY"
  in
  Lwt.finalize
    (fun () -> f port)
    (fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      (try close_in ic with _ -> ());
      Lwt.return_unit)

(* --- genuine-shaped OTel records (our proto builders; the collector decodes them
   with the canonical proto so the CONTENT crossing implementations is what's
   validated) --- *)
let sample_logs ?(schema_url = "") () : OLogs.resource_logs list =
  let lr1 =
    OLogs.make_log_record ~time_unix_nano:111L
      ~severity_number:OLogs.Severity_number_info ~severity_text:"INFO" ()
  in
  let lr2 =
    OLogs.make_log_record ~time_unix_nano:222L
      ~severity_number:OLogs.Severity_number_warn ~severity_text:"WARN" ()
  in
  [
    OLogs.make_resource_logs
      ~scope_logs:[ OLogs.make_scope_logs ~log_records:[ lr1; lr2 ] () ]
      ~schema_url ();
  ]

let sample_metrics ?(schema_url = "") () : OMetrics.resource_metrics list =
  let m1 = OMetrics.make_metric ~name:"req_count" ~unit_:"1" () in
  [
    OMetrics.make_resource_metrics
      ~scope_metrics:[ OMetrics.make_scope_metrics ~metrics:[ m1 ] () ]
      ~schema_url ();
  ]

type outcome = {
  logs_ok : bool;
  logs_rejected : int64;
  metrics_ok : bool;
  partial_rejected : int64;
  mints : int;
  collector_saw_2_logs : bool;
  collector_saw_info : bool;
}

let read_file p =
  try In_channel.with_open_bin p In_channel.input_all with _ -> ""

let contains hay needle =
  let nl = String.length needle and hl = String.length hay in
  let rec go i =
    if i + nl > hl then false
    else if String.sub hay i nl = needle then true
    else go (i + 1)
  in
  go 0

let run () : outcome Lwt.t =
  let* token_url = start_token_server () in
  let log_path = Filename.temp_file "otel_collector" ".log" in
  with_collector ~log_path (fun port ->
      let endpoint = Printf.sprintf "127.0.0.1:%d" port in
      let* client_r =
        Otlp.create ~tls:false ~endpoint ~workspace_url:token_url ~table
          ~client_id:"sp-app-id" ~client_secret:"sp-secret" ()
      in
      match client_r with
      | Error e -> failwith (Zerobus_core.Error.to_string e)
      | Ok client ->
          let* r_logs = Otlp.export_logs client (sample_logs ()) in
          let* r_metrics = Otlp.export_metrics client (sample_metrics ()) in
          let* r_partial =
            Otlp.export_logs client (sample_logs ~schema_url:"REJECT1" ())
          in
          (* Give the collector a beat to flush its decode log. *)
          let* () = Lwt_unix.sleep 0.2 in
          let clog = read_file log_path in
          let ok = function Ok _ -> true | Error _ -> false in
          let rej = function
            | Ok (r : Otlp.export_result) -> r.rejected
            | Error _ -> -1L
          in
          Lwt.return
            {
              logs_ok = ok r_logs;
              logs_rejected = rej r_logs;
              metrics_ok = ok r_metrics;
              partial_rejected = rej r_partial;
              mints = !token_mints;
              collector_saw_2_logs = contains clog "canonical-decoded 2 record";
              collector_saw_info = contains clog "sev0=INFO";
            })

let result =
  lazy
    (Lwt_main.run
       (let* o = run () in
        Printf.eprintf
          "ZEROBUS OCAML — OTLP CROSS-IMPL TEST (Lwt, canonical \
           opentelemetry.proto)\n\
           logs_export ok=%b rejected=%Ld ; metrics ok=%b ; partial rejected=%Ld\n\
           token_mints=%d ; collector canonical-decoded 2 logs=%b ; saw INFO=%b\n\
           %!"
          o.logs_ok o.logs_rejected o.metrics_ok o.partial_rejected o.mints
          o.collector_saw_2_logs o.collector_saw_info;
        Lwt.return o))

let () =
  Alcotest.run "otlp-otel-lwt"
    [
      ( "cross-impl-export",
        [
          Alcotest.test_case "logs Export accepted" `Slow (fun () ->
              Alcotest.(check bool) "ok" true (Lazy.force result).logs_ok);
          Alcotest.test_case "logs fully accepted (0 rejected)" `Slow (fun () ->
              Alcotest.(check int64) "rej" 0L (Lazy.force result).logs_rejected);
          Alcotest.test_case "metrics Export accepted" `Slow (fun () ->
              Alcotest.(check bool) "ok" true (Lazy.force result).metrics_ok);
          Alcotest.test_case "partial success surfaced (1 rejected)" `Slow
            (fun () ->
              Alcotest.(check int64)
                "rej" 1L (Lazy.force result).partial_rejected);
          Alcotest.test_case "token cached (mints once)" `Slow (fun () ->
              Alcotest.(check int) "mints" 1 (Lazy.force result).mints);
          Alcotest.test_case "CANONICAL decoder read 2 log records" `Slow
            (fun () ->
              Alcotest.(check bool)
                "2 logs" true (Lazy.force result).collector_saw_2_logs);
          Alcotest.test_case "CANONICAL decoder read severity INFO" `Slow
            (fun () ->
              Alcotest.(check bool)
                "sev" true (Lazy.force result).collector_saw_info);
        ] );
    ]
