(** Phase 7d acceptance: {!Zerobus_otlp} (the OTLP logs/metrics exporter)
    against a mock OTLP collector (separate process, cleartext h2c) + an
    in-process cohttp OIDC token endpoint.

    Proves the full otlp/grpc flow end-to-end without a live workspace:
    - the exporter mints a table-scoped token (client-credentials grant) and
      opens a TLS-off h2c connection to the collector;
    - a batch of OTel logs is framed as a valid ExportLogsServiceRequest, the
      collector DECODES it (proving the wire types + framing), and returns OK;
    - likewise for metrics via MetricsService/Export;
    - a partial-success response (rejected > 0) is surfaced to the caller;
    - an empty batch is a no-op (no RPC), and the token is cached (one mint).

    Cleartext h2c, loopback, ephemeral ports. Runs on fl414. *)

module Otlp = Zerobus_otlp
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

let ( let* ) = Lwt.bind
let table = "main.default.otlp_mock"

(* --- in-process OIDC token endpoint (counts mints) --- *)
let token_mints = ref 0

let token_handler _conn (req : Cohttp.Request.t) (_body : Cohttp_lwt.Body.t) =
  let path = Uri.path (Cohttp.Request.uri req) in
  if path = "/oidc/v1/token" then begin
    incr token_mints;
    Cohttp_lwt_unix.Server.respond_string ~status:`OK
      ~body:
        {|{"access_token":"tok-otlp-1","token_type":"Bearer","expires_in":3600}|}
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

(* --- spawn the mock OTLP collector (separate process), await READY --- *)
let collector_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "otlp_collector.exe" in
  if Sys.file_exists cand then cand else "./otlp_collector.exe"

let with_collector (f : int -> 'a Lwt.t) : 'a Lwt.t =
  let exe = collector_exe () in
  let stdout_r, stdout_w = Unix.pipe () in
  let pid =
    Unix.create_process exe [| exe; "0" |] Unix.stdin stdout_w Unix.stderr
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

(* --- build a few OTel records --- *)
let sample_logs ?(schema_url = "") () : OLogs.resource_logs list =
  let lr1 = OLogs.make_log_record ~time_unix_nano:1L ~severity_text:"INFO" () in
  let lr2 = OLogs.make_log_record ~time_unix_nano:2L ~severity_text:"WARN" () in
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
  metrics_rejected : int64;
  partial_rejected : int64;
  empty_ok : bool;
  mints : int;
}

let run () : outcome Lwt.t =
  let* token_url = start_token_server () in
  with_collector (fun port ->
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
          let* r_empty = Otlp.export_logs client [] in
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
              metrics_rejected = rej r_metrics;
              partial_rejected = rej r_partial;
              empty_ok = r_empty = Ok { rejected = 0L; error_message = "" };
              mints = !token_mints;
            })

let result =
  lazy
    (Lwt_main.run
       (let* o = run () in
        Printf.eprintf
          "ZEROBUS OCAML PHASE 7d OTLP TEST (Lwt) -- evidence\n\
           ocaml_version : %s\n\
           logs_export   : ok=%b rejected=%Ld\n\
           metrics_export: ok=%b rejected=%Ld\n\
           partial_success (REJECT1): rejected=%Ld (expect 1)\n\
           empty_is_noop : %b\n\
           token_mints   : %d (expect 1 — cached across exports)\n\
           %!"
          Sys.ocaml_version o.logs_ok o.logs_rejected o.metrics_ok
          o.metrics_rejected o.partial_rejected o.empty_ok o.mints;
        Lwt.return o))

let () =
  Alcotest.run "phase7d-otlp"
    [
      ( "otlp-export",
        [
          Alcotest.test_case "logs Export accepted" `Slow (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool) "logs ok" true o.logs_ok);
          Alcotest.test_case "logs fully accepted (0 rejected)" `Slow (fun () ->
              let o = Lazy.force result in
              Alcotest.(check int64) "logs rejected" 0L o.logs_rejected);
          Alcotest.test_case "metrics Export accepted" `Slow (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool) "metrics ok" true o.metrics_ok);
          Alcotest.test_case "metrics fully accepted (0 rejected)" `Slow
            (fun () ->
              let o = Lazy.force result in
              Alcotest.(check int64) "metrics rejected" 0L o.metrics_rejected);
          Alcotest.test_case "partial success surfaced (1 rejected)" `Slow
            (fun () ->
              let o = Lazy.force result in
              Alcotest.(check int64) "partial" 1L o.partial_rejected);
          Alcotest.test_case "empty batch is a no-op" `Slow (fun () ->
              let o = Lazy.force result in
              Alcotest.(check bool) "empty" true o.empty_ok);
          Alcotest.test_case "token cached (mints once)" `Slow (fun () ->
              let o = Lazy.force result in
              Alcotest.(check int) "mints" 1 o.mints);
        ] );
    ]
