(** Eio OTLP acceptance: {!Zerobus_otlp_eio} (the OTLP logs/metrics exporter,
    Eio runtime) against a mock OTLP collector (separate process, cleartext h2c)
    \+ an in-process cohttp-eio OIDC token endpoint.

    The Eio counterpart of test_otlp/test_otlp_lwt.ml — same coverage, direct
    style. Proves the full otlp/grpc flow end-to-end without a live workspace:
    - the exporter mints a table-scoped token (client-credentials grant) and
      opens a TLS-off h2c connection to the collector via the Eio H2 client;
    - a batch of OTel logs is framed as a valid ExportLogsServiceRequest, the
      collector DECODES it (proving the wire types + framing), and returns OK;
    - likewise for metrics via MetricsService/Export;
    - a partial-success response (rejected > 0) is surfaced to the caller;
    - an empty batch is a no-op (no RPC), and the token is cached (one mint).

    Cleartext h2c, loopback, ephemeral ports. Runs on zbeio (OCaml 5.x). *)

module Otlp = Zerobus_otlp_eio
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

let table = "main.default.otlp_mock"

(* --- in-process cohttp-eio OIDC token endpoint (counts mints) --- *)
let token_mints = ref 0

let start_token_server ~sw ~env : string =
  let net = Eio.Stdenv.net env in
  let socket =
    Eio.Net.listen ~sw ~backlog:16 ~reuse_addr:true net
      (`Tcp (Eio.Net.Ipaddr.V4.loopback, 0))
  in
  let port =
    match Eio.Net.listening_addr socket with
    | `Tcp (_, p) -> p
    | `Unix _ -> failwith "no port"
  in
  let handler _conn (request : Http.Request.t) _body =
    let path = Uri.path (Uri.of_string request.Http.Request.resource) in
    if path = "/oidc/v1/token" then begin
      incr token_mints;
      Cohttp_eio.Server.respond_string ~status:`OK
        ~body:
          {|{"access_token":"tok-otlp-1","token_type":"Bearer","expires_in":3600}|}
        ()
    end
    else Cohttp_eio.Server.respond_string ~status:`Not_found ~body:"" ()
  in
  Eio.Fiber.fork_daemon ~sw (fun () ->
      Cohttp_eio.Server.run socket (Cohttp_eio.Server.make ~callback:handler ())
        ~on_error:(fun _ -> ()));
  Printf.sprintf "http://127.0.0.1:%d" port

(* --- spawn the mock OTLP collector (separate process), await READY --- *)
let collector_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "otlp_collector_eio.exe" in
  if Sys.file_exists cand then cand else "./otlp_collector_eio.exe"

let with_collector (f : int -> 'a) : 'a =
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
  Fun.protect
    ~finally:(fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      try close_in ic with _ -> ())
    (fun () -> f port)

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

let run ~env ~sw : outcome =
  let token_url = start_token_server ~sw ~env in
  with_collector (fun port ->
      let endpoint = Printf.sprintf "127.0.0.1:%d" port in
      match
        Otlp.create ~tls:false ~endpoint ~workspace_url:token_url ~table
          ~client_id:"sp-app-id" ~client_secret:"sp-secret" ()
      with
      | Error e -> failwith (Zerobus_core.Error.to_string e)
      | Ok client ->
          let r_logs = Otlp.export_logs ~env ~sw client (sample_logs ()) in
          let r_metrics =
            Otlp.export_metrics ~env ~sw client (sample_metrics ())
          in
          let r_partial =
            Otlp.export_logs ~env ~sw client
              (sample_logs ~schema_url:"REJECT1" ())
          in
          let r_empty = Otlp.export_logs ~env ~sw client [] in
          let ok = function Ok _ -> true | Error _ -> false in
          let rej = function
            | Ok (r : Otlp.export_result) -> r.rejected
            | Error _ -> -1L
          in
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
    ( Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      let o = run ~env ~sw in
      Printf.eprintf
        "ZEROBUS OCAML — EIO OTLP TEST -- evidence\n\
         ocaml_version : %s\n\
         logs_export   : ok=%b rejected=%Ld\n\
         metrics_export: ok=%b rejected=%Ld\n\
         partial_success (REJECT1): rejected=%Ld (expect 1)\n\
         empty_is_noop : %b\n\
         token_mints   : %d (expect 1 — cached across exports)\n\
         %!"
        Sys.ocaml_version o.logs_ok o.logs_rejected o.metrics_ok
        o.metrics_rejected o.partial_rejected o.empty_ok o.mints;
      o )

let () =
  Alcotest.run "otlp-eio"
    [
      ( "otlp-export-eio",
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
