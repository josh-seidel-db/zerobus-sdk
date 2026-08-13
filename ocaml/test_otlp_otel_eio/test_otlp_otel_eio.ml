(** Cross-implementation OTLP acceptance (Eio): our {!Zerobus_otlp_eio} exporter
    (vendored [Zerobus_otlp_proto]) against a mock collector that decodes with
    the canonical upstream [opentelemetry.proto] types (otel_collector_eio). The
    Eio counterpart of test_otlp_otel/test_otlp_otel_lwt.ml — proves genuine
    OTLP wire-compatibility for the Eio exporter (independent encoder/decoder
    impls).

    Cleartext h2c, loopback, ephemeral ports. Runs on zbeio (OCaml 5.x). *)

module Otlp = Zerobus_otlp_eio
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics

let table = "main.default.otlp_otel_mock"
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
          {|{"access_token":"tok-otel-1","token_type":"Bearer","expires_in":3600}|}
        ()
    end
    else Cohttp_eio.Server.respond_string ~status:`Not_found ~body:"" ()
  in
  Eio.Fiber.fork_daemon ~sw (fun () ->
      Cohttp_eio.Server.run socket (Cohttp_eio.Server.make ~callback:handler ())
        ~on_error:(fun _ -> ()));
  Printf.sprintf "http://127.0.0.1:%d" port

let collector_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "otel_collector_eio.exe" in
  if Sys.file_exists cand then cand else "./otel_collector_eio.exe"

let with_collector ~log_path (f : int -> 'a) : 'a =
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
  Fun.protect
    ~finally:(fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      try close_in ic with _ -> ())
    (fun () -> f port)

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

let run ~env ~sw : outcome =
  let token_url = start_token_server ~sw ~env in
  let log_path = Filename.temp_file "otel_collector_eio" ".log" in
  with_collector ~log_path (fun port ->
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
          Eio.Time.sleep (Eio.Stdenv.clock env) 0.2;
          let clog = read_file log_path in
          let ok = function Ok _ -> true | Error _ -> false in
          let rej = function
            | Ok (r : Otlp.export_result) -> r.rejected
            | Error _ -> -1L
          in
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
    ( Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      let o = run ~env ~sw in
      Printf.eprintf
        "ZEROBUS OCAML — OTLP CROSS-IMPL TEST (Eio, canonical \
         opentelemetry.proto)\n\
         logs ok=%b rejected=%Ld ; metrics ok=%b ; partial rejected=%Ld\n\
         token_mints=%d ; canonical-decoded 2 logs=%b ; saw INFO=%b\n\
         %!"
        o.logs_ok o.logs_rejected o.metrics_ok o.partial_rejected o.mints
        o.collector_saw_2_logs o.collector_saw_info;
      o )

let () =
  Alcotest.run "otlp-otel-eio"
    [
      ( "cross-impl-export-eio",
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
