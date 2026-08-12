(** The OTLP exporter for the Zerobus Ingest SDK — Async runtime (DESIGN.md §4.3).

    The Async counterpart of {!Zerobus_otlp} (the Lwt reference). Same two unary
    Export RPCs over the same TLS 1.3 + ALPN-h2 transport — but reusing the {b
    Async} H2 client ([Zerobus_async.Io_async_for_test.H2_client]) and a
    [cohttp-async] token mint. The OTLP wire types come from the shared
    [Zerobus_otlp_proto] library (vendored OpenTelemetry protos), identical to the
    Lwt exporter.

    This library is [optional]: it only builds on a switch where [cohttp-async] is
    installed (which downgrades cohttp 6.0->5.3 — benign, see
    doc/arch/tls_async_status.md). See {!Zerobus_otlp_async} (the .mli) for the
    contract. *)

open! Core
open! Async
module Config = Zerobus_core.Config
module Error = Zerobus_core.Error
module H2 = Zerobus_async.Io_async_for_test.H2_client
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics
module LSvc = Zerobus_otlp_proto.Logs_service
module MSvc = Zerobus_otlp_proto.Metrics_service

type error = Error.t
type export_result = { rejected : int64; error_message : string }

type t = {
  workspace_url : string;
  workspace_id : string;
  endpoint_host : string;
  endpoint_port : int;
  table : string;
  tls : bool;
  application_name : string option [@ocaml.warning "-69"];
  client_id : string;
  client_secret : string;
  mutable token_cache : (string * float) option;
  token_cache_lock : unit Sequencer.t;
}

let _ = fun (x : t) -> ignore x.application_name

let create ?(application_name : string option) ?(tls = true) ~endpoint
    ~workspace_url ~table ~client_id ~client_secret () : (t, error) result =
  match Config.workspace_id_of_url workspace_url with
  | Error e -> Error e
  | Ok workspace_id -> (
      match Config.endpoint_of_workspace ~endpoint ~workspace_url with
      | Error e -> Error e
      | Ok (endpoint_host, endpoint_port) ->
          Ok
            {
              workspace_url;
              workspace_id;
              endpoint_host;
              endpoint_port;
              table;
              tls;
              application_name;
              client_id;
              client_secret;
              token_cache = None;
              token_cache_lock = Sequencer.create ();
            })

(* Mint (or reuse a cached) table-scoped token — same client-credentials grant as
   the Lwt {!Zerobus_otlp.mint_token}, 30s early-refresh. Scoped to [t.table]. The
   HTTP POST is the shared pure-ocaml-tls backend from zerobus-async
   ({!Zerobus_async.Oauth}, over [Tls_async.connect]) — NOT cohttp-async. *)
let mint_token t : (string, error) result Deferred.t =
  Throttle.enqueue t.token_cache_lock (fun () ->
      let now = Unix.gettimeofday () in
      match t.token_cache with
      | Some (token, expiry) when Float.( < ) (now +. 30.0) expiry ->
          return (Ok token)
      | _ -> (
          match
            Config.oauth_token_request_body ~workspace_id:t.workspace_id
              ~table:t.table
          with
          | Error e -> return (Error e)
          | Ok body -> (
              let token_url = t.workspace_url ^ "/oidc/v1/token" in
              match%map
                Zerobus_async.Oauth.post_token ~token_url
                  ~client_id:t.client_id ~client_secret:t.client_secret ~body
              with
              | Error exn ->
                  Error
                    (Error.Transport_error
                       (Printf.sprintf "token request failed: %s"
                          (Exn.to_string exn)))
              | Ok (code, body_str) ->
                  if code < 200 || code >= 300 then
                    Error
                      (Error.Auth_error
                         (Printf.sprintf "token endpoint HTTP %d" code))
                  else (
                    match Zerobus_async.Oauth.parse_token_response ~now body_str with
                    | Some (tok, expiry) ->
                        t.token_cache <- Some (tok, expiry);
                        Ok tok
                    | None -> Error (Error.Auth_error "malformed token response"))
              )))

(* The generic unary-Export path — same shape as the Lwt reference, Async idioms:
   connect, open the RPC, send the one framed request, half-close, read one
   response, check the gRPC status, tear down. *)
let unary_export t ~service ~rpc ~(encode : unit -> string)
    ~(decode : string -> export_result) : (export_result, error) result Deferred.t
    =
  match%bind mint_token t with
  | Error e -> return (Error e)
  | Ok token -> (
      let headers =
        [
          ("authorization", "Bearer " ^ token);
          ("x-databricks-zerobus-table-name", t.table);
          (":authority", Printf.sprintf "%s:%d" t.endpoint_host t.endpoint_port);
        ]
      in
      match%bind
        H2.connect ~host:t.endpoint_host ~port:t.endpoint_port ~tls:t.tls
          ~headers ()
      with
      | Error e -> return (Error e)
      | Ok conn ->
          let finish result =
            let%map () = H2.shutdown conn in
            result
          in
          let%bind call_r = H2.start_bidi conn ~service ~rpc () in
          (match call_r with
          | Error e -> finish (Error e)
          | Ok call -> (
              let%bind send_r = H2.send call (encode ()) in
              match send_r with
              | Error e -> finish (Error e)
              | Ok () -> (
                  let%bind close_r = H2.close_send call in
                  match close_r with
                  | Error e -> finish (Error e)
                  | Ok () -> (
                      let%bind recv_r = H2.recv call in
                      match recv_r with
                      | Error e -> finish (Error e)
                      | Ok resp_opt -> (
                          (* Drain to end-of-stream so the gRPC status trailer is
                             observed. *)
                          let rec drain () =
                            match%bind H2.recv call with
                            | Ok (Some _) -> drain ()
                            | _ -> return ()
                          in
                          let%bind () =
                            match resp_opt with
                            | Some _ -> drain ()
                            | None -> return ()
                          in
                          let%bind status_r = H2.status call in
                          match status_r with
                          | Error e -> finish (Error e)
                          | Ok () -> (
                              match resp_opt with
                              | Some resp -> finish (Ok (decode resp))
                              | None ->
                                  finish
                                    (Ok { rejected = 0L; error_message = "" }))
                          ))))))

let export_logs t (rls : OLogs.resource_logs list) :
    (export_result, error) result Deferred.t =
  match rls with
  | [] -> return (Ok { rejected = 0L; error_message = "" })
  | _ ->
      let encode () =
        let req = LSvc.make_export_logs_service_request ~resource_logs:rls () in
        let enc = Pbrt.Encoder.create () in
        LSvc.encode_pb_export_logs_service_request req enc;
        Pbrt.Encoder.to_string enc
      in
      let decode s =
        let resp =
          LSvc.decode_pb_export_logs_service_response (Pbrt.Decoder.of_string s)
        in
        match resp.LSvc.partial_success with
        | Some ps ->
            {
              rejected = ps.LSvc.rejected_log_records;
              error_message = ps.LSvc.error_message;
            }
        | None -> { rejected = 0L; error_message = "" }
      in
      unary_export t
        ~service:"opentelemetry.proto.collector.logs.v1.LogsService" ~rpc:"Export"
        ~encode ~decode

let export_metrics t (rms : OMetrics.resource_metrics list) :
    (export_result, error) result Deferred.t =
  match rms with
  | [] -> return (Ok { rejected = 0L; error_message = "" })
  | _ ->
      let encode () =
        let req =
          MSvc.make_export_metrics_service_request ~resource_metrics:rms ()
        in
        let enc = Pbrt.Encoder.create () in
        MSvc.encode_pb_export_metrics_service_request req enc;
        Pbrt.Encoder.to_string enc
      in
      let decode s =
        let resp =
          MSvc.decode_pb_export_metrics_service_response
            (Pbrt.Decoder.of_string s)
        in
        match resp.MSvc.partial_success with
        | Some ps ->
            {
              rejected = ps.MSvc.rejected_data_points;
              error_message = ps.MSvc.error_message;
            }
        | None -> { rejected = 0L; error_message = "" }
      in
      unary_export t
        ~service:"opentelemetry.proto.collector.metrics.v1.MetricsService"
        ~rpc:"Export" ~encode ~decode
