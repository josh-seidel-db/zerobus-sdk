(** The OTLP exporter for the Zerobus Ingest SDK — Eio runtime (DESIGN.md §4.3).

    The Eio (direct-style) counterpart of {!Zerobus_otlp} (the Lwt reference).
    Same two unary Export RPCs over the same TLS 1.3 + ALPN-h2 transport — but
    reusing the {b Eio} H2 client ([Zerobus_eio.Io_eio_for_test.H2_client]) and
    a [cohttp-eio] token mint. The OTLP wire types come from the shared
    [Zerobus_otlp_proto] library (vendored OpenTelemetry protos), identical to
    the Lwt exporter.

    See {!Zerobus_otlp_eio} (the .mli) for the contract. *)

module Config = Zerobus_core.Config
module Error = Zerobus_core.Error
module H2 = Zerobus_eio.Io_eio_for_test.H2_client
module Ctx = Zerobus_eio.Io_eio_for_test.Ctx
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
  application_name : string option; [@ocaml.warning "-69"]
  client_id : string;
  client_secret : string;
  mutable token_cache : (string * float) option;
  token_cache_lock : Eio.Mutex.t;
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
              token_cache_lock = Eio.Mutex.create ();
            })

(* Build a cohttp-eio client whose TLS layer uses the system trust store; a
   cleartext http:// token URL (the mock) sidesteps TLS. Mirrors
   Zerobus_eio.mint_token / Zerobus_rest_eio. *)
let make_httpc env =
  Mirage_crypto_rng_unix.use_default ();
  let authenticator =
    match Ca_certs.authenticator () with
    | Ok a -> a
    | Error (`Msg m) -> failwith ("ca-certs: " ^ m)
  in
  let https =
    let cfg =
      match Tls.Config.client ~authenticator () with
      | Ok c -> c
      | Error (`Msg m) -> failwith ("tls config: " ^ m)
    in
    fun uri raw ->
      let host =
        Uri.host uri
        |> Option.map (fun h -> Domain_name.(host_exn (of_string_exn h)))
      in
      Tls_eio.client_of_flow ?host cfg raw
  in
  Cohttp_eio.Client.make ~https:(Some https) (Eio.Stdenv.net env)

(* Mint (or reuse a cached) table-scoped token — same client-credentials grant as
   the Lwt {!Zerobus_otlp.mint_token}, 30s early-refresh. Scoped to [t.table]. *)
let mint_token ~env ~sw t : (string, error) result =
  Eio.Mutex.use_rw ~protect:true t.token_cache_lock (fun () ->
      let now = Unix.gettimeofday () in
      match t.token_cache with
      | Some (token, expiry) when now +. 30.0 < expiry -> Ok token
      | _ -> (
          match
            Config.oauth_token_request_body ~workspace_id:t.workspace_id
              ~table:t.table
          with
          | Error e -> Error e
          | Ok body -> (
              try
                let httpc = make_httpc env in
                let basic =
                  Base64.encode_string (t.client_id ^ ":" ^ t.client_secret)
                in
                let headers =
                  Http.Header.of_list
                    [
                      ("Content-Type", "application/x-www-form-urlencoded");
                      ("Authorization", "Basic " ^ basic);
                    ]
                in
                let uri = Uri.of_string (t.workspace_url ^ "/oidc/v1/token") in
                let resp, rbody =
                  Cohttp_eio.Client.post ~sw httpc ~headers
                    ~body:(Cohttp_eio.Body.of_string body)
                    uri
                in
                let resp_str =
                  Eio.Buf_read.(parse_exn take_all) rbody ~max_size:max_int
                in
                let status = Http.Status.to_int resp.Http.Response.status in
                if status < 200 || status >= 300 then
                  Error
                    (Error.Auth_error
                       (Printf.sprintf "token endpoint HTTP %d" status))
                else
                  match Yojson.Safe.from_string resp_str with
                  | `Assoc l -> (
                      let token =
                        match List.assoc_opt "access_token" l with
                        | Some (`String s) -> Some s
                        | _ -> None
                      in
                      let expires_in =
                        match List.assoc_opt "expires_in" l with
                        | Some (`Int n) -> float_of_int n
                        | Some (`Float f) -> f
                        | Some (`String s) -> (
                            try float_of_string s with _ -> 3600.0)
                        | _ -> 3600.0
                      in
                      match token with
                      | Some tok ->
                          t.token_cache <- Some (tok, now +. expires_in);
                          Ok tok
                      | None ->
                          Error (Error.Auth_error "no access_token in response")
                      )
                  | _ -> Error (Error.Auth_error "malformed token response")
              with exn ->
                Error
                  (Error.Transport_error
                     (Printf.sprintf "token request failed: %s"
                        (Printexc.to_string exn))))))

(* The generic unary-Export path — same shape as the Lwt reference but
   direct-style: connect, open the RPC, send the one framed request, half-close,
   read one response, check the gRPC status, tear down. The Eio H2 client reads
   net/sw from [Ctx], installed by the caller-supplied env/sw. *)
let unary_export ~env ~sw t ~service ~rpc ~(encode : unit -> string)
    ~(decode : string -> export_result) : (export_result, error) result =
  match mint_token ~env ~sw t with
  | Error e -> Error e
  | Ok token ->
      Ctx.with_env ~net:(Eio.Stdenv.net env) ~sw (fun () ->
          let headers =
            [
              ("authorization", "Bearer " ^ token);
              ("x-databricks-zerobus-table-name", t.table);
              ( ":authority",
                Printf.sprintf "%s:%d" t.endpoint_host t.endpoint_port );
            ]
          in
          match
            H2.connect ~host:t.endpoint_host ~port:t.endpoint_port ~tls:t.tls
              ~headers ()
          with
          | Error e -> Error e
          | Ok conn -> (
              let finish result =
                H2.shutdown conn;
                result
              in
              match H2.start_bidi conn ~service ~rpc () with
              | Error e -> finish (Error e)
              | Ok call -> (
                  match H2.send call (encode ()) with
                  | Error e -> finish (Error e)
                  | Ok () -> (
                      match H2.close_send call with
                      | Error e -> finish (Error e)
                      | Ok () -> (
                          match H2.recv call with
                          | Error e -> finish (Error e)
                          | Ok resp_opt -> (
                              (* Drain to end-of-stream so the gRPC status
                                 trailer is observed. *)
                              let rec drain () =
                                match H2.recv call with
                                | Ok (Some _) -> drain ()
                                | _ -> ()
                              in
                              (match resp_opt with
                              | Some _ -> drain ()
                              | None -> ());
                              match H2.status call with
                              | Error e -> finish (Error e)
                              | Ok () -> (
                                  match resp_opt with
                                  | Some resp -> finish (Ok (decode resp))
                                  | None ->
                                      finish
                                        (Ok
                                           { rejected = 0L; error_message = "" })
                                  )))))))

let export_logs ~env ~sw t (rls : OLogs.resource_logs list) :
    (export_result, error) result =
  match rls with
  | [] -> Ok { rejected = 0L; error_message = "" }
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
      unary_export ~env ~sw t
        ~service:"opentelemetry.proto.collector.logs.v1.LogsService"
        ~rpc:"Export" ~encode ~decode

let export_metrics ~env ~sw t (rms : OMetrics.resource_metrics list) :
    (export_result, error) result =
  match rms with
  | [] -> Ok { rejected = 0L; error_message = "" }
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
      unary_export ~env ~sw t
        ~service:"opentelemetry.proto.collector.metrics.v1.MetricsService"
        ~rpc:"Export" ~encode ~decode
