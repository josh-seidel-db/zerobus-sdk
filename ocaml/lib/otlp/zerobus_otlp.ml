(** The OTLP exporter for the Zerobus Ingest SDK (DESIGN.md §4.3).

    See {!Zerobus_otlp} (the .mli) for the contract. A unary [Export] over the
    same TLS+ALPN-h2 transport the gRPC SDK uses ([Zerobus.Io_lwt_for_test.H2_client]),
    plus the same scoped client-credentials OAuth grant
    ([Zerobus_core.Config.oauth_token_request_body]). *)

module Config = Zerobus_core.Config
module Error = Zerobus_core.Error
module H2 = Zerobus.Io_lwt_for_test.H2_client
module OLogs = Zerobus_otlp_proto.Logs
module OMetrics = Zerobus_otlp_proto.Metrics
module LSvc = Zerobus_otlp_proto.Logs_service
module MSvc = Zerobus_otlp_proto.Metrics_service

let ( let* ) = Lwt.bind

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
  token_cache_lock : Lwt_mutex.t;
}

let _ = fun (x : t) -> ignore x.application_name

let create ?(application_name : string option) ?(tls = true) ~endpoint
    ~workspace_url ~table ~client_id ~client_secret () : (t, error) result Lwt.t =
  match Config.workspace_id_of_url workspace_url with
  | Error e -> Lwt.return (Error e)
  | Ok workspace_id -> (
      match Config.endpoint_of_workspace ~endpoint ~workspace_url with
      | Error e -> Lwt.return (Error e)
      | Ok (endpoint_host, endpoint_port) ->
          Lwt.return
            (Ok
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
                 token_cache_lock = Lwt_mutex.create ();
               }))

(* Mint (or reuse a cached) table-scoped token — same client-credentials grant as
   Zerobus/Zerobus_rest, 30s early-refresh. The token is scoped to [t.table]. *)
let mint_token t : (string, error) result Lwt.t =
  Lwt_mutex.with_lock t.token_cache_lock (fun () ->
      let now = Unix.gettimeofday () in
      match t.token_cache with
      | Some (token, expiry) when now +. 30.0 < expiry -> Lwt.return (Ok token)
      | _ -> (
          match
            Config.oauth_token_request_body ~workspace_id:t.workspace_id
              ~table:t.table
          with
          | Error e -> Lwt.return (Error e)
          | Ok body -> (
              let token_url = t.workspace_url ^ "/oidc/v1/token" in
              let basic =
                Base64.encode_string (t.client_id ^ ":" ^ t.client_secret)
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
                         (Error.Auth_error
                            (Printf.sprintf "token endpoint HTTP %d" status)))
                  else
                    try
                      let json = Yojson.Safe.from_string resp_str in
                      let field k =
                        match json with
                        | `Assoc l -> List.assoc_opt k l
                        | _ -> None
                      in
                      let token =
                        match field "access_token" with
                        | Some (`String s) -> Some s
                        | _ -> None
                      in
                      let expires_in =
                        match field "expires_in" with
                        | Some (`Int n) -> float_of_int n
                        | Some (`Float f) -> f
                        | Some (`String s) -> (
                            try float_of_string s with _ -> 3600.0)
                        | _ -> 3600.0
                      in
                      match token with
                      | Some tok ->
                          t.token_cache <- Some (tok, now +. expires_in);
                          Lwt.return (Ok tok)
                      | None ->
                          Lwt.return
                            (Error
                               (Error.Auth_error "no access_token in response"))
                    with _ ->
                      Lwt.return
                        (Error (Error.Auth_error "malformed token response")))
                (fun exn ->
                  Lwt.return
                    (Error
                       (Error.Transport_error
                          (Printf.sprintf "token request failed: %s"
                             (Printexc.to_string exn))))))))

(* The generic unary-Export path: connect, open the RPC, send the one framed
   request, half-close, read exactly one response frame, check the gRPC status,
   tear down. [encode] frames the request proto; [decode] parses the response and
   extracts (rejected, error_message). *)
let unary_export t ~service ~rpc ~(encode : unit -> string)
    ~(decode : string -> export_result) : (export_result, error) result Lwt.t =
  let* token_r = mint_token t in
  match token_r with
  | Error e -> Lwt.return (Error e)
  | Ok token -> (
      let headers =
        [
          ("authorization", "Bearer " ^ token);
          ("x-databricks-zerobus-table-name", t.table);
          ( ":authority",
            Printf.sprintf "%s:%d" t.endpoint_host t.endpoint_port );
        ]
      in
      let* conn_r =
        H2.connect ~host:t.endpoint_host ~port:t.endpoint_port ~tls:t.tls
          ~headers ()
      in
      match conn_r with
      | Error e -> Lwt.return (Error e)
      | Ok conn ->
          let finish result =
            let* () = H2.shutdown conn in
            Lwt.return result
          in
          let* call_r = H2.start_bidi conn ~service ~rpc () in
          (match call_r with
          | Error e -> finish (Error e)
          | Ok call -> (
              let* send_r = H2.send call (encode ()) in
              match send_r with
              | Error e -> finish (Error e)
              | Ok () -> (
                  let* close_r = H2.close_send call in
                  match close_r with
                  | Error e -> finish (Error e)
                  | Ok () -> (
                      let* recv_r = H2.recv call in
                      match recv_r with
                      | Error e -> finish (Error e)
                      | Ok resp_opt -> (
                          (* Drain to end-of-stream so the gRPC status trailer is
                             observed (status is only set once recv hits None). *)
                          let rec drain () =
                            let* r = H2.recv call in
                            match r with
                            | Ok (Some _) -> drain ()
                            | _ -> Lwt.return_unit
                          in
                          let* () =
                            match resp_opt with
                            | Some _ -> drain ()
                            | None -> Lwt.return_unit
                          in
                          let* status_r = H2.status call in
                          match status_r with
                          | Error e -> finish (Error e)
                          | Ok () -> (
                              match resp_opt with
                              | Some resp -> finish (Ok (decode resp))
                              | None ->
                                  (* OK status but no response message: treat as
                                     a clean full-accept. *)
                                  finish
                                    (Ok { rejected = 0L; error_message = "" }))
                          )))))
      )

let export_logs t (rls : OLogs.resource_logs list) :
    (export_result, error) result Lwt.t =
  match rls with
  | [] -> Lwt.return (Ok { rejected = 0L; error_message = "" })
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
      unary_export t ~service:"opentelemetry.proto.collector.logs.v1.LogsService"
        ~rpc:"Export" ~encode ~decode

let export_metrics t (rms : OMetrics.resource_metrics list) :
    (export_result, error) result Lwt.t =
  match rms with
  | [] -> Lwt.return (Ok { rejected = 0L; error_message = "" })
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
