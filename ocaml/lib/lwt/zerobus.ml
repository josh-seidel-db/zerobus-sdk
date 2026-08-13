(** The public Lwt API for the Zerobus Ingest SDK (DESIGN.md §5.2, §5.3).

    This module bridges {!Zerobus_core.Make(Zerobus_io_lwt)} to provide an
    ergonomic entry point: [create] constructs a client from workspace
    credentials, and [create_stream] mints a table-scoped OAuth token, opens a
    stream to the gRPC endpoint, and returns a live [stream] ready for
    ingestion.

    The cardinal rule of ingestion: queue records in a loop, then call [flush]
    once. Per-record waiting defeats pipelining and loses orders of magnitude of
    throughput. *)

module Core = Zerobus_core
module Io_lwt = Zerobus_io_lwt

let ( let* ) = Lwt.bind

type t = {
  workspace_url : string;
  workspace_id : string;
  endpoint_host : string;
  endpoint_port : int;
  application_name : string option; [@ocaml.warning "-69"]
  mutable token_cache : (string * float) option;
  token_cache_lock : Lwt_mutex.t;
}
(** A Zerobus client holds workspace credentials and endpoint metadata.

    The public type is opaque; it holds:
    - workspace_url: the Databricks workspace URL (for metadata)
    - workspace_id: extracted workspace-id
    - endpoint_host, endpoint_port: the gRPC endpoint
    - application_name: optional name for diagnostics
    - token_cache: Lwt_mutex-guarded (token, expiry) ref for OAuth token caching
*)

(** The concrete driver behind a stream: the default [EphemeralStream] driver
    (JSON/Proto) or the Arrow/Flight [DoPut] driver, chosen by [record_type] at
    open time. Both are the same runtime-generic driver over the Lwt IO,
    differing only in the wire protocol — so each op just dispatches to the
    right one. *)
type stream_impl =
  | Ephemeral of Io_lwt.Stream.stream
  | Flight of Io_lwt.Stream_flight.stream

type stream = {
  impl : stream_impl;
  scope : Io_lwt.Scope.t;
  table_name : string; [@ocaml.warning "-69"]
}
(** A live stream bound to a long-lived scope (§5.2, §5.3).

    The stream holds:
    - impl: the concrete driver stream (EphemeralStream or Flight/DoPut)
    - scope: the Io_lwt.Scope.t that keeps the ack-reader alive
    - table_name: for error diagnostics *)

type offset = Core.Options.offset
(** An error handle is an offset the caller can wait on (DESIGN.md §5.3(a)). *)

(** The three offset-based ingestion operations (re-exported from core). *)

(* Silence warnings about unused record fields (used only to build/match, not access). *)
let _ = fun (x : t) -> ignore x.application_name
let _ = fun (x : stream) -> ignore x.table_name

let ingest stream buf =
  match stream.impl with
  | Ephemeral s -> Io_lwt.Stream.ingest s buf
  | Flight s -> Io_lwt.Stream_flight.ingest s buf

let ingest_records stream bufs =
  match stream.impl with
  | Ephemeral s -> Io_lwt.Stream.ingest_records s bufs
  | Flight s -> Io_lwt.Stream_flight.ingest_records s bufs

let wait_for_offset stream off =
  match stream.impl with
  | Ephemeral s -> Io_lwt.Stream.wait_for_offset s off
  | Flight s -> Io_lwt.Stream_flight.wait_for_offset s off

let flush stream =
  match stream.impl with
  | Ephemeral s -> Io_lwt.Stream.flush s
  | Flight s -> Io_lwt.Stream_flight.flush s

(** Close the stream and tear down its scope (cancels ack-reader).

    Idempotent: safe to call multiple times or after an error. *)
let close stream =
  let* result =
    match stream.impl with
    | Ephemeral s -> Io_lwt.Stream.close s
    | Flight s -> Io_lwt.Stream_flight.close s
  in
  (* Cancel and join all scope daemons *)
  List.iter Lwt.cancel stream.scope.daemons;
  let* () =
    Lwt.join
      (List.map
         (fun d -> Lwt.catch (fun () -> d) (fun _ -> Lwt.return_unit))
         stream.scope.daemons)
  in
  Lwt.return result

type table_properties = Core.Options.table_properties
(** Re-export table_properties and stream_options for convenience. *)

type stream_options = Core.Options.stream_options

let default_stream_options = Core.Options.default_stream_options

(* Open the stream on the driver selected by [record_type]: Arrow → the Flight
   [DoPut] driver, everything else → the default [EphemeralStream] driver. Both use
   the same Lwt IO + scope; only the wire protocol differs. Returns the wrapped
   [stream_impl] (or the driver's error). *)
let open_impl ~host ~port ~tls ~headers ~(options : stream_options)
    ~(table : table_properties) scope : (stream_impl, Core.Error.t) result Lwt.t
    =
  match options.Core.Options.record_type with
  | Core.Options.Arrow ->
      let* r =
        Io_lwt.Stream_flight.open_stream ~host ~port ~tls ~headers ~options
          ~table scope
      in
      Lwt.return (Result.map (fun s -> Flight s) r)
  | Core.Options.Json | Core.Options.Proto ->
      let* r =
        Io_lwt.Stream.open_stream ~host ~port ~tls ~headers ~options ~table
          scope
      in
      Lwt.return (Result.map (fun s -> Ephemeral s) r)

(** Construct a Zerobus client from workspace credentials.

    Parses the workspace URL to extract the workspace-id and derives the gRPC
    endpoint. If [endpoint] is provided and non-empty, it is used as-is;
    otherwise, the endpoint is derived as
    [<workspace-id>.zerobus.<region>.cloud.databricks.com:443].

    The [application_name] is optional and used for user-agent headers (future).

    Raises [Auth_error] or [Transport_error] if the workspace URL cannot be
    parsed. *)
let create ?(application_name : string option) ~endpoint ~workspace_url () :
    (t, Core.Error.t) result Lwt.t =
  try
    let* wsid_result =
      Lwt.return (Core.Config.workspace_id_of_url workspace_url)
    in
    match wsid_result with
    | Error e -> Lwt.return (Error e)
    | Ok wsid -> (
        let* endpoint_result =
          Lwt.return
            (Core.Config.endpoint_of_workspace ~endpoint ~workspace_url)
        in
        match endpoint_result with
        | Error e -> Lwt.return (Error e)
        | Ok (host, port) ->
            let client =
              {
                workspace_url;
                workspace_id = wsid;
                endpoint_host = host;
                endpoint_port = port;
                application_name;
                token_cache = None;
                token_cache_lock = Lwt_mutex.create ();
              }
            in
            Lwt.return (Ok client))
  with exn ->
    Lwt.return (Error (Core.Error.Transport_error (Printexc.to_string exn)))

(** Mint a table-scoped OAuth token via client-credentials grant.

    Makes an HTTP POST to <workspace_url>/oidc/v1/token with:
    - grant_type=client_credentials
    - scope=all-apis
    - resource=api://databricks/workspaces/<workspace_id>/zerobusDirectWriteApi
    - authorization_details: UC privileges for the table (catalog/schema/table)

    The token is cached with its expiry; tokens are refreshed ~30s before
    expiry.

    Returns [Auth_error] on HTTP failure or malformed response. *)
let mint_token ~client ~table ~client_id ~client_secret :
    (string, Core.Error.t) result Lwt.t =
  Lwt_mutex.with_lock client.token_cache_lock (fun () ->
      (* Check if we have a cached token that is still fresh (30s buffer) *)
      let now = Unix.gettimeofday () in
      match client.token_cache with
      | Some (token, expiry) when now +. 30.0 < expiry -> Lwt.return (Ok token)
      | _ -> (
          (* Build the token request body *)
          let* body_result =
            Lwt.return
              (Core.Config.oauth_token_request_body
                 ~workspace_id:client.workspace_id ~table)
          in
          match body_result with
          | Error e -> Lwt.return (Error e)
          | Ok body -> (
              (* POST to the token endpoint *)
              let token_url = client.workspace_url ^ "/oidc/v1/token" in
              let basic =
                Base64.encode_string (client_id ^ ":" ^ client_secret)
              in
              let headers =
                Cohttp.Header.of_list
                  [
                    ("Content-Type", "application/x-www-form-urlencoded");
                    ("Authorization", "Basic " ^ basic);
                  ]
              in
              try
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
                       (Core.Error.Auth_error
                          (Printf.sprintf "token endpoint HTTP %d" status)))
                else
                  (* Parse the JSON response *)
                  try
                    let json = Yojson.Safe.from_string resp_str in
                    let field k =
                      match json with
                      | `Assoc l -> List.assoc_opt k l
                      | _ -> None
                    in
                    let token =
                      match field "access_token" with
                      | Some (`String t) -> Some t
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
                    | Some t ->
                        let expiry = now +. expires_in in
                        client.token_cache <- Some (t, expiry);
                        Lwt.return (Ok t)
                    | None ->
                        Lwt.return
                          (Error
                             (Core.Error.Auth_error
                                "no access_token in response"))
                  with _ ->
                    Lwt.return
                      (Error (Core.Error.Auth_error "malformed token response"))
              with exn ->
                Lwt.return
                  (Error
                     (Core.Error.Transport_error
                        (Printf.sprintf "token request failed: %s"
                           (Printexc.to_string exn)))))))

(** Open a stream to the table with client-credentials OAuth.

    Mints a table-scoped token, builds gRPC headers with the bearer token and
    table name, creates a long-lived scope, and opens the stream via
    {!Core.Make(Io_lwt).open_stream}.

    The returned [stream] owns the scope and must be closed with {!close}.

    IMPORTANT: The scope is created here and torn down by {!close}. The caller
    must not use the stream after [close] is called. *)
let create_stream client (table_props : table_properties) ~client_id
    ~client_secret ?(options = default_stream_options) () :
    (stream, Core.Error.t) result Lwt.t =
  let* token_result =
    mint_token ~client ~table:table_props.table_name ~client_id ~client_secret
  in
  match token_result with
  | Error e -> Lwt.return (Error e)
  | Ok token -> (
      (* Build gRPC headers *)
      let headers =
        [
          ("authorization", "Bearer " ^ token);
          ("x-databricks-zerobus-table-name", table_props.table_name);
          ( ":authority",
            Printf.sprintf "%s:%d" client.endpoint_host client.endpoint_port );
        ]
      in

      (* Create a long-lived scope (not using with_scope, which closes on return) *)
      let scope = { Io_lwt.Scope.daemons = [] } in

      (* Open the stream inside the scope (driver selected by record_type). *)
      let* stream_result =
        open_impl ~host:client.endpoint_host ~port:client.endpoint_port
          ~tls:true ~headers ~options ~table:table_props scope
      in
      match stream_result with
      | Error e ->
          (* If opening fails, cancel and join scope daemons *)
          List.iter Lwt.cancel scope.daemons;
          let* () =
            Lwt.join
              (List.map
                 (fun d -> Lwt.catch (fun () -> d) (fun _ -> Lwt.return_unit))
                 scope.daemons)
          in
          Lwt.return (Error e)
      | Ok impl ->
          let stream = { impl; scope; table_name = table_props.table_name } in
          Lwt.return (Ok stream))

(** Custom-auth variant: supply headers directly instead of using OAuth.

    Useful for federated auth, custom bearer token schemes, or when the caller
    already has a valid OAuth token.

    The [headers_provider] is called each time a new token/headers are needed
    (e.g. on recovery reconnect). It should return a list of tuples
    [(name, value)].

    LIMITATION: This version does not cache tokens or refresh them on expiry.
    For production use, the caller should implement their own caching in the
    [headers_provider]. *)
let create_stream_with_headers ?(tls = true) client
    (table_props : table_properties) ~headers_provider
    ?(options = default_stream_options) () : (stream, Core.Error.t) result Lwt.t
    =
  let* headers_result = headers_provider () in
  match headers_result with
  | Error e -> Lwt.return (Error e)
  | Ok headers -> (
      (* Ensure :authority is set *)
      let headers =
        let has_authority =
          List.exists (fun (k, _) -> k = ":authority") headers
        in
        if has_authority then headers
        else
          ( ":authority",
            Printf.sprintf "%s:%d" client.endpoint_host client.endpoint_port )
          :: headers
      in

      let scope = { Io_lwt.Scope.daemons = [] } in

      let* stream_result =
        open_impl ~host:client.endpoint_host ~port:client.endpoint_port ~tls
          ~headers ~options ~table:table_props scope
      in
      match stream_result with
      | Error e ->
          List.iter Lwt.cancel scope.daemons;
          let* () =
            Lwt.join
              (List.map
                 (fun d -> Lwt.catch (fun () -> d) (fun _ -> Lwt.return_unit))
                 scope.daemons)
          in
          Lwt.return (Error e)
      | Ok impl ->
          let stream = { impl; scope; table_name = table_props.table_name } in
          Lwt.return (Ok stream))

(* Low-level access for tests / advanced callers: the raw IO instantiation and the
   Make(Io) streaming driver, so a caller can drive open_stream directly (e.g. a
   cleartext-h2c mock without OAuth). Not the ergonomic path — prefer create_stream. *)
module Io_lwt_for_test = Io_lwt
module Driver = Io_lwt.Stream
