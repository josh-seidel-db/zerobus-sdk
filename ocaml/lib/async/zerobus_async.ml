(** The public Async API for the Zerobus Ingest SDK (DESIGN.md §5.2, §5.3).

    The Async counterpart of the Lwt {!Zerobus} module. It bridges
    {!Zerobus_core.Make(Zerobus_io_async)} to an ergonomic entry point: [create]
    constructs a client from workspace metadata, [with_stream] opens a
    table-scoped stream given caller-supplied gRPC headers, and
    [with_stream_oauth] mints a token itself (built-in client-credentials
    OAuth).

    {b Scope of this façade.} It is bracket-shaped ([with_stream] /
    [with_stream_oauth]), matching Async's lack of hard fiber cancellation. Live
    TLS and built-in OAuth are both available as dune [select]s on optional deps
    ([tls-async]; [cohttp-async]): present → they work; absent →
    [with_stream_oauth] /[mint_token] return an [Auth_error] ([oauth_available]
    reports which) and TLS connects error honestly, so the caller supplies a
    bearer via [headers_provider] and (with [tls:false]) drives the full
    façade→driver path against a cleartext mock. Lwt/Eio are the always-on TLS
    references. See doc/arch/tls_async_status.md.

    The cardinal rule of ingestion: queue records in a loop, then call [flush]
    once. Per-record waiting defeats pipelining. *)

open! Core
open! Async
module Core_z = Zerobus_core
module Io_async = Zerobus_io_async

type t = {
  workspace_url : string;
  workspace_id : string;
  endpoint_host : string;
  endpoint_port : int;
  application_name : string option; [@warning "-69"]
  mutable token_cache : (string * float) option;
  token_cache_lock : Io_async.Mutex.t;
}
(** A Zerobus client holds workspace metadata and the derived gRPC endpoint.

    [token_cache]/[token_cache_lock] back the built-in client-credentials OAuth
    ({!mint_token}): a cached [(token, expiry)] refreshed ~30s before expiry,
    guarded by an [Io_async.Mutex] (a concurrency-1 Sequencer) so concurrent
    mints don't stampede the token endpoint — mirrors the Lwt reference. *)

(** The concrete driver behind a stream: the default [EphemeralStream] driver
    (JSON/Proto) or the Arrow/Flight [DoPut] driver, chosen by [record_type] at
    open time. Both are the same runtime-generic driver over the Async IO,
    differing only in the wire protocol — so each op dispatches to the right
    one. Mirrors the Lwt/Eio façades. *)
type stream_impl =
  | Ephemeral of Io_async.Stream.stream
  | Flight of Io_async.Stream_flight.stream

type stream = {
  impl : stream_impl;
  scope : Io_async.Scope.t;
  table_name : string; [@warning "-69"]
}
(** A live stream bound to a long-lived scope (§5.2, §5.3). *)

type offset = Core_z.Options.offset
(** An offset handle the caller can wait on (DESIGN.md §5.3(a)). *)

type table_properties = Core_z.Options.table_properties
type stream_options = Core_z.Options.stream_options

let default_stream_options = Core_z.Options.default_stream_options

(* Silence unused-field warnings (set for diagnostics / future use). *)
let _ = fun (x : t) -> ignore x.application_name
let _ = fun (x : stream) -> ignore x.table_name

(** {1 Ingestion operations (re-exported over the driver)} *)

let ingest stream buf =
  match stream.impl with
  | Ephemeral s -> Io_async.Stream.ingest s buf
  | Flight s -> Io_async.Stream_flight.ingest s buf

let ingest_records stream bufs =
  match stream.impl with
  | Ephemeral s -> Io_async.Stream.ingest_records s bufs
  | Flight s -> Io_async.Stream_flight.ingest_records s bufs

let wait_for_offset stream off =
  match stream.impl with
  | Ephemeral s -> Io_async.Stream.wait_for_offset s off
  | Flight s -> Io_async.Stream_flight.wait_for_offset s off

let flush stream =
  match stream.impl with
  | Ephemeral s -> Io_async.Stream.flush s
  | Flight s -> Io_async.Stream_flight.flush s

(** Close the stream and tear down its scope (best-effort join of the
    ack-reader). *)
let close stream =
  let%bind result =
    match stream.impl with
    | Ephemeral s -> Io_async.Stream.close s
    | Flight s -> Io_async.Stream_flight.close s
  in
  (* Signal the scope's daemons to stop and best-effort join without blocking on
     an uncancellable one (same discipline as Io_async.Scope.with_scope). *)
  Ivar.fill_if_empty stream.scope.Io_async.Scope.stop ();
  let%map () =
    Deferred.any
      [
        Deferred.all_unit stream.scope.Io_async.Scope.daemons;
        (* [Clock_ns]/[Time_ns] for async v0.15/v0.16 portability (the tls-async
           0.17.0 in-tree TLS test forces v0.15). See {!Zerobus_io_async}. *)
        Async.Clock_ns.after (Time_ns.Span.of_ms 0.);
      ]
  in
  result

(** Construct a client from workspace URL and optional gRPC endpoint.

    If [endpoint] is empty or "default", it is derived from [workspace_url] via
    {!Zerobus_core.Config}. Returns [Auth_error]/[Transport_error] on a URL that
    cannot be parsed. *)
let create ?(application_name : string option) ~endpoint ~workspace_url () :
    (t, Core_z.Error.t) result Deferred.t =
  match Core_z.Config.workspace_id_of_url workspace_url with
  | Error e -> return (Error e)
  | Ok wsid -> (
      match Core_z.Config.endpoint_of_workspace ~endpoint ~workspace_url with
      | Error e -> return (Error e)
      | Ok (host, port) ->
          return
            (Ok
               {
                 workspace_url;
                 workspace_id = wsid;
                 endpoint_host = host;
                 endpoint_port = port;
                 application_name;
                 token_cache = None;
                 token_cache_lock = Io_async.Mutex.create ();
               }))

(* Open the stream on the driver selected by [record_type]: Arrow → the Flight
   [DoPut] driver, everything else → the default [EphemeralStream] driver. Both use
   the same Async IO + scope; only the wire protocol differs. Mirrors the Lwt/Eio
   [open_impl]. *)
let open_impl ~host ~port ~tls ~headers ~(options : stream_options)
    ~(table : table_properties) scope :
    (stream_impl, Core_z.Error.t) result Deferred.t =
  match options.Core_z.Options.record_type with
  | Core_z.Options.Arrow ->
      let%map r =
        Io_async.Stream_flight.open_stream ~host ~port ~tls ~headers ~options
          ~table scope
      in
      Result.map r ~f:(fun s -> Flight s)
  | Core_z.Options.Json | Core_z.Options.Proto ->
      let%map r =
        Io_async.Stream.open_stream ~host ~port ~tls ~headers ~options ~table
          scope
      in
      Result.map r ~f:(fun s -> Ephemeral s)

(* Close a [stream_impl]'s underlying driver. *)
let close_impl = function
  | Ephemeral s -> Io_async.Stream.close s
  | Flight s -> Io_async.Stream_flight.close s

(** Bracket form: open a stream with caller-supplied gRPC headers (custom auth),
    run [f] with it inside the owning scope, then tear the stream + scope down.

    The [headers_provider] returns the header list (e.g. an [authorization]
    bearer token + the table-name header). [:authority] is added from the
    endpoint if the provider did not supply it. [tls] defaults to [true]; pass
    [false] for a cleartext-h2c mock backend.

    This is the entry point for Async: the bracket owns the {!Io_async.Scope}
    for the whole body, so the ack-reader daemon is forked into a live scope and
    torn down deterministically on exit — the reliable shape given Async's lack
    of hard fiber cancellation. (Detached create/return, as the Lwt façade
    offers, relies on the caller keeping the scheduler pumping; not exposed
    here.) *)
let with_stream ?(tls = true) client (table_props : table_properties)
    ~(headers_provider :
       unit -> ((string * string) list, Core_z.Error.t) result Deferred.t)
    ?(options = default_stream_options) (f : stream -> 'a Deferred.t) :
    ('a, Core_z.Error.t) result Deferred.t =
  Io_async.Scope.with_scope (fun scope ->
      let%bind headers_result = headers_provider () in
      match headers_result with
      | Error e -> return (Error e)
      | Ok headers -> (
          let headers =
            if List.Assoc.mem headers ":authority" ~equal:String.equal then
              headers
            else
              ( ":authority",
                Printf.sprintf "%s:%d" client.endpoint_host client.endpoint_port
              )
              :: headers
          in
          let%bind stream_result =
            open_impl ~host:client.endpoint_host ~port:client.endpoint_port ~tls
              ~headers ~options ~table:table_props scope
          in
          match stream_result with
          | Error e -> return (Error e)
          | Ok impl ->
              let stream =
                { impl; scope; table_name = table_props.table_name }
              in
              let%bind result = f stream in
              let%map _ = close_impl stream.impl in
              Ok result))

(** {1 Built-in client-credentials OAuth (§12.2)} *)

(** [true] iff cohttp-async was present at build time (dune [select]); see the
    .mli. *)
let oauth_available = Oauth.available

(** Mint a table-scoped OAuth token via the client-credentials grant: an HTTPS
    POST to [<workspace_url>/oidc/v1/token] with
    [grant_type=client_credentials], the Zerobus scope/resource, and the table's
    UC [authorization_details]; the [client_id]/[client_secret] go in the HTTP
    Basic header (not the body).

    The HTTP call is the {!Oauth} backend — a dune [select] on [cohttp-async]:
    present → the real POST (system trust store for TLS); absent → an honest
    error (supply a bearer via {!with_stream}'s [headers_provider] instead). The
    token is cached with its expiry and refreshed ~30s early; the cache is
    guarded by the client's mutex so concurrent mints don't stampede the
    endpoint. Mirrors the Lwt reference.

    Returns [Auth_error] on HTTP/parse failure, [Transport_error] on a request
    exception. *)
let mint_token ~(client : t) ~table ~client_id ~client_secret :
    (string, Core_z.Error.t) result Deferred.t =
  Io_async.Mutex.with_lock client.token_cache_lock (fun () ->
      let now = Core_unix.gettimeofday () in
      match client.token_cache with
      | Some (token, expiry) when Float.( < ) (now +. 30.0) expiry ->
          return (Ok token)
      | _ -> (
          match
            Core_z.Config.oauth_token_request_body
              ~workspace_id:client.workspace_id ~table
          with
          | Error e -> return (Error e)
          | Ok body -> (
              let token_url = client.workspace_url ^ "/oidc/v1/token" in
              match%bind
                Oauth.post_token ~token_url ~client_id ~client_secret ~body
              with
              | Error exn ->
                  return
                    (Error
                       (Core_z.Error.Transport_error
                          (Printf.sprintf "token request failed: %s"
                             (Exn.to_string exn))))
              | Ok (code, body_str) -> (
                  if code < 200 || code >= 300 then
                    return
                      (Error
                         (Core_z.Error.Auth_error
                            (Printf.sprintf "token endpoint HTTP %d" code)))
                  else
                    match Oauth.parse_token_response ~now body_str with
                    | None ->
                        return
                          (Error
                             (Core_z.Error.Auth_error
                                "no access_token in response"))
                    | Some (token, expiry) ->
                        client.token_cache <- Some (token, expiry);
                        return (Ok token)))))

(** Open a stream with built-in client-credentials OAuth and run [f] with it
    (bracket form). Mints a table-scoped token (HTTPS to /oidc/v1/token), builds
    the bearer + table-name headers, then opens the stream over TLS inside the
    owning scope — the ergonomic Async counterpart of the Lwt
    {!Zerobus.create_stream} and the Eio {!Zerobus_eio.with_stream_oauth}.

    TLS is always on for the gRPC stream (the live path). The stream + scope are
    torn down on exit, exactly as {!with_stream}. *)
let with_stream_oauth client (table_props : table_properties) ~client_id
    ~client_secret ?(options = default_stream_options)
    (f : stream -> 'a Deferred.t) : ('a, Core_z.Error.t) result Deferred.t =
  let headers_provider () =
    match%map
      mint_token ~client ~table:table_props.Core_z.Options.table_name ~client_id
        ~client_secret
    with
    | Error e -> Error e
    | Ok token ->
        Ok
          [
            ("authorization", "Bearer " ^ token);
            ( "x-databricks-zerobus-table-name",
              table_props.Core_z.Options.table_name );
          ]
  in
  with_stream ~tls:true client table_props ~headers_provider ~options f

(** {1 Low-level access (tests / advanced)} *)

module Io_async_for_test = Io_async
module Driver = Io_async.Stream

module Tls_connect = Tls_connect
(** The TLS connect backend (a dune [select] on tls-async — real or honest-error
    stub). Exposed so the in-tree live-TLS test can pin a self-signed cert via
    [Tls_connect.pinned_cert_fp_sha256_b64] (the Async analogue of the Eio
    [~authenticator] override). Not part of the ergonomic surface. *)

module Oauth = Oauth
(** The built-in OAuth HTTP backend (a dune [select] on tls-async — the real
    pure-ocaml-tls token POST, or an honest-error stub). Exposed so the sibling
    REST/OTLP Async packages ([zerobus-rest-async], [zerobus-otlp-async]) can
    reuse the {b same} pure-TLS token mint instead of pulling cohttp-async's
    OpenSSL conduit. Not part of the ergonomic surface. *)
