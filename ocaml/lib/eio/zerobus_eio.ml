(** The public Eio API for the Zerobus Ingest SDK (DESIGN.md §5.2, §6.2).

    The direct-style counterpart of the Lwt {!Zerobus} module. Because Eio is
    scope-based — a background fiber must live inside a {!Eio.Switch} — this
    façade is bracket-shaped: {!with_stream} owns the switch for the stream's
    lifetime and runs the caller's body with a live stream, tearing everything
    down on exit. This is the §6.2 [env]/[~sw]-threading façade: it installs the
    caller's [env]/[~sw] into the transport (via {!Zerobus_io_eio.Ctx}) so the
    runtime-generic [connect] can reach them.

    {b Auth.} {!with_stream_oauth} does a full client-credentials OAuth mint
    (HTTPS to /oidc/v1/token via cohttp-eio) and opens a live TLS stream — the
    ergonomic path. {!with_stream} takes caller-supplied [headers] instead
    (custom auth, or [~tls:false] against a cleartext mock).

    The cardinal rule of ingestion: queue records in a loop, then [flush] once.
*)

module Core_z = Zerobus_core
module Io_eio = Zerobus_io_eio

type t = {
  workspace_url : string;
  workspace_id : string;
  endpoint_host : string;
  endpoint_port : int;
  application_name : string option;
}
(** A Zerobus client holds workspace metadata and the derived gRPC endpoint. *)

(** The concrete driver behind a stream: the default [EphemeralStream] driver
    (JSON/Proto) or the Arrow/Flight [DoPut] driver, chosen by [record_type] at
    open time (mirrors the Lwt façade). *)
type stream_impl =
  | Ephemeral of Io_eio.Stream.stream
  | Flight of Io_eio.Stream_flight.stream

type stream = { impl : stream_impl; table_name : string }
(** A live stream, valid only within the {!with_stream} body that owns it. *)

type offset = Core_z.Options.offset
type table_properties = Core_z.Options.table_properties
type stream_options = Core_z.Options.stream_options

let default_stream_options = Core_z.Options.default_stream_options

let _ =
 fun (x : t) -> ignore (x.workspace_url, x.workspace_id, x.application_name)

let _ = fun (x : stream) -> ignore x.table_name

(** {1 Ingestion operations (direct-style — no monad)} *)

let ingest stream buf =
  match stream.impl with
  | Ephemeral s -> Io_eio.Stream.ingest s buf
  | Flight s -> Io_eio.Stream_flight.ingest s buf

let ingest_records stream bufs =
  match stream.impl with
  | Ephemeral s -> Io_eio.Stream.ingest_records s bufs
  | Flight s -> Io_eio.Stream_flight.ingest_records s bufs

let wait_for_offset stream off =
  match stream.impl with
  | Ephemeral s -> Io_eio.Stream.wait_for_offset s off
  | Flight s -> Io_eio.Stream_flight.wait_for_offset s off

let flush stream =
  match stream.impl with
  | Ephemeral s -> Io_eio.Stream.flush s
  | Flight s -> Io_eio.Stream_flight.flush s

let close stream =
  match stream.impl with
  | Ephemeral s -> Io_eio.Stream.close s
  | Flight s -> Io_eio.Stream_flight.close s

(* Open the stream on the driver selected by [record_type]: Arrow → Flight DoPut,
   else the default EphemeralStream driver. Both use the same Eio IO + scope. *)
let open_impl ~host ~port ~tls ~headers ~options ~table scope :
    (stream_impl, Core_z.Error.t) result =
  match options.Core_z.Options.record_type with
  | Core_z.Options.Arrow ->
      Result.map
        (fun s -> Flight s)
        (Io_eio.Stream_flight.open_stream ~host ~port ~tls ~headers ~options
           ~table scope)
  | Core_z.Options.Json | Core_z.Options.Proto ->
      Result.map
        (fun s -> Ephemeral s)
        (Io_eio.Stream.open_stream ~host ~port ~tls ~headers ~options ~table
           scope)

(** Construct a client from workspace URL and optional gRPC endpoint. Derives
    the endpoint from [workspace_url] when [endpoint] is empty/"default". *)
let create ?(application_name : string option) ~endpoint ~workspace_url () :
    (t, Core_z.Error.t) result =
  match Core_z.Config.workspace_id_of_url workspace_url with
  | Error e -> Error e
  | Ok wsid -> (
      match Core_z.Config.endpoint_of_workspace ~endpoint ~workspace_url with
      | Error e -> Error e
      | Ok (host, port) ->
          Ok
            {
              workspace_url;
              workspace_id = wsid;
              endpoint_host = host;
              endpoint_port = port;
              application_name;
            })

(** Open a stream with caller-supplied gRPC headers and run [f] with it, tearing
    the stream + its scope down on exit (bracket form — the Eio-idiomatic shape,
    since the ack-reader fiber lives in the scope's switch).

    [env] and [sw] come from the caller's [Eio_main.run] / [Switch.run]; they
    are installed into the transport context for the duration of [f]. [tls]
    defaults to [true]; pass [false] for a cleartext-h2c mock. [:authority] is
    derived from the endpoint if [headers] did not include it. [authenticator]
    overrides the system trust store for the TLS handshake (default: [Ca_certs])
    — mainly for tests that pin a self-signed server cert.

    Returns the result of [f], or [Error] if the stream could not be opened. *)
let with_stream ~env ~sw ?(tls = true) ?authenticator client
    (table_props : table_properties) ~(headers : (string * string) list)
    ?(options = default_stream_options) (f : stream -> 'a) :
    ('a, Core_z.Error.t) result =
  let net = Eio.Stdenv.net env in
  Io_eio.Ctx.with_env ?authenticator ~net ~sw (fun () ->
      let headers =
        if List.exists (fun (k, _) -> k = ":authority") headers then headers
        else
          ( ":authority",
            Printf.sprintf "%s:%d" client.endpoint_host client.endpoint_port )
          :: headers
      in
      (* The stream's ack-reader is forked into [scope]; [with_scope] runs a
         Switch that is cancelled when this body returns. *)
      Io_eio.Scope.with_scope (fun scope ->
          match
            open_impl ~host:client.endpoint_host ~port:client.endpoint_port ~tls
              ~headers ~options ~table:table_props scope
          with
          | Error e -> Error e
          | Ok impl ->
              let stream = { impl; table_name = table_props.table_name } in
              let result = f stream in
              let _ = close stream in
              Ok result))

(** {1 Built-in client-credentials OAuth (§12.2)} *)

(* Mint a table-scoped OAuth token via the client-credentials grant: an HTTPS POST
   to <workspace_url>/oidc/v1/token with the Zerobus scope/resource and the table's
   UC authorization_details (id/secret go in the HTTP Basic header, not the body).
   Uses cohttp-eio over a real TLS connection (system trust store). Direct-style. *)
let mint_token ~env ~sw ~(client : t) ~table ~client_id ~client_secret :
    (string, Core_z.Error.t) result =
  match
    Core_z.Config.oauth_token_request_body ~workspace_id:client.workspace_id
      ~table
  with
  | Error e -> Error e
  | Ok body -> (
      try
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
        let httpc =
          Cohttp_eio.Client.make ~https:(Some https) (Eio.Stdenv.net env)
        in
        let basic = Base64.encode_string (client_id ^ ":" ^ client_secret) in
        let headers =
          Http.Header.of_list
            [
              ("Content-Type", "application/x-www-form-urlencoded");
              ("Authorization", "Basic " ^ basic);
            ]
        in
        let uri = Uri.of_string (client.workspace_url ^ "/oidc/v1/token") in
        let resp, rbody =
          Cohttp_eio.Client.post ~sw httpc ~headers
            ~body:(Cohttp_eio.Body.of_string body)
            uri
        in
        let body_str =
          Eio.Buf_read.(parse_exn take_all) rbody ~max_size:max_int
        in
        let code = Http.Status.to_int resp.Http.Response.status in
        if code < 200 || code >= 300 then
          Error
            (Core_z.Error.Auth_error
               (Printf.sprintf "token endpoint HTTP %d" code))
        else
          match Yojson.Safe.from_string body_str with
          | `Assoc l -> (
              match List.assoc_opt "access_token" l with
              | Some (`String t) -> Ok t
              | _ ->
                  Error (Core_z.Error.Auth_error "no access_token in response"))
          | _ -> Error (Core_z.Error.Auth_error "malformed token response")
      with exn ->
        Error
          (Core_z.Error.Transport_error
             (Printf.sprintf "token request failed: %s" (Printexc.to_string exn)))
      )

(** Open a stream with built-in client-credentials OAuth and run [f] with it
    (bracket form). Mints a table-scoped token (HTTPS to /oidc/v1/token), builds
    the bearer + table-name headers, then opens the stream — the ergonomic
    counterpart of the Lwt {!Zerobus.create_stream}. [env]/[sw] come from the
    caller's [Eio_main.run] / [Switch.run]. TLS is always on for the gRPC stream
    (live path); tokens are not cached across calls (open once per stream). *)
let with_stream_oauth ~env ~sw client (table_props : table_properties)
    ~client_id ~client_secret ?(options = default_stream_options)
    (f : stream -> 'a) : ('a, Core_z.Error.t) result =
  match
    mint_token ~env ~sw ~client ~table:table_props.Core_z.Options.table_name
      ~client_id ~client_secret
  with
  | Error e -> Error e
  | Ok token ->
      let headers =
        [
          ("authorization", "Bearer " ^ token);
          ( "x-databricks-zerobus-table-name",
            table_props.Core_z.Options.table_name );
        ]
      in
      with_stream ~env ~sw ~tls:true client table_props ~headers ~options f

(** {1 Low-level access (tests / advanced)} *)

module Io_eio_for_test = Io_eio
module Driver = Io_eio.Stream
