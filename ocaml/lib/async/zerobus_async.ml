(** The public Async API for the Zerobus Ingest SDK (DESIGN.md §5.2, §5.3).

    The Async counterpart of the Lwt {!Zerobus} module. It bridges
    {!Zerobus_core.Make(Zerobus_io_async)} to an ergonomic entry point: [create]
    constructs a client from workspace metadata, and [create_stream_with_headers]
    opens a table-scoped stream given caller-supplied gRPC headers.

    {b Scope of this façade.} Unlike the Lwt reference, this module does NOT offer
    a built-in client-credentials [create_stream]: the Async switch has no HTTP
    client (cohttp-async) or JSON library wired, and the Async TLS transport is
    deferred (Lwt is the live-verified TLS+ALPN reference). Auth is therefore
    supplied by the caller through [headers_provider] — which, with [tls:false],
    also drives the full façade→driver path against a cleartext mock. Live TLS +
    built-in OAuth on Async are tracked follow-ups.

    The cardinal rule of ingestion: queue records in a loop, then call [flush]
    once. Per-record waiting defeats pipelining. *)

open! Core
open! Async
module Core_z = Zerobus_core
module Io_async = Zerobus_io_async

(** A Zerobus client holds workspace metadata and the derived gRPC endpoint. *)
type t = {
  workspace_url : string; [@warning "-69"]
  workspace_id : string; [@warning "-69"]
  endpoint_host : string;
  endpoint_port : int;
  application_name : string option; [@warning "-69"]
}

(** A live stream bound to a long-lived scope (§5.2, §5.3). *)
type stream = {
  stream : Io_async.stream_handle;
  scope : Io_async.Scope.t;
  table_name : string; [@warning "-69"]
}

(** An offset handle the caller can wait on (DESIGN.md §5.3(a)). *)
type offset = Core_z.Options.offset

type table_properties = Core_z.Options.table_properties
type stream_options = Core_z.Options.stream_options

let default_stream_options = Core_z.Options.default_stream_options

(* Silence unused-field warnings (fields are set for diagnostics / future use). *)
let _ = fun (x : t) -> ignore (x.workspace_url, x.workspace_id, x.application_name)
let _ = fun (x : stream) -> ignore x.table_name

(** {1 Ingestion operations (re-exported over the driver)} *)

let ingest stream buf = Io_async.Stream.ingest stream.stream buf
let ingest_records stream bufs = Io_async.Stream.ingest_records stream.stream bufs
let wait_for_offset stream off = Io_async.Stream.wait_for_offset stream.stream off
let flush stream = Io_async.Stream.flush stream.stream

(** Close the stream and tear down its scope (best-effort join of the ack-reader). *)
let close stream =
  let%bind result = Io_async.Stream.close stream.stream in
  (* Signal the scope's daemons to stop and best-effort join without blocking on
     an uncancellable one (same discipline as Io_async.Scope.with_scope). *)
  Ivar.fill_if_empty stream.scope.Io_async.Scope.stop ();
  let%map () =
    Deferred.any
      [ Deferred.all_unit stream.scope.Io_async.Scope.daemons;
        Async.Clock.after (Time_float.Span.of_ms 0.) ]
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
               }))

(** Bracket form: open a stream with caller-supplied gRPC headers (custom auth),
    run [f] with it inside the owning scope, then tear the stream + scope down.

    The [headers_provider] returns the header list (e.g. an [authorization]
    bearer token + the table-name header). [:authority] is added from the
    endpoint if the provider did not supply it. [tls] defaults to [true]; pass
    [false] for a cleartext-h2c mock backend.

    This is the entry point for Async: the bracket owns the {!Io_async.Scope} for
    the whole body, so the ack-reader daemon is forked into a live scope and torn
    down deterministically on exit — the reliable shape given Async's lack of hard
    fiber cancellation. (Detached create/return, as the Lwt façade offers, relies
    on the caller keeping the scheduler pumping; not exposed here.) *)
let with_stream ?(tls = true) client (table_props : table_properties)
    ~(headers_provider :
       unit -> ((string * string) list, Core_z.Error.t) result Deferred.t)
    ?(options = default_stream_options)
    (f : stream -> 'a Deferred.t) : ('a, Core_z.Error.t) result Deferred.t =
  Io_async.Scope.with_scope (fun scope ->
      let%bind headers_result = headers_provider () in
      match headers_result with
      | Error e -> return (Error e)
      | Ok headers ->
          let headers =
            if List.Assoc.mem headers ":authority" ~equal:String.equal then headers
            else
              ( ":authority",
                Printf.sprintf "%s:%d" client.endpoint_host client.endpoint_port )
              :: headers
          in
          let%bind stream_result =
            Io_async.Stream.open_stream ~host:client.endpoint_host
              ~port:client.endpoint_port ~tls ~headers ~options
              ~table:table_props scope
          in
          (match stream_result with
           | Error e -> return (Error e)
           | Ok s ->
               let stream = { stream = s; scope; table_name = table_props.table_name } in
               let%bind result = f stream in
               let%map _ = Io_async.Stream.close stream.stream in
               Ok result))

(** {1 Low-level access (tests / advanced)} *)

module Io_async_for_test = Io_async
module Driver = Io_async.Stream
