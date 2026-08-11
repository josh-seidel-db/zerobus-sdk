(** Abstract bidirectional gRPC transport.

    The Zerobus hot path is a single client-initiated bidirectional streaming RPC
    (`EphemeralStream`, and Arrow's `DoPut`): the client pushes framed messages
    while concurrently draining acknowledgements. The three runtime spikes proved
    this shape on `grpc-lwt` (push-fn + [Lwt_stream]), `grpc-async` ([Pipe]), and
    `grpc-eio` ([Seq]). Despite the surface differences, all three reduce to the
    same three operations, which this signature captures:

    - {b send} one already-framed message,
    - {b close_send} to half-close the request side (no more sends),
    - {b recv} to pull the next response message, or [None] once the server side
      is closed and drained.

    Messages crossing this boundary are opaque [string]s — the caller frames the
    protobuf (the generated wire types in {!Zerobus_proto}); the transport only
    moves bytes. This is what {!Io.IO.H2_client} is required to provide, and what
    the streaming driver ({!Zerobus_core.Make}) runs its send-fiber / ack-daemon
    pair over. Keeping it a bare signature means [zerobus-core] links no runtime. *)

module type S = sig
  (** The runtime's effect type — [Lwt.t], [Async.Deferred.t], or [fun x -> x]
      for Eio. Shared with the enclosing {!Io.IO} via [with type 'a io = 'a t]. *)
  type 'a io

  (** A live, connected transport (one TLS+ALPN-h2 connection). *)
  type connection

  (** One open bidirectional RPC call over a {!connection}. *)
  type call

  (** Establish a connection to [host]:[port]. When [tls] is [true] (the default)
      the runtime packages negotiate TLS 1.3 + ALPN [h2] (proven in [spike-live/])
      and fail fast if ALPN ≠ [h2]; when [false] they use cleartext h2c (the
      loopback mock backends). [tls] is an explicit flag rather than a port
      heuristic so a bearer token is never sent over cleartext by accident.
      [headers] are connection-level metadata (e.g. the table-name header). *)
  val connect :
    host:string ->
    port:int ->
    ?tls:bool ->
    ?headers:(string * string) list ->
    unit ->
    (connection, Error.t) result io

  (** Open the bidirectional RPC named [service]/[rpc] on [connection]. The
      returned {!call} is ready to {!send}/{!recv}. Per-call [headers] (e.g. the
      re-stamped table-name header on a reconnect) are merged over the
      connection's. *)
  val start_bidi :
    connection ->
    service:string ->
    rpc:string ->
    ?headers:(string * string) list ->
    unit ->
    (call, Error.t) result io

  (** Queue one framed message on the request side. Returns once buffered; the
      wire flush happens on the runtime's own schedule (see the grpc-eio flush
      finding, §6.5 — the Eio instantiation must ensure a flush actually occurs). *)
  val send : call -> string -> (unit, Error.t) result io

  (** Half-close the request side: the server may still send acks after this. *)
  val close_send : call -> (unit, Error.t) result io

  (** Pull the next response message. [Ok None] means the server side is closed
      and fully drained — the normal end of a healthy stream. A broken stream
      surfaces as [Error (Transport_error _)] or, when the server ends with a
      non-OK status, is reported via {!status} after [recv] returns [Ok None]. *)
  val recv : call -> (string option, Error.t) result io

  (** The final gRPC status of a finished call — available once {!recv} has
      returned [Ok None] (or the call errored). The recovery driver uses this to
      distinguish a clean [OK] end from a retryable break (e.g. UNAVAILABLE). *)
  val status : call -> (unit, Error.t) result io

  (** Tear down the connection and any open call; idempotent. *)
  val shutdown : connection -> unit io
end
