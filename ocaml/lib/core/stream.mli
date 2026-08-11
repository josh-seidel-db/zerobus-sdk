(** The runtime-agnostic streaming driver (DESIGN.md §5, §6.4), functorized over
    {!Io.IO}. The per-runtime packages apply [Make] to their {!Io.IO} and wrap
    {!Make.open_stream} in a scope to expose the public [create_stream].

    The driver's offset/ack/watermark/recovery control plane is protocol-agnostic;
    the on-the-wire specifics live behind {!PROTOCOL}. [Make] uses the default
    {!Ephemeral} (Zerobus [EphemeralStream], JSON/Proto). An Arrow/Flight [DoPut]
    driver is obtained by applying {!Make_with_protocol} to a Flight [PROTOCOL]
    defined OUTSIDE [lib/core] (so core needs no Flight proto / libarrow). *)

(** What a decoded response frame means to the driver's control plane. *)
type ack =
  | Watermark of int64  (** durability watermark: all offsets <= n are durable *)
  | Created  (** stream/schema accepted; not an ack, keep reading *)
  | Closed  (** server signalled end-of-stream *)

(** The on-the-wire protocol the driver runs over. *)
module type PROTOCOL = sig
  val service : string
  val rpc : string

  (** Build the framed create/schema request sent first (and re-sent on
      reconnect), from the table + options. *)
  val create_frame :
    table:Options.table_properties -> options:Options.stream_options -> string

  (** Build the framed record request for one record at [offset]. *)
  val record_frame :
    record_type:Options.record_type -> offset:int64 -> bytes -> string

  (** Decode a response frame into an {!ack}, or an error on a malformed frame. *)
  val decode_ack : string -> (ack, Error.t) result
end

(** The default Zerobus [EphemeralStream] protocol (JSON/Proto). *)
module Ephemeral : PROTOCOL

module Make_with_protocol (Io : Io.IO) (_ : PROTOCOL) : sig
  type offset = Options.offset

  (** An open stream to one table: owns the bidi RPC, offset/ack state, the
      un-acked replay buffer, and (via its scope) the ack-reader daemon. *)
  type stream

  (** Open a stream inside the given scope: connect (TLS+ALPN h2 unless [tls] is
      false), start the [P.service]/[P.rpc] bidi RPC, send the create/schema
      frame, and fork the ack-reader. [headers] carry the bearer token +
      table-name metadata. The scope must outlive the stream — per-runtime
      [create_stream] supplies it via [Scope.with_scope]. *)
  val open_stream :
    host:string ->
    port:int ->
    tls:bool ->
    headers:(string * string) list ->
    options:Options.stream_options ->
    table:Options.table_properties ->
    Io.Scope.t ->
    (stream, Error.t) result Io.t

  (** Queue one record and return its offset immediately. Does {b not} wait for
      an ack — that is the cardinal rule; wait later with {!wait_for_offset} or
      {!flush}. *)
  val ingest : stream -> bytes -> (offset, Error.t) result Io.t

  (** Queue a batch; returns the offset of the last record. *)
  val ingest_records : stream -> bytes list -> (offset, Error.t) result Io.t

  (** Wait until [offset] is durably acked (the watermark is monotonic, so this
      also implies all prior offsets). Returns the stream's fatal error if one
      occurred. *)
  val wait_for_offset : stream -> offset -> (unit, Error.t) result Io.t

  (** Wait once for every record issued so far to be durable. *)
  val flush : stream -> (unit, Error.t) result Io.t

  (** Half-close the send side and flush pending acks; idempotent. The stream's
      scope (and its ack-reader) is torn down when the enclosing [with_scope]
      returns. *)
  val close : stream -> (unit, Error.t) result Io.t
end

(** The default driver: {!Make_with_protocol} over {!Ephemeral}. This is the
    single-arg API the per-runtime packages apply ([Zerobus_core.Make (Io)]). *)
module Make (Io : Io.IO) : module type of Make_with_protocol (Io) (Ephemeral)
