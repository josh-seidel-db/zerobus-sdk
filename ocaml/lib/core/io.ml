(** The minimal effect surface the SDK is functorized over (DESIGN.md §6.2).

    The whole SDK is [Make(Io : IO)]; three thin packages instantiate it for Lwt,
    Eio, and Async. The exact shape is driven by two correctness findings (design
    review #1/#2), both stemming from the core being *concurrent bidirectional
    streaming* rather than request/response:

    - {b Concurrency combinators take thunks, not values.} In a direct-style
      runtime (Eio) ['a t] is a bare ['a], so an already-evaluated argument has
      already run — sequentially. [both]/[fork_daemon] therefore take a suspended
      [unit -> _] so the {e runtime}, not the call site, decides when each side
      runs. (Harmless on Lwt/Async, where ['a t] is already suspended.)

    - {b Background fibers need a scope.} Eio forbids unscoped background fibers
      ([Fiber.fork] needs a [Switch.t]). So the core owns a {!IO.Scope}, and
      [fork_daemon] forks the ack-reader into it; [close] cancels the scope. On
      Lwt/Async the scope is a lightweight cancellation token + fiber registry. *)

module type IO = sig
  type 'a t
  (** The runtime's effect: [Lwt.t], [Async.Deferred.t], or ['a] itself (Eio). *)

  (** Alias for the effect type, so nested signatures (e.g. [Scope]) can refer to
      it without [t] being shadowed by a local [type t]. *)
  type 'a io = 'a t

  val return : 'a -> 'a t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val map : 'a t -> ('a -> 'b) -> 'b t

  (** A structured-concurrency scope: owns background fibers and their
      cancellation. Eio: wraps [Switch.t]. Lwt/Async: a cancellation token and a
      registry of forked fibers, joined/cancelled on scope exit. *)
  module Scope : sig
    type t

    (** Run [f] with a fresh scope. When [f] returns or raises, every daemon
        forked into the scope (via {!fork_daemon}) is cancelled and joined before
        [with_scope] returns. This is the stream's lifetime boundary:
        [create_stream] opens it, [close] ends it. *)
    val with_scope : (t -> 'a io) -> 'a io
  end

  (** Run two computations concurrently and pair their results. Thunked so the
      runtime sequences them (see the module note) — the send-loop and ack-loop
      must actually overlap. *)
  val both : (unit -> 'a t) -> (unit -> 'b t) -> ('a * 'b) t

  (** Run two computations concurrently and return the result of whichever finishes
      FIRST, abandoning (Lwt/Eio: cancelling) the loser. Both branches yield the
      same type so the caller can race a real computation against a timer branch
      (e.g. [first connect (fun () -> sleep t; Timeout)]) — see the [timeout] use in
      the stream driver. Cancellation semantics follow the runtime: Lwt [Lwt.pick]
      and Eio [Fiber.first] hard-cancel the loser; Async has no hard fiber
      cancellation, so the losing [Deferred] is simply detached (matching the
      discipline in {!Scope}) — callers must therefore keep loser side effects
      idempotent / harmless when abandoned. *)
  val first : (unit -> 'a t) -> (unit -> 'a t) -> 'a t

  (** Fork a long-lived background fiber {e into a scope} — never unscoped, so
      Eio's switch requirement is met. Used for the ack-reader daemon. *)
  val fork_daemon : Scope.t -> (unit -> unit t) -> unit

  module Mutex : sig
    type t

    val create : unit -> t
    val with_lock : t -> (unit -> 'a io) -> 'a io
  end

  (** A bounded, blocking mailbox between the send side and the ack reader.
      (Named [Mailbox] rather than [Stream] to avoid colliding with the Zerobus
      [stream] type.) *)
  module Mailbox : sig
    type 'a t

    val create : capacity:int -> 'a t

    (** Put a value; blocks (in the effect) when the mailbox is full. *)
    val put : 'a t -> 'a -> unit io

    (** Take the next value; [None] once the mailbox is closed and drained. *)
    val take : 'a t -> 'a option io

    val close : 'a t -> unit
  end

  (** The HTTP/2 client capable of a bidi gRPC stream over TLS+ALPN [h2]. *)
  module H2_client : Grpc_transport.S with type 'a io = 'a t

  val sleep : float -> unit t  (** seconds; recovery backoff *)
end
