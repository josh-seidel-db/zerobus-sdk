(** The Async live-TLS connect backend — a dune [select] on tls-async (see
    lib/async/dune). The real backend (tls_connect.real.ml) does a
    [Tls_async.connect] with ALPN h2 + system trust store (or a pinned
    self-signed cert for tests); the stub (tls_connect.dummy.ml) returns an
    honest error when tls-async is absent. This interface is shared by both
    variants, so it also guarantees they stay in sync. *)

open! Core
open! Async

val available : bool
(** [true] iff the real (tls-async) backend was selected at build time. *)

val pinned_cert_fp_sha256_b64 : string option ref
(** When [Some "<sha256-b64>"], authenticate the TLS peer by pinning that exact
    certificate fingerprint instead of the system trust store — for testing
    against a self-signed h2 mock. Prod leaves it [None]. Inert in the
    honest-error stub backend. *)

val connect :
  host:string ->
  port:int ->
  (Reader.t * Writer.t * (unit -> unit Deferred.t), string) Deferred.Result.t
(** Establish a TLS 1.3 + ALPN-h2 connection to [host]:[port], returning an
    Async [Reader]/[Writer] duplex plus a shutdown thunk (for {!H2_pump}). In
    the stub backend, returns an honest [Error] explaining that tls-async is not
    installed. *)
