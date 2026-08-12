(** Fallback TLS connect for the Async transport — used when [tls-async] is NOT
    installed (dune [select] picks this over [tls_connect.real.ml]). Async live
    TLS then returns an honest error; cleartext h2c (mocks) is unaffected. The Lwt
    and Eio runtimes are the live-TLS references. To enable real Async TLS, install
    [tls-async] on the switch (see doc/arch/tls_async_status.md) — dune then selects
    [tls_connect.real.ml] automatically. *)

open! Core
open! Async

let available = false

(* Signature parity with [tls_connect.real.ml] (see its doc). No TLS here, so the
   pin is inert — kept so both [select] alternatives expose the same interface. *)
let pinned_cert_fp_sha256_b64 : string option ref = ref None

let connect ~host:_ ~port:_ :
    (Reader.t * Writer.t * (unit -> unit Deferred.t), string) Deferred.Result.t =
  return
    (Error
       "Async TLS not available: install tls-async on this switch to enable it \
        (dune selects the real backend); use tls:false for a cleartext mock, or \
        the Lwt/Eio runtime for live TLS (see doc/arch/tls_async_status.md)")
