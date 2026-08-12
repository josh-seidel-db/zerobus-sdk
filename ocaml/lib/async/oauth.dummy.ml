(** Fallback OAuth backend for the Async runtime — used when [cohttp-async] is NOT
    installed (dune [select] picks this over [oauth.real.ml]). Built-in
    client-credentials OAuth then returns an honest error; the caller can still
    supply a bearer token itself via {!Zerobus_async.with_stream}'s
    [headers_provider]. Signature parity with [oauth.real.ml]. To enable built-in
    OAuth, install [cohttp-async] on the switch — dune then selects the real
    backend automatically. See doc/arch/tls_async_status.md. *)

open! Core
open! Async

let available = false

let unavailable =
  Failure
    "Async built-in OAuth/HTTP not available: install tls-async (+ ca-certs, \
     domain-name, tls) on this switch to enable it (dune selects the real \
     backend), or supply a bearer token via with_stream's headers_provider"

let https_post ~url:_ ~headers:_ ~body:_ : (int * string, exn) result Deferred.t =
  return (Error unavailable)

let post_token ~token_url:_ ~client_id:_ ~client_secret:_ ~body:_ :
    (int * string, exn) result Deferred.t =
  return (Error unavailable)

let parse_token_response ~now:_ (_ : string) : (string * float) option = None
