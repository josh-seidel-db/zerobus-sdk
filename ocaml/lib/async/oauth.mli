(** Built-in client-credentials OAuth HTTP backend for the Async runtime — a
    dune [select] on tls-async (see lib/async/dune). The real backend
    (oauth.real.ml) hand-rolls HTTP/1.1 over [Tls_async.connect] (the same
    pure-OCaml TLS stack the gRPC transport uses, NOT cohttp-async/OpenSSL); the
    stub (oauth.dummy.ml) returns an honest error when tls-async is absent. This
    interface is shared by both variants, so it also guarantees they stay in
    sync. Reused by the sibling REST/OTLP Async packages via
    {!Zerobus_async.Oauth}. *)

open! Core
open! Async

val available : bool
(** [true] iff the real (tls-async) backend was selected at build time. *)

val https_post :
  url:string ->
  headers:(string * string) list ->
  body:string ->
  (int * string, exn) result Deferred.t
(** POST to [url] over pure-ocaml-tls (system trust store; https) or plain TCP
    (http, for the cleartext test mock), with the given headers and body.
    [Connection: close] + [Content-Length] are added automatically. Returns
    [(http_status, response_body)] or the request exception. *)

val post_token :
  token_url:string ->
  client_id:string ->
  client_secret:string ->
  body:string ->
  (int * string, exn) result Deferred.t
(** POST the client-credentials grant to [token_url] ([body] is the URL-encoded
    request body; [client_id]/[client_secret] go in the HTTP Basic header).
    Returns [(http_status, response_body)] or the request exception. *)

val parse_token_response : now:float -> string -> (string * float) option
(** Parse the token endpoint's JSON reply into [(access_token, expiry_epoch_s)]
    ([now] is the base for the absolute expiry); [None] if malformed. *)
