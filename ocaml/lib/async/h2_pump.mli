(** Drive the runtime-agnostic {!H2.Client_connection} core over an Async
    [Reader.t] / [Writer.t] duplex — the same job gluten does over a socket, but
    without gluten-async. This lets the Async transport run h2 over BOTH a plain
    TCP duplex (cleartext h2c, for mocks) and a [tls-async] duplex (live TLS +
    ALPN h2) — see {!Tls_connect}. Proven live against the real Zerobus endpoint
    (spike-async-tls/). *)

open! Core
open! Async

val start : H2.Client_connection.t -> reader:Reader.t -> writer:Writer.t -> unit
(** Start the read and write pumps for a fresh [H2.Client_connection] over the
    given duplex. The write pump runs inline (it immediately yields until there
    is something to send); the read pump is detached and runs for the
    connection's life, feeding each chunk into h2 and signalling EOF on close.
*)
