# Async transport spike — `zerobus-async` (validated v1 scope)

Proves the same three transport properties as `spike/` (Lwt) and `spike-eio/`
(Eio), on **`grpc-async` + `h2-async`**, so the `zerobus-async` package is
**required, validated v1 scope** — not design-only. This is the third headline
runtime alongside `zerobus` (Lwt) and `zerobus-eio` (Eio).

**RESULT: PASS (compiled and run).** OCaml 4.14.1, `grpc-async 0.2.0`,
`h2-async 0.12.0`, `async v0.16.0`, `core v0.16.2`. 200 sent, 200 acked, first
ack at send **#11–14 of 200** (interleaved), offsets 0–199 contiguous. Evidence:
[`EVIDENCE_ASYNC.txt`](EVIDENCE_ASYNC.txt); regenerate with `dune runtest --force`.

## What it proves

The driver spawns the mock server as a subprocess, then runs the client with two
concurrent Async jobs on `Pipe`s — a sender pushing 200 `IngestRequest`s and an
ack reader draining `IngestResponse`s — and asserts:

1. **all 200 records acked**,
2. **acks interleave with sends** (`sends_done_at_first_ack` ≪ 200 — genuine
   concurrency on the Async scheduler, not send-all-then-receive),
3. **offsets strictly increasing** (per-stream ordering preserved).

## Topology — separate process, on purpose (leveraging the grpc-eio finding)

Like the Eio spike, the server runs as a **separate OS process**
(`ingest_server_async.exe`, OS-assigned port announced as `READY <port>` on
stdout). This directly applies the finding from `spike-eio/` (see its README's
"Eio finding"): grpc's client send path and server both live on one cooperative
scheduler, and running both in a single process risks a send/flush interleave
hazard. Separate processes sidestep it and match the real Zerobus topology
(client and server are always different processes). The Async spike was built
this way from the start rather than rediscovering the deadlock.

## Async-specific API notes (what differed from the Lwt spike)

Confirmed against the installed `.mli` and reflected in the code:

- Client bidi handler is `~handler:(string Pipe.Writer.t -> string Pipe.Reader.t -> 'a Deferred.t)`;
  half-close = `Pipe.close writer`.
- Server bidi handler is `string Pipe.Reader.t -> string Pipe.Writer.t -> Grpc.Status.t Deferred.t`,
  registered as a `Grpc_async.Server.Rpc.Bidirectional_streaming` variant.
- `Grpc_async.Client.call` returns `(_ * Grpc.Status.t, H2.Status.t) result Deferred.t`.
- `h2-async` uses Async `Socket.t` (not fds): `Tcp.connect_sock` for the client,
  `Tcp.Server.create_sock` for the server; connect/shutdown are `Deferred.t`.
- `let%bind`/`let%map` require the `ppx_let` preprocessor (from `ppx_jane`).
- The client must `H2_async.Client.shutdown` after the call (same lesson as the
  Eio spike, where the Switch otherwise never resolves).

## Running it

```bash
eval $(opam env --switch=fl414)   # switch with grpc-async/h2-async/async
cd ocaml/spike-async
dune runtest --force
```

## Scope

Proves **bidi transport mechanics** for the Async runtime. Same out-of-scope
items as the other transport spikes: no TLS/ALPN (that is proven natively in
`spike-live/`), no OAuth, no recovery, no real Zerobus service. The `IO`-functor
core (DESIGN.md §6.2) sits behind these primitives, so this validates that the
functor's third instantiation (`Zerobus_io_async`) rests on a working transport.
