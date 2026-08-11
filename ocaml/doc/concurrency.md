# Concurrency guarantees

Every SDK in this monorepo states its thread-safety contract (Go: safe for
concurrent goroutines; Java: not thread-safe, external sync required; Python:
async safe, sync single-threaded per instance). This is the OCaml SDK's contract.
It holds identically across all three runtime packages — `zerobus` (Lwt),
`zerobus-eio` (Eio), and `zerobus-async` (Async) — because they are all thin
instantiations of the same runtime-agnostic core (`zerobus-core`).

## The client handle `t` is safe to share

A client (`Zerobus.t` / `Zerobus_eio.t` / `Zerobus_async.t`) is immutable apart
from a lock-guarded OAuth token cache. Creating streams from one `t`
concurrently is fine, and the token cache serializes token mints/refreshes
internally (an `Lwt_mutex` on Lwt; the same discipline on the other runtimes).
The REST (`Zerobus_rest.t`) and OTLP (`Zerobus_otlp.t`) clients are likewise
shareable — each guards a per-table token cache the same way.

## A `stream` is single-writer

`ingest` / `ingest_records` / `flush` / `wait_for_offset` on one stream must come
from a **single logical writer** — one Lwt promise chain, one Eio fiber, or one
Async job. This mirrors Zerobus's per-stream ordering guarantee: offsets are
assigned in call order, so interleaving independent writers on one stream would
make ordering meaningless.

Internally the send side and the ack-reader **are** concurrent — that is the
whole design (queue records while acks stream back) — but those are the SDK's own
fibers, coordinated through a bounded mailbox. They are not the caller's
concurrency to manage. The SDK deliberately does **not** put a lock on the hot
`ingest` path: that would tax the common single-writer case for a guarantee
callers rarely want.

If you genuinely need concurrent writers to one logical table, either serialize
them behind your own mutex, or — better — shard across multiple streams (below).

## Fan-out = more streams, not more writers per stream

To parallelize ingestion, open multiple streams and write to each from its own
fiber/promise/job. This is the documented Zerobus throughput lever. Streams are
cheap-ish but count against connection quotas (the docs' "connection tax"), so
this is a deliberate tradeoff, not free.

## `close` is idempotent

`close` half-closes the send side, drains pending acks, and cancels the stream's
scope (the ack-reader). It is safe to call more than once. After `close`, further
`ingest` calls return `Error (Stream_error _)` rather than raising — consistent
with the results-over-exceptions rule.

## Per-runtime notes

- **Lwt (`zerobus`)** — the reference runtime. Single-threaded cooperative
  scheduling: "single writer" means a single promise chain. Live TLS + built-in
  client-credentials OAuth are verified here.
- **Eio (`zerobus-eio`)** — OCaml 5.x, direct style. The façade is bracket-shaped
  (`with_stream` / `with_stream_oauth`): the ack-reader fiber lives in the
  `Switch` the bracket owns, and the stream is valid only inside the body. "Single
  writer" means a single fiber. Do not share a `stream` across fibers without your
  own synchronization.
- **Async (`zerobus-async`)** — the façade is bracket-shaped (`with_stream`) and
  takes a caller-supplied `headers_provider` for auth (built-in OAuth and live TLS
  are deferred follow-ups; Lwt is the TLS reference). "Single writer" means a
  single Async job.

## The bidirectional core

Unlike a request/response client, the Zerobus core is a client-initiated
**bidirectional stream**: it sends framed records while concurrently draining
acknowledgements. That concurrency is internal and structured (a scope owns the
ack-reader daemon; a mailbox carries acks back to waiters and to your
`ack_callback`). The single-writer rule is what lets that machinery stay
lock-free on the hot path.
