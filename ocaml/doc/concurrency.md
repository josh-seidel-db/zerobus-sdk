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
concurrency to manage. `ingest` does take the stream's state lock briefly to
assign the offset, admit the record to the replay buffer, and check the flow-control
bound atomically (so a record can never be queued onto an already-failed stream and
the un-acked byte accounting stays exact); the lock is held only for that
bookkeeping, not across the network send.

If you genuinely need concurrent writers to one logical table, either serialize
them behind your own mutex, or — better — shard across multiple streams (below).

## Flow control: `ingest` never loses, and may block

The driver holds an un-acked replay buffer (for recovery) bounded by
`max_inflight_requests` and `max_inflight_bytes`. Records are **never dropped** to
stay within the bound. When the buffer is full, `overflow_policy` governs `ingest`:

- **`Block`** (default): `ingest` suspends *in the effect* — the Lwt promise / Eio
  fiber / Async job parks until the ack-reader drains the buffer, then proceeds.
  This is the one place `ingest` can block, and it is cooperative (it never blocks
  an OS thread): other fibers on the same runtime keep running. It still comes from
  the single writer, so ordering is preserved.
- **`Fail`**: `ingest` returns `Error (Backpressure _)` immediately instead of
  parking. The stream stays healthy — this is a producer-over-production signal, not
  a fault, so it does not trigger recovery.

The byte ceiling defaults (`max_inflight_bytes = None`) to a budget derived from the
process's available memory, so it scales with the machine but never grows unbounded.
Blocked producers are also released if the stream fails, so a `Block` `ingest` never
hangs forever — it wakes and observes the error.

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
- **Async (`zerobus-async`)** — the façade is bracket-shaped (`with_stream` /
  `with_stream_oauth`). Built-in client-credentials OAuth and live TLS are both
  available as dune `select`s on optional deps (`cohttp-async`, `tls-async`); where
  a dep is absent, pass a bearer via `with_stream`'s `headers_provider` (Lwt/Eio
  are the always-on TLS references). "Single writer" means a single Async job.

## The bidirectional core

Unlike a request/response client, the Zerobus core is a client-initiated
**bidirectional stream**: it sends framed records while concurrently draining
acknowledgements. That concurrency is internal and structured (a scope owns the
ack-reader daemon; a mailbox carries acks back to waiters and to your
`ack_callback`). The single-writer rule is what lets that machinery stay
lock-free on the hot path.
