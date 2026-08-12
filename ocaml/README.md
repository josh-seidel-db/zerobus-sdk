# Zerobus SDK for OCaml

A native OCaml client for high-throughput data ingestion into Databricks Delta
tables via the Zerobus service. It speaks the Zerobus gRPC protocol directly — no
FFI to the Rust core — and runs on all three OCaml concurrency runtimes: **Lwt**,
**Eio**, and **Async**.

> Status: pre-release. Built and tested against mock servers on OCaml 4.14 (Lwt /
> Async) and 5.x (Eio); the transport, scoped OAuth, and Proto path are also
> verified live. Not yet published to a package registry.

## Quick start: loop, then flush

Ingestion is **asynchronous and pipelined**. `ingest` only *queues* a record and
returns its offset handle immediately; the record is sent and acknowledged on
background tasks. So the one pattern to internalize is: **queue in a loop, then
`flush` once at the end.**

```ocaml
let* client = Zerobus.create ~endpoint:"" ~workspace_url () in   (* endpoint derived *)
let* stream =
  Zerobus.create_stream client
    { Zerobus_core.Options.table_name = "main.default.my_table"; descriptor = None }
    ~client_id ~client_secret
    ~options:{ Zerobus.default_stream_options with record_type = Zerobus_core.Options.Json }
    ()
in
(* 1) queue in a loop — never wait per record *)
let* () =
  Lwt_list.iter_s
    (fun r -> let* _off = Zerobus.ingest stream r in Lwt.return_unit)
    records
in
(* 2) flush once — waits for every queued record to be durable *)
let* () = Zerobus.flush stream in
Zerobus.close stream
```

### The cardinal rule

**Never wait for an acknowledgment after every `ingest`.** Calling
`wait_for_offset` (or awaiting a per-record durability) inside the ingest loop
forces one full server round-trip per record and collapses throughput by orders
of magnitude — from the SDK's native pipelined rate down to ~1 record per
round-trip.

- `ingest` returns an **offset handle** you *can* wait on later — it is not a
  signal to wait now.
- For continuous / unbounded streams, either `flush` periodically (every N
  records) or register an **ack callback** for out-of-band durability
  notification — see [`examples/ack_callback_lwt.ml`](examples/ack_callback_lwt.ml).
- Reserve per-record `wait_for_offset` for genuinely low-volume cases where each
  record must be confirmed durable before continuing.
- Prefer the batch API (`ingest_records`) in hot paths.

### Flow control (no data loss)

Because `ingest` returns before the record is durable, the driver keeps an
**un-acked replay buffer** so it can re-send after a reconnect. That buffer is
bounded — by `max_inflight_requests` (a record count) and `max_inflight_bytes` (a
byte ceiling) — but **a record is never dropped to stay within the bound.** When
the buffer is full, `overflow_policy` decides what `ingest` does:

- **`Block`** (default) — apply backpressure: `ingest` waits until acknowledgements
  drain the buffer, then queues the record. Fast producers are throttled to the
  server's ack rate; nothing is lost.
- **`Fail`** — `ingest` returns `Error (Backpressure _)` instead of blocking, so
  your code decides (retry later, slow down, or deliberately drop). The buffer is
  never silently truncated.

```ocaml
let options =
  { Zerobus.default_stream_options with
    record_type = Zerobus_core.Options.Json;
    overflow_policy = Zerobus_core.Options.Fail;   (* or Block, the default *)
    max_inflight_requests = 100_000;
    max_inflight_bytes = None;                      (* None = smart, memory-derived *)
  }
```

`max_inflight_bytes = None` derives a budget from the process's available memory
(a fraction of free RAM, clamped to a safe range), so a capable machine buffers far
more than the 1M-record count cap without risking an out-of-memory — set an explicit
value to pin it. `Backpressure` is **not** a stream failure (the stream stays
healthy and recovery is not triggered); just retry the `ingest` after a `flush` or a
short wait.

## Interfaces

The gRPC streaming SDK is the primary interface; two thin optional helpers cover
the low-frequency and OpenTelemetry cases.

| Interface | Module | Package | Use when |
|---|---|---|---|
| gRPC streaming | `Zerobus` (Lwt), `Zerobus_eio`, `Zerobus_async` | `zerobus`, `zerobus-eio`, `zerobus-async` | High-volume, long-lived producers |
| REST | `Zerobus_rest` (Lwt), `Zerobus_rest_eio`, `Zerobus_rest_async` | `zerobus-rest`, `zerobus-rest-eio`, `zerobus-rest-async` | Infrequent / edge reporting, webhooks |
| OTLP | `Zerobus_otlp` (Lwt), `Zerobus_otlp_eio`, `Zerobus_otlp_async` | `zerobus-otlp`, `zerobus-otlp-eio`, `zerobus-otlp-async` | Already emitting OpenTelemetry logs/metrics |

Each interface is available on all three runtimes (Lwt is the reference). The Eio
REST/OTLP packages need OCaml 5.x; the Async REST/OTLP packages are optional and
build only where `cohttp-async` is installed (same posture as the Async built-in
OAuth — see the runtime notes below).

### Record types

The streaming SDK carries three encodings, chosen via `record_type` in
`stream_options`:

- **JSON** — no descriptor needed (`descriptor = None`).
- **Proto** — pass a serialized `DescriptorProto` in `table_properties.descriptor`.
- **Arrow** — Arrow IPC batches over Flight `DoPut`; the IPC codec lives in the
  optional `zerobus-arrow` package (the only component that links Apache Arrow /
  libarrow, kept separate so JSON/Proto users never pull it).

## Runtime packages

One runtime-agnostic core (`zerobus-core`) is instantiated by three thin runtime
packages. Pick the one matching your app's concurrency library:

- **`zerobus` (Lwt)** — the reference runtime. OCaml 4.14+. Live TLS + built-in
  client-credentials OAuth.
- **`zerobus-eio` (Eio)** — OCaml 5.x, direct style. Bracket-shaped façade
  (`with_stream` / `with_stream_oauth`).
- **`zerobus-async` (Async)** — bracket-shaped façade (`with_stream` /
  `with_stream_oauth`). Built-in client-credentials OAuth and live TLS are both
  optional (dune `select`s on `cohttp-async` and `tls-async`): present → they work;
  absent → supply a bearer via `with_stream`'s `headers_provider` and use cleartext
  (Lwt/Eio are the always-on TLS references).

See [`examples/json_loop_then_flush_eio.ml`](examples/json_loop_then_flush_eio.ml)
for the Eio (direct-style) shape.

## Examples

All under [`examples/`](examples/); each reads workspace credentials from the
environment (see each file's header):

| File | Shows |
|---|---|
| `json_loop_then_flush_lwt.ml` | **Start here** — the loop-then-flush pattern (Lwt, JSON) |
| `ack_callback_lwt.ml` | Async durability notification via an ack callback |
| `json_loop_then_flush_eio.ml` | The same pattern in Eio's direct style |
| `rest_insert_lwt.ml` | One-POST-per-batch via the REST interface |
| `otlp_export_lwt.ml` | Exporting OpenTelemetry logs via OTLP |

## Concurrency

The short version: **the client `t` is safe to share; a `stream` is
single-writer; fan out with more streams, not more writers per stream.** Full
contract in [`doc/concurrency.md`](doc/concurrency.md).

## Building and testing

The repo uses two opam switches — 4.14 (Lwt / Async) and 5.x (Eio). From `ocaml/`:

```sh
# 4.14 switch (Lwt, Async, REST, OTLP, Arrow):
dune build lib/core/ lib/lwt/ lib/async/ lib/arrow/ lib/rest/ lib/otlp/
dune test  test_driver/ test_driver_async/ test_driver_arrow/ test_rest/ test_otlp/
# Arrow tests (incl. test_driver_async_arrow) need: export PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig
# Async live-TLS / built-in-OAuth tests need a switch with tls-async / cohttp-async
# (optional deps) — see doc/arch/tls_async_status.md.

# 5.x switch (Eio):
dune build lib/core/ lib/eio/
dune test  test_driver_eio/
```

(`dune build` of the whole tree only succeeds on a switch that has *both* runtime
stacks installed; build the per-runtime directories on the matching switch.)

## Design

Architecture and rationale are in [`DESIGN.md`](DESIGN.md); the phased build plan
and its status are in [`PLAN.md`](PLAN.md).

## License

Apache-2.0, matching the rest of the monorepo.
