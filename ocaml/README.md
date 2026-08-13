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

Each interface is available on all three runtimes (Lwt is the reference), and all
three have been verified live against a real workspace. The Eio REST/OTLP packages
need OCaml 5.x; the Async REST/OTLP packages reuse the same pure-ocaml-tls HTTP
backend as the Async gRPC transport (`tls-async`, no `cohttp-async`/OpenSSL — see
the runtime notes below), so they build wherever `zerobus-async` does.

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
  `with_stream_oauth`). Live TLS and built-in client-credentials OAuth both use
  pure ocaml-tls via a single dune `select` on `tls-async` (the OAuth token POST is
  a hand-rolled HTTP/1.1 request over `Tls_async.connect`, not `cohttp-async`/
  OpenSSL): present → both work; absent → supply a bearer via `with_stream`'s
  `headers_provider` and use cleartext (Lwt/Eio are the always-on TLS references).
  The `zerobus-rest-async` / `zerobus-otlp-async` packages reuse this same
  pure-TLS HTTP backend, so no interface pulls OpenSSL.

See [`examples/json_loop_then_flush_eio.ml`](examples/json_loop_then_flush_eio.ml)
for the Eio (direct-style) shape.

## Examples

All under [`examples/`](examples/); each reads workspace credentials from the
environment (see each file's header):

Each example exists for all three runtimes (`_lwt`, `_eio`, `_async`), so you can
copy the one matching your concurrency library:

| Example | Shows | Runtimes |
|---|---|---|
| `json_loop_then_flush_*` | **Start here** — the loop-then-flush pattern (JSON) | Lwt, Eio, Async |
| `ack_callback_*` | Async durability notification via an ack callback | Lwt, Eio, Async |
| `rest_insert_*` | One-POST-per-batch via the REST interface | Lwt, Eio, Async |
| `otlp_export_*` | Exporting OpenTelemetry logs via OTLP | Lwt, Eio, Async |

(e.g. `json_loop_then_flush_lwt.ml`, `json_loop_then_flush_eio.ml`,
`json_loop_then_flush_async.ml`.)

## Concurrency

The short version: **the client `t` is safe to share; a `stream` is
single-writer; fan out with more streams, not more writers per stream.** Full
contract in [`doc/concurrency.md`](doc/concurrency.md).

## Building and testing

The repo uses two opam switches — 4.14 (Lwt / Async) and 5.x (Eio). From `ocaml/`:

Arrow-linked builds/tests need libarrow on the pkg-config path:
`export PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig` (macOS Homebrew).

```sh
# 4.14 switch (Lwt + Async, plus REST / OTLP / Arrow):
dune build lib/core/ lib/lwt/ lib/async/ lib/arrow/ lib/rest/ lib/otlp/proto/ lib/otlp/
dune test  test/ test_driver/ test_driver_async/ test_driver_arrow/ \
           test_driver_async_arrow/ test_rest/ test_otlp/ test_otlp_otel/
# test_otlp_otel/ decodes with the canonical `opentelemetry` opam package's protos
# (a cross-implementation OTLP wire-compat check) — install `opentelemetry`.

# 5.x switch (Eio, OCaml >= 5.0):
dune build lib/core/ lib/eio/ lib/rest/eio/ lib/otlp/proto/ lib/otlp/eio/
dune test  test_driver_eio/ test_driver_eio_arrow/ test_rest_eio/ \
           test_otlp_eio/ test_otlp_otel_eio/
```

The Async built-in-TLS/OAuth and Async REST/OTLP/OTEL tests
(`test_driver_async_tls/`, `test_driver_async_oauth/`, `test_rest_async/`,
`test_otlp_otel_async/`) need a switch with `tls-async` (and, for the mock-server
side, `cohttp-async`) on top of the 4.14 stack — see
[`doc/arch/tls_async_status.md`](doc/arch/tls_async_status.md). They are `(optional)`
and vanish on a switch lacking those deps; the shipped Async libraries themselves
build on the plain 4.14 switch. In CI they run in a separate **non-blocking** job
(`.github/workflows/ci-ocaml-async-bespoke.yml`) that builds the combined switch.
`test_otlp_async/` is the one exception that stays out even there: its mock
collector needs `Grpc_async.Server` (grpc-async 0.2.0 / async v0.16), which can't
coexist with `tls-async` (async v0.15) on one switch — its Async OTLP coverage is
provided by `test_otlp_otel_async/`.

The env-gated live integration suite (`test_integration/`, per DESIGN §12.4) runs on
all three runtimes against a real workspace when `DATABRICKS_HOST` /
`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` / `ZEROBUS_TEST_TABLE` are set,
and is a clean no-op success otherwise.

(`dune build` of the whole tree only succeeds on a switch that has *both* runtime
stacks installed; build the per-runtime directories on the matching switch.)

### Formatting and docs

Source is formatted with [ocamlformat](https://github.com/ocaml-ppx/ocamlformat)
0.29.0 (`.ocamlformat`, `profile = default`; only OCaml is formatted — the `dune`
files keep their hand-formatting). CI enforces it. Locally:

```sh
dune build @fmt            # check formatting
dune build @fmt --auto-promote   # apply it
```

API docs build with odoc; CI compiles every doc comment (scoped per switch, since
the Eio-only packages can't be documented alongside the Lwt ones):

```sh
dune build @doc            # then open _build/default/_doc/_html/index.html
```

## Design

Architecture and rationale are in [`DESIGN.md`](DESIGN.md); the phased build plan
and its status are in [`PLAN.md`](PLAN.md).

## License

Apache-2.0, matching the rest of the monorepo.
