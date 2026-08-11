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

## Interfaces

The gRPC streaming SDK is the primary interface; two thin optional helpers cover
the low-frequency and OpenTelemetry cases.

| Interface | Module | Package | Use when |
|---|---|---|---|
| gRPC streaming | `Zerobus` (Lwt), `Zerobus_eio`, `Zerobus_async` | `zerobus`, `zerobus-eio`, `zerobus-async` | High-volume, long-lived producers |
| REST | `Zerobus_rest` | `zerobus-rest` | Infrequent / edge reporting, webhooks |
| OTLP | `Zerobus_otlp` | `zerobus-otlp` | Already emitting OpenTelemetry logs/metrics |

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
- **`zerobus-async` (Async)** — bracket-shaped façade with caller-supplied auth
  headers (built-in OAuth + live TLS are follow-ups; Lwt is the TLS reference).

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
