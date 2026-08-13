# Arrow Flight `DoPut` spike — the native, free half of Arrow support

Proves the half of Arrow ingestion that needs **no Arrow library** (DESIGN.md
§8.5): the **real Apache Arrow Flight protobuf messages** flowing over a
client-initiated **bidirectional gRPC stream** in native OCaml, with the Arrow
IPC payload carried as an **opaque `data_body`** and the Zerobus per-batch offset
riding in `app_metadata` — exactly as `rust/sdk/src/stream/arrow/` does.

**RESULT: PASS (compiled and run).** OCaml 4.14.1, `grpc-lwt 0.2.0`, `h2 0.12.0`,
`ocaml-protoc 4.1` — **no `arrow`/`parquet`/`libarrow` dependency**. 200 batches
sent, 200 acked, first ack mid-send (~#60–100 of 200), offsets 0–199 contiguous,
and every batch's opaque `data_body` verified byte-exact. Evidence:
[`EVIDENCE_FLIGHT.txt`](EVIDENCE_FLIGHT.txt); regenerate with `dune runtest --force`.

## Why this matters

An earlier draft called Arrow "the one dependency that may not exist." Inspecting
this repo's own SDKs corrected that (DESIGN.md §8.5): Zerobus's Arrow path is
Flight **`DoPut` only** (a `grep` of the Rust core shows `do_put` and nothing
else), and `DoPut` is a plain bidi gRPC RPC. So Arrow support splits in two:

- **The Flight RPC** — `FlightData`/`PutResult` over bidi gRPC. **This spike.**
  Native OCaml, no Arrow library.
- **The Arrow IPC codec** — building/parsing the columnar byte blob. The only
  real gap; an optional scoped `libarrow` binding on the Arrow record type,
  added when the Beta record type is prioritized. Not covered here, on purpose.

This spike nails down that the first half is genuinely free.

## What it proves (4 assertions)

The driver spawns the mock server as a subprocess (the proven topology from
`spike-eio`/`spike-async`), then runs a `DoPut` client with two concurrent Lwt
fibers — a sender streaming `FlightData` and a reader draining `PutResult` acks:

1. **every batch acked** — the schema `FlightData` (no `data_body`) is correctly
   *not* acked, matching Arrow IPC framing; the 200 record-batch messages are.
2. **acks interleave with sends** — `sends_done_at_first_ack` ≪ 200, so the
   server's acks come back while the client is still streaming (real bidi).
3. **offsets strictly increasing** — per-stream ordering preserved.
4. **opaque `data_body` carried byte-exact** — each batch body is distinct; the
   server returns `"<offset>:<len>:<checksum>"` in `PutResult.app_metadata` and
   the client verifies len+checksum against what it sent. This proves the proto
   carries arbitrary Arrow IPC bytes through unmodified **without understanding
   them** — the whole premise of "the RPC half needs no Arrow library."

## The proto (`proto/flight.proto`)

A faithful subset of the upstream `Flight.proto`: `FlightDescriptor`,
`FlightData` (with the authentic **`data_body = 1000`** high field number),
`PutResult`, and `service FlightService { rpc DoPut(stream FlightData) returns
(stream PutResult); }`. `ocaml-protoc` handles the high field number and nested
enum without issue.

## Fidelity to real Zerobus (and the boundary)

Faithful: the real Flight message types and field numbers; `DoPut` bidi shape;
offset in `FlightData.app_metadata`; ack in `PutResult.app_metadata`; schema
message first, then batch messages.

Out of scope (same boundary as the other transport spikes): real Arrow IPC
encoding (`data_body` is a stand-in blob), TLS/ALPN (proven in `spike-live/`),
OAuth, recovery, and the live Zerobus service. Real Zerobus puts JSON in
`app_metadata`; the spike uses a plain integer + `len:sum` to stay free of a JSON
dep — the mechanism is identical.

## Running it

```bash
eval $(opam env --switch=fl414)
cd ocaml/spike-flight
dune runtest --force
```
