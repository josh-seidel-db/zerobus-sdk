# Recovery Spike for the OCaml Zerobus SDK

**Purpose.** De-risk Phase 6 (Recovery) — the highest-risk, last-unspiked
transport mechanic in the SDK design (DESIGN.md §12.3, PLAN.md Phase 6). Proves,
with a compiled-**and-run** test, that:

1. When a bidi gRPC ingest stream breaks mid-stream, the client **detects** it
   (the server ends the stream with a retryable, non-OK gRPC status).
2. The client **reconnects** with exponential backoff + jitter + wall-clock budget
   (mirroring the sister SDK's `Retry` module).
3. The client **replays only the un-acked tail of records, in order**.
4. **Every record is ultimately acked exactly once** — no loss, no duplicate,
   offsets contiguous.

Recovery is what turns a raw bidi RPC into a *durable* ingest stream — the core
difference from the transport spikes (`spike/`, `spike-eio/`, `spike-async/`).

## RESULT: ✅ COMPILED, RUN, AND GREEN

Runs on the `fl414` opam switch (OCaml 4.14.1, `grpc-lwt 0.2.0`, `h2 0.12.0`).
Captured evidence in `EVIDENCE_RECOVERY.txt`; stable across repeated runs
(1 reconnect, 200/200 acked exactly once, contiguous). Verdict: **the recovery
design in §12.3 holds — no revision needed.**

```
records_target         : 200
reconnects             : 1      <- >=1 proves recovery triggered
acks_observed          : 200 (incl. replays)
all_offsets_acked      : true
no_duplicates          : true
offsets_contiguous     : true
```

### Topology

**Separate-process** (the proven pattern from `spike-flight` / `spike-eio` /
`spike-async`): the mock server (`ingest_server_recovery.ml`) is spawned as a
subprocess and announces `READY <port>` on stdout; the test driver
(`recovery_spike.ml`) reads that, connects, and drives the recovery client. This
gives a *real* connection break (a fresh TCP+h2 connection per reconnect), which
is both the realistic topology and what makes the recovery observable.

### Scenario

- **Server:** acks every record **by the record's own `client_seq`** (its durable
  offset). On the first connection, after acking 50 records it aborts the stream
  with gRPC status **`Unavailable`** (retryable). A process-global `already_dropped`
  flag ensures only the first stream breaks; the reconnected stream is served to
  completion.
- **Client:** sends 200 records; on a non-OK stream end it backs off and reconnects,
  re-sending **only the records not yet acked**. Repeats until the un-acked set is
  empty or the 30 s budget expires.
- **Assertions (all run, all green):** recovery triggered (≥1 reconnect); all 200
  acked; no offset acked twice; distinct offsets are exactly 0..199.

## Running it

```bash
cd ocaml/spike-recovery
eval $(opam env --switch=fl414)   # must use fl414; PATH dune/ocaml may be system 5.x
dune runtest --force
# or, capturing evidence:
dune exec ./recovery_spike.exe
```

macOS has no `timeout`; if you want a watchdog, run in the background and
`kill -9` after ~60 s. A green run exits 0 and prints the evidence block above.

## Layout

```
spike-recovery/
├── README.md                 # this file
├── dune-project
├── dune                      # protobuf codegen + server exe + test driver
├── proto/
│   └── ingest.proto          # minimal Zerobus-shaped bidi service
├── ingest_server_recovery.ml # mock server: acks by client_seq, drops first stream once
├── recovery_spike.ml         # test driver: reconnect + replay un-acked tail
└── EVIDENCE_RECOVERY.txt      # captured passing run
```

## Findings / drift corrected

This spike was first drafted with an **in-process** server+client and a server
that (a) ended the "dropped" stream with a clean `OK` status and (b) assigned
offsets from a per-connection counter reset to 0 on each connection. That version
**compiled but hung**: because acks came back under counter offsets 0..49 every
time — never the client's `client_seq` — the client's un-acked set (50..199) never
shrank, so it reconnected forever. Two corrections made it correct and runnable:

1. **Ack by `client_seq`, not a per-connection counter.** A replayed record must be
   acked under its *original* offset so the un-acked set shrinks monotonically. This
   is a real invariant the production driver must preserve (the Zerobus ack watermark
   `durability_ack_up_to_offset` is defined over the client's offsets).
2. **Break with a retryable status + separate-process topology.** Ending the stream
   with `Unavailable` (not `OK`) is what a recoverable server break actually looks
   like, and the separate-process server produces a genuine connection drop rather
   than an in-process half-close.

No gRPC/h2 **API** drift beyond what `spike/` already documented — the recovery
logic lives entirely in the client driver.

## Relationship to the design & next steps

- Closes the last unproven transport risk (§12.3, PLAN.md Phase 6).
- The retry + replay logic ports into the real streaming driver
  (`lib/core/stream.ml`, the `Make(Io)` functor): un-acked replay buffer bounded by
  `max_inflight_requests`, failure classification → retryable flag, reconnect with a
  fresh token + re-stamped table header, ack watchdog (`server_lack_of_ack_timeout_ms`).
- Production reuses the sister SDK's `Retry.with_retry` (D2) rather than the local
  copy here.
- Still to validate at integration time: recovery on Eio/Async (behind the `IO`
  functor) and against the live service.
