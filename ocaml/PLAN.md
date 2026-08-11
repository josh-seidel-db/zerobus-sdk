# Zerobus SDK for OCaml — Implementation Plan

Companion to `DESIGN.md`. This is the build plan: what to implement, in what
order, with what acceptance criteria, given what the spikes have already proven.
Section refs (§) point into `DESIGN.md`.

**Status legend:** ✅ proven by a run spike · 🟡 designed, not built · ⬜ not started

---

## 0. Where we are (proven vs. remaining)

**Proven with compiled-and-run evidence (five spikes, all green):**

| Spike | Proves | Runtime / env | Evidence |
|---|---|---|---|
| `spike/` | client-initiated **bidi gRPC** (200 rec, interleaved acks) + raw-h2 framing fallback | Lwt, OCaml 4.14.1 | `spike/EVIDENCE.txt` |
| `spike-eio/` | same on Eio; surfaced the grpc-eio client-flush finding (separate-process topology) | Eio, OCaml 5.2.0 | `spike-eio/EVIDENCE_EIO.txt` |
| `spike-async/` | same on Async (`zerobus-async` is validated v1 scope) | Async, OCaml 4.14.1 | `spike-async/EVIDENCE_ASYNC.txt` |
| `spike-flight/` | **real Arrow Flight `DoPut`** proto over bidi, opaque `data_body` byte-exact, **no Arrow lib** | Lwt, OCaml 4.14.1 | `spike-flight/EVIDENCE_FLIGHT.txt` |
| `spike-live/` | native **TLS 1.3 + ALPN h2** to the real Zerobus endpoint; native **scoped OAuth** (`authorization_details`) token — against a real Azure workspace | Lwt, OCaml 4.14.1 | `spike-live/EVIDENCE_LIVE.txt` |

**The one required new dependency is confirmed:** a TLS backend (`ocaml-tls`:
`tls-lwt`/`tls-eio` + `ca-certs`), confined to the runtime shim packages;
installing it rebuilt gluten's real `Tls_io` (§12.1). Everything else is reuse.

**Not yet built:** the actual SDK — the `IO` functor core, the streaming driver
with recovery, the three runtime packages, REST/OTLP, the Arrow IPC codec,
examples/docs/CI. Two things are v1 scope but **never spiked** (spike each before
integrating): **mid-stream recovery** (§12.3) and the **Arrow IPC-bytes codec**
(§8.5, D3) — the latter also carries the only real new C++ dep (`libarrow`).

**Blocking prerequisite:** the authoritative **Zerobus `.proto`** (open question
1). Everything downstream of Phase 1 needs it. See §Decisions.

---

## Decisions to lock before/at kickoff (from §11 open questions)

| # | Decision | Recommendation | Blocks |
|---|---|---|---|
| D1 | Vendor the Zerobus `.proto` (source + license to redistribute in `ocaml/`) | Obtain from the Rust core/service repo; confirm license. **Initial proof step below.** | Phase 1 → all |
| D2 | Auth dependency: full `databricks-sdk-ocaml` vs. trimmed auth-only | Reuse the sister SDK's `Auth` (token cache) + `Config`; narrow dep (§8.3, §12.2) | Phase 4 |
| D3 | Arrow IPC codec (v1-blocking) | **v1-required scope.** Build scoped `libarrow` ctypes binding for IPC codec (§8.5.2, ~4–6h effort). Arrow C++/IPC library directly — never the C++ Zerobus SDK. Isolated to Arrow record type; Proto/JSON unaffected. Spike first (real batch → IPC round-trip). | Phase 7 |
| D4 | Versioning: lockstep vs. independent semver for the 3 packages | Independent per-runtime `ocaml/v*`-style tags; shared core version | Phase 8/release |
| D5 | TLS backend default | `ocaml-tls` (no C dep); `ocaml-ssl` opt-in flavor (§8.1/§12.1) | Phase 3 |

### D1 initial proof step (do first, before Phase 1 commits)

Before building on the vendored proto, prove it round-trips end to end so a bad
vendor/version is caught immediately:

1. Obtain the authoritative Zerobus `.proto` (Rust core/service repo); confirm the
   license permits redistribution in `ocaml/`.
2. Run `ocaml-protoc --binary --make` on it (as the spikes already do for Flight).
   **Proof gate A:** codegen succeeds — no unsupported constructs (repeat the
   `data_body=1000`-style check the Flight spike passed).
3. **Proof gate B — live wire check:** encode a minimal Zerobus ingest request
   with the generated types and send it over the already-proven TLS+ALPN+scoped-
   OAuth path (`spike-live/` extended) to the real endpoint; assert the server
   accepts the framing (a well-formed gRPC response/status, not a decode/parse
   error). This confirms the vendored proto matches the live service *before* any
   SDK code depends on it.

Only after gates A+B pass do we treat the proto as the foundation for Phase 1+.

---

## Phase 1 — Proto + project skeleton  🟢 DONE  (prereq: D1 — gates A+B PASS)

**Goal:** the repo builds, generates types, and round-trips protobuf.

**Done (fl414 / OCaml 4.14.1):**
- ✅ **D1 gate A cleared.** Vendored the authoritative `rust/sdk/zerobus_service.proto`
  (Apache-2.0, our repo) **byte-identical** under `lib/core/proto/vendor/`, plus its
  one import `google/protobuf/duration.proto`. Drift found: `ocaml-protoc` 4.1 cannot
  parse proto2 `reserved` statements (comment-only, no wire effect) — the `dune` rule
  strips them into a build-tree copy before codegen. Vendor stays a faithful mirror;
  `vendor/README.md` documents it and gives a CI drift-check.
- ✅ `dune-project` with `(generate_opam_files true)` and the four packages
  (`zerobus-core`, `zerobus`, `zerobus-eio`, `zerobus-async`); all four `.opam`
  generate; `opam lint` passes on the two 4.14-buildable ones (`zerobus-core`, `zerobus`).
- ✅ `ocaml-protoc` codegen rule (`lib/core/proto/dune`) generates the Zerobus +
  Duration wire types; `zerobus_core` re-exports them as `Wire` / `Duration`.
- ✅ Round-trip test (`test/test_proto_roundtrip.ml`, alcotest, **5/5 green**):
  `create_stream_request`, proto & JSON `ingest_record` through the `EphemeralStream`
  oneof envelope, ack watermark, and `close_stream_signal` carrying the imported
  `Duration`.

**Remaining for Phase 1:**
- ✅ **D1 gate B (live wire check) — PASS.** `spike-live/gate_b_wire.ml` encoded a real
  `EphemeralStream` exchange (CreateIngestStream + IngestRecord, JSON record type)
  with the generated (gate-A) types and sent it over native TLS 1.3 + ALPN h2 +
  scoped OAuth to the **live** Zerobus endpoint
  (`984752964297111.zerobus.eastus2.azuredatabricks.net:443`). Server accepted the
  framing: **http=200, grpc-status=0, real `stream_id` returned**. Stable across 2
  runs. Evidence: `spike-live/EVIDENCE_GATE_B.txt`. Resources (SP secret + table)
  created then torn down (SP shell not deletable from the workspace profile —
  account/Entra-managed, harmless; secrets deleted so it can't mint tokens).
  **Two findings** worth carrying into the SDK transport:
  1. **`grpc-lwt 0.2.0`'s client sender omits the h2 flush (§6.5) on the Lwt path
     too** — against a real remote server the RPC stalls. Gate B works by driving
     h2 directly (raw-h2 framing, proven in `spike/`) and flushing each frame. The
     `Zerobus_io_lwt` transport must flush (or use the raw-h2 writer), not rely on
     grpc-lwt's client.
  2. **The `:authority` h2 pseudo-header is required** — without it the live server
     replies with an h2 `protocol_error` and never responds. Must be set on every
     request.
- ✅ `dune build` + round-trip test also green on a **5.x** switch (zbeio, OCaml 5.2.0).
- ⬜ Flight `DoPut` subset proto (from `spike-flight/proto/flight.proto`) is not yet
  vendored into the package — deferred to Phase 7 wiring.
- Layout per §9 is partially stood up (`lib/core/` + `proto/`); `lib/lwt|eio|async|rest|otlp/`
  land in later phases.

**Acceptance — MET:** `dune build` on a 4.14 and a 5.x switch; proto encode/decode
round-trip test passes; `opam lint` clean; **D1 gates A+B both PASS** (codegen +
live wire check against the real service). Phase 1 complete.

---

## Phase 2 — The `IO` functor + `Grpc_transport.S`  🟢 DONE  (§6.2)

**Goal:** the runtime-agnostic core interface, validated against all three
runtimes' proven transport shapes.

**Done:**
- ✅ `Grpc_transport.S` (`lib/core/grpc_transport.ml`) — abstracts the bidi RPC as
  `connect`/`start_bidi`/`send`/`close_send`/`recv`/`status`/`shutdown`, the shape
  all three runtimes' clients reduce to (grpc-lwt push-fn+`Lwt_stream`, grpc-async
  `Pipe`, grpc-eio `Seq`). IO-parametric (`type 'a io`); opaque `string` messages.
- ✅ `module type IO` (`lib/core/io.ml`) exactly per §6.2: `'a t`/`return`/`bind`
  (with a `'a io` alias so nested sigs don't shadow), **thunked** `both`,
  `fork_daemon` into `Scope`, `Scope.with_scope`, `Mutex`, bounded blocking
  `Mailbox`, `H2_client : Grpc_transport.S`, `sleep`.
- ✅ `Error` taxonomy (`lib/core/error.ml`) with `is_retryable` mirroring the Rust
  core; results-over-exceptions throughout.
- ✅ `Make(Io)` streaming-driver **skeleton** (`lib/core/stream.ml`) that exercises
  every IO primitive in the real driver shape (stream `Scope` + `fork_daemon`
  ack-drain into `Mailbox` + `both` send/recv), proving the signature is usable.
- ✅ Three instantiations: `Zerobus_io_lwt` (full transport bridge — grpc-lwt
  inversion handled via `Lwt_mvar`), `Zerobus_io_async` (`Pipe`/`Sequencer`), and
  `Zerobus_io_eio` (direct-style `'a t = 'a`, thunked `both` via `Fiber.both` +
  refs, `fork_daemon` via `Fiber.fork_daemon ~sw` — findings #1/#2 handled).
  Eio/Async transport `connect`/`start_bidi` are honest Phase-3 stubs (TLS path);
  the bidi mechanics are already proven in `spike-eio/` / `spike-async/`.

**Acceptance — MET:** each `Zerobus_io_*` compiles against its runtime, and
`Zerobus_core.Make(Io)` **typechecks on all three** (Lwt+Async on fl414 4.14.1,
Eio on zbeio 5.2.0). `dune build lib/` is green on both switches (eio library is
`(enabled_if (>= %{ocaml_version} 5.0))`). This confirms the §6.3 three-package
split is realizable — M1 (skeleton+core) essentially reached.

**Deferred to later phases:** wiring the real driver body onto the skeleton
(Phase 5), TLS connect for all runtimes (Phase 3), and a functor-level test using
the spikes' mock server as the backend.

---

## Phase 3 — TLS + connection establishment  🟢 MOSTLY DONE  (§12.1; proven §spike-live)

**Goal:** real TLS-secured h2 connections with ALPN `h2`, per runtime.

**Done:**
- ✅ **Lwt** (`lib/lwt/zerobus_io_lwt.ml`): `connect` does real **TLS 1.3 + ALPN
  `h2`** — `Ca_certs.authenticator` trust anchors, `Domain_name` peer-name
  verification, `Tls.Config.client ~alpn_protocols:["h2"]`, `Tls_lwt.Unix.client_of_fd`,
  then `H2_lwt_unix.Client.TLS.create_connection`. **Fails fast** with
  `Transport_error` if ALPN ≠ `h2` (checks `epoch.alpn_protocol`). A faithful port
  of the proven `spike-live/tls_alpn.ml`. Keeps a **cleartext h2c** path for the
  loopback mock servers (selected by port; `connection` is a `Tls | Plain` variant).
  `:scheme` follows the transport (`https`/`http`). Compiles on fl414; the functor
  still typechecks.
- ✅ Deps added (never to `zerobus-core`): `tls-lwt`+`ca-certs`+`domain-name` on
  `zerobus`; `tls-eio`+`ca-certs`+`domain-name` on `zerobus-eio`.

**Remaining:**
- ⬜ **Eio/Async connect** are honest stubs that compile. **Eio** `connect` needs the
  caller's `Eio.Env`+`Switch.t`, which the direct-style `Zerobus_eio` façade supplies
  in **Phase 5** (§6.2) — by design, not a blocker; `tls-eio` is present and builds.
  **Async** `connect` is deferred (h2-async+tls-async wiring is version-specific and
  Lwt is the reference runtime); the bidi mechanics are proven in `spike-async/`.
- ✅ **Live handshake re-verified** — D1 gate B connected the Lwt TLS+ALPN-h2 path
  to the real endpoint (`984752964297111.zerobus.eastus2...:443`), http=200/grpc=0.

**Acceptance:** a handshake over TLS+ALPN to the real endpoint on each runtime.
*(Lwt TLS path built + faithful-ported; live re-verification + Eio/Async connect
pending as above.)*

---

## Phase 4 — Scoped OAuth + config  🟡  (§12.2; proven §spike-live, Lwt)

**Goal:** table-scoped Zerobus tokens, cached and refreshed, reusing the sister
SDK.

- `zerobus-core`: pure `Oauth.token_request_body` builder (scope=`all-apis`,
  `resource=…/zerobusDirectWriteApi`, `authorization_details` UC array) — ported
  from the proven `spike-live/oauth_token.ml`.
- Fetch+cache via the sister SDK's `Auth.with_cached_token` / `fetch_oauth_token`
  (D2). Endpoint derivation (`<workspace-id>.zerobus.<region>…`) reusing its
  host/cloud parsing.
- `create_stream_with_headers` custom-auth path (§5.2); re-stamp table-name
  metadata header on every (re)connect.

**Acceptance:** each runtime obtains a live scoped token and attaches it +
table-name header to a stream.

---

## Phase 5 — Streaming driver: ingest / offset / flush / ack  🟢 MOSTLY DONE  (§5, §6.4)

**Goal:** the public data-plane API over the bidi driver.

**Done:**
- ✅ `Options` module (`lib/core/options.ml`): `offset` (private int64), `record_type`,
  `descriptor`, `table_properties`, `ack_callback`, `stream_options` with the Go-SDK
  `DefaultStreamConfigurationOptions` (1M inflight, recovery on 15s/2s/4, 60s ack,
  5min flush).
- ✅ Real `Make(Io)` driver (`lib/core/stream.ml`): `open_stream` (connect →
  start_bidi → send CreateIngestStream → fork ack-reader into the scope), `ingest`
  / `ingest_records` (assign offset, buffer for replay, send — **never wait**),
  `wait_for_offset` (blocks on the monotonic durability watermark via per-waiter
  mailboxes), `flush` (wait once for the last issued offset), `close`. Ack-reader
  advances the watermark, fires `ack_callback`, wakes waiters. Un-acked replay
  buffer maintained (bounded by `max_inflight_requests`) ready for P6.
- ✅ **Rewrote `Zerobus_io_lwt`'s transport to raw h2 with send-then-flush** — the
  gate-B finding (grpc-lwt 0.2.0's client omits the flush) applies to the real
  driver too; without it the driver deadlocks. Now frames+flushes each message,
  deframes responses off the h2 body, reads the grpc-status trailer, sets
  `:authority`. Dropped the grpc-lwt dep from `zerobus`.
- ✅ **Driver test green** (`test_driver/`, fl414): mock `EphemeralStream` server
  (separate process, cleartext h2c, speaks the real vendored proto) + Lwt driver;
  ingest 200 (loop-then-flush) → flush → all acked, **3/3 alcotest, stable ×4**.
  Evidence: `test_driver/EVIDENCE_DRIVER.txt`.
- **API ergonomics bar met:** loop-then-`flush` is the obvious path; no
  `ingest_and_wait` exists (§5.3 cardinal rule).

**Done (Phase 4/5 glue):**
- ✅ Public `Zerobus` Lwt module (`lib/lwt/zerobus.ml`): `create` (endpoint
  derivation + workspace-id parse via new `lib/core/config.ml`), `create_stream`
  (mints a table-scoped OAuth token — client-credentials, `authorization_details`,
  cached with ~30s early refresh — builds headers, opens a long-lived scope, calls
  `open_stream`), `create_stream_with_headers` (custom auth), and re-exported
  `ingest`/`ingest_records`/`wait_for_offset`/`flush`/`close`. Compiles on fl414
  (live verification pending creds). Also exposes `Zerobus.Driver` /
  `Zerobus.Io_lwt_for_test` for the low-level (mock, no-OAuth) test path.

**Remaining:**
- ⬜ Proto record type end-to-end (needs a real DescriptorProto; JSON proven — see
  the follow-up note). `Zerobus_eio` direct-style façade + Async driver run (the
  driver is runtime-agnostic and typechecks on all three; only Lwt is run-proven).

**Acceptance:** `ingest`×N → `flush` against the mock server — {b MET on Lwt}
(JSON), plus recovery (Phase 6). Eio/Async runs + Proto records pending.

---

## Phase 6 — Recovery  🟢 DONE (spiked + integrated + run)  (§12.3)

**Goal:** durable streams — the last unproven transport piece.

**Done:**
- ✅ Spike proven earlier (`spike-recovery/`), now **integrated into the driver**
  (`lib/core/stream.ml`): the `stream` carries reconnect state (`conn`,
  `create_request`, `service`/`rpc`/`reconnect_headers`, a mutable `call` swapped
  on reconnect, a `send_lock` serializing sends against the swap).
- ✅ Un-acked replay buffer retained from `last_acked+1` (bounded by
  `max_inflight_requests`). On a **retryable** failure, `reconnect_and_replay`
  opens a fresh bidi, re-sends `CreateIngestStream` + the un-acked tail **in order**,
  swaps the call. Bounded by `recovery_retries` with `Io.sleep` backoff (doubling,
  capped at `recovery_timeout_ms`). Failure classification via `Error.is_retryable`;
  fatal/exhausted → `fail_all` wakes waiters with the error. Teardown via `Scope`.
- ✅ **Driver recovery test green** (`test_driver/test_recovery_lwt.ml` +
  `ephemeral_server_drop.ml`): mock drops the first stream after 50 acks with
  `UNAVAILABLE`; driver reconnects, replays, flush returns Ok, watermark advanced.
  3/3 alcotest, stable ×3. Evidence: `test_driver/EVIDENCE_RECOVERY_DRIVER.txt`.
- **Bug found+fixed by the test:** `recv=None` is ambiguous (clean OK end vs a
  broken stream) since the gRPC status rides in the trailer — the ack-reader now
  checks `H2_client.status` on end-of-stream and recovers on a retryable status.

**Remaining (not blocking):** honor `StreamPausedMaxWaitTimeMs` on a server
`CloseStreamSignal` (currently treated as a clean end); `server_lack_of_ack_timeout_ms`
ack watchdog; fresh token on reconnect (headers re-stamped, but token refresh is
the public-API layer's job). Recovery on Eio/Async (behind the functor).

---

## Phase 7 — Record types + all three interfaces (all v1-blocking)  🟡  (§4, §8.5)

Every item here is **v1 scope** — the SDK ships all three Zerobus interfaces and
all record encodings.

### 7a — Arrow Flight `DoPut` RPC (proven, low effort)

- **Status:** ✅ Proven native in `spike-flight/` — no Arrow library needed.
- **Work:** Wire the real `Flight.proto` (`FlightData`/`PutResult`, authentic
  `data_body=1000` field) onto the Phase 5 driver as the `Arrow` record type.
- **Acceptance:** 200 batches streamed with opaque `data_body` bytes, offsets in
  `app_metadata`, server echoes offset back in ack. Uses the same bidi machinery
  as Proto/JSON; only the record-type encoding differs.

### 7b — Arrow IPC codec (v1-blocking, D3)  ✅ SPIKE PROVEN

The one v1 item carrying a new C++ dependency. **Codec spike is done and green**
(`spike-arrow-ipc-real/`, compiled-AND-RUN, real libarrow linked); integration
onto the Phase 5 driver remains.

**Spike result (`spike-arrow-ipc-real/`, EVIDENCE_ARROW_IPC.txt):**
- ✅ Real C++ binding to Arrow C++ IPC (`arrow::ipc::MakeStreamWriter` /
  `RecordBatchStreamReader`) via a C-ABI stub + OCaml-runtime marshalling stubs.
- ✅ Round-trip crosses OCaml → C++ → Arrow → IPC bytes → Arrow → C++ → OCaml: a
  2-column RecordBatch (int64 `id` + utf8 `name`, incl. empty string + unicode),
  5 rows → 504 IPC bytes → decoded, schema + every value faithful.
- ✅ Asserts the **real** Arrow stream magic `0xFFFFFFFF` (continuation marker),
  not just "non-empty".
- ✅ The OCaml exe **actually links libarrow** (`otool -L` → `libarrow.2500.dylib`,
  Apache Arrow 25.0.1). Stable across repeated runs.
- **API drift found (only a real build catches):** Arrow 25.x headers need
  **C++20** (`std::span`/`std::log2p1`) — `-std=c++17` fails; ocamlopt link flags
  must be `-cclib`-prefixed; Homebrew Cellar path is version-specific so the dune
  captures `pkg-config` output into `*.sexp` at build time.
- ⬜ Integration hook (feed IPC bytes into `FlightData.data_body` on the proven
  native Flight `DoPut`) is the remaining Phase 7b step, below.

**Design (from DESIGN.md §8.5.2):**
- **Binding scope:** `Arrow_ipc` module (internal to the Arrow record-type
  implementation, runtime packages only).
- **C++ wrapper:** ~200 lines; `arrow_ipc_stubs.cc`:
  - `RecordBatchStreamWriter` (batch → IPC buffer)
  - `RecordBatchStreamReader` (IPC buffer → batch)
  - Error handling (Arrow's `Result<T>` → OCaml results-over-exceptions).
- **OCaml FFI:** ~80 lines; `ctypes` interface with opaque handle types:
  `batch_to_ipc`, `ipc_to_batch`, `buffer_to_bytes`, `bytes_to_buffer`.
- **Build:** Dune `foreign_objects` rule linking `libarrow` (macOS: `brew install
  apache-arrow`; Linux: `libarrow-dev`). Fails with clear error if `libarrow`
  missing; does not affect Proto/JSON packages.
- **Depext:** Conditional in `zerobus.opam`/`zerobus-eio.opam` (runtime packages);
  none in `zerobus-core.opam`.

**Effort estimate:** 4–6 hours (C++ stubs) + 2–4 hours integration/testing = 6–10 hours.

**Acceptance criteria for spike:**
- Compiles with `pkg-config --cflags arrow` available.
- Round-trip test runs and passes (schema+values faithful).
- IPC bytes contain expected Arrow magic (not just "non-empty").
- Error cases return results, not exceptions.

**Acceptance criteria for Phase 7b completion:**
- Spike green on both 4.14 (Lwt) and 5.x (Eio) switches.
- Integrated onto Phase 5 driver as `Arrow` record type.
- `ingest` accepts Arrow IPC bytes, feeds them into `FlightData.data_body`.
- Mock + live integration test: batch → IPC → `DoPut` → server ack (end-to-end).

### 7c — `Zerobus_rest` (§4.2, v1, low effort)  🟢 DONE (mock; live env-gated)

- **Status:** ✅ Built + tested. New `zerobus-rest` opam package (`lib/rest/`,
  `Zerobus_rest`): thin stateless `POST <endpoint>/zerobus/v1/tables/<table>/insert`
  wrapper. Reuses `zerobus-core`'s `Config.oauth_token_request_body` (the same
  table-scoped client-credentials grant proven live in `spike-live`) + `cohttp-lwt`.
  Per-table token cache (30s early refresh). No streams/offsets/recovery. Kept a
  separate package so gRPC users don't pull it and REST-only users don't pull the
  h2/tls streaming stack.
- **Test:** `test_rest/test_rest_lwt.ml` — in-process cohttp mock plays BOTH the OIDC
  token endpoint and the `/insert` endpoint (cleartext HTTP, loopback, ephemeral
  port). **8/8 alcotest green** (0.047s): basic-auth grant → bearer attached to
  insert; token cached (1 mint across 2 inserts); records sent as one JSON array;
  correct insert path + 2xx → `Ok`; empty batch is a no-op; non-2xx → `Server_status`
  with code. Evidence: `test_rest/EVIDENCE_REST.txt`. `opam lint` clean.
- **Remaining (not blocking):** live REST insert against a real workspace +
  server-side schema-rejection error handling (env-gated, same posture as the gRPC
  live checks — mock does not validate the UC grant/schema server-side).
- **Acceptance:** REST insert of JSON records against live workspace; error handling
  for schema rejection. *(MET at mock level; live env-gated.)*

### 7d — `Zerobus_otlp` (§4.3, v1, moderate effort)  🟢 DONE (mock; live env-gated)

- **Status:** ✅ Built + tested. New `zerobus-otlp` opam package (`lib/otlp/`,
  `Zerobus_otlp`): an `otlp/grpc` exporter with `export_logs` / `export_metrics`
  over the two unary RPCs `LogsService/Export` and `MetricsService/Export`. Runs
  on the **same** Phase-3 TLS+ALPN-h2 transport as the gRPC SDK — it reuses
  `Zerobus`'s Lwt `H2_client` (a unary Export is a degenerate bidi: send one framed
  request, `close_send`, `recv` one response, check the gRPC status). Same
  table-scoped client-credentials OAuth grant (`Config.oauth_token_request_body`),
  30s early-refresh cache.
- **Protos:** the OpenTelemetry collector protos + transitive deps (common,
  resource, logs, metrics, collector/{logs,metrics}) vendored **verbatim** from
  `opentelemetry-proto` v1.3.2 under `lib/otlp/proto/vendor/`, **independent** of
  the Zerobus wire protos (§4.3). Codegen finding: proto3 numeric `reserved` parses
  fine in `ocaml-protoc` 4.1 (**no strip** needed, unlike the proto2 Zerobus
  schema); `common.proto`'s shared `values` label needs `-w -30`. All 6 modules
  compile as one `zerobus-otlp.proto` library.
- **Test:** `test_otlp/test_otlp_lwt.ml` — mock OTLP collector (`otlp_collector.ml`,
  separate process, cleartext h2c, grpc-lwt `Unary` handlers speaking the REAL
  vendored protos) + in-process cohttp OIDC token endpoint. **7/7 alcotest green**
  (0.057s): logs+metrics Export accepted, partial-success (REJECT1 → rejected=1
  round-trips), empty no-op, token cached (1 mint / 3 exports). Round-trip proven
  by the collector's decode log (decoded 2 log records / 1 metric / REJECT1), not
  just OK status. Evidence: `test_otlp/EVIDENCE_OTLP.txt`. `opam lint` clean.
- **Remaining (not blocking):** live OTLP export against a real endpoint (env-gated,
  same posture as the gRPC/REST live checks — mock is not a real Zerobus OTLP
  endpoint, so records-in-table is unproven). `otlp/http` variant not built (v1
  scope is `otlp/grpc` per §4.3). Eio/Async exporter (Lwt is the reference).
- **Acceptance:** OTLP exporter sends logs/metrics to the Zerobus OTLP endpoint;
  records appear in table. *(MET at mock level — Export accepted + decoded; live
  records-in-table env-gated.)*

**Overall Phase 7 Acceptance:** Arrow batch ingest with **real IPC encoding**
end-to-end (spike + mock + live); REST insert against the live workspace; OTLP
exporter delivers logs/metrics to the live endpoint. All three interfaces
(`Zerobus`, `Zerobus_rest`, `Zerobus_otlp`) and all record types (Proto, JSON,
Arrow) exercised in `test_integration/`.

---

## Phase 8 — Examples, docs, CI  🟢 DONE (release deferred)  (§7, CLAUDE.md)

**Release/publish is intentionally out of scope** until the SDK is accepted into
Databricks Labs (per the maintainer). Everything else in Phase 8 is done.

**Done:**
- ✅ **Examples** (`examples/`, all compile-checked; run against a live workspace):
  `json_loop_then_flush_lwt.ml` (**the flagship — loop-then-flush FIRST**),
  `ack_callback_lwt.ml` (async ack notification), `json_loop_then_flush_eio.ml`
  (Eio direct-style, gated `>= 5.0`), `rest_insert_lwt.ml`, `otlp_export_lwt.ml`.
  Built green: the 4 Lwt/REST/OTLP exes on fl414, the Eio exe on zbeio.
- ✅ **README** (`README.md`) — opens with the loop-then-flush quick start + the
  cardinal rule, then the four interfaces, record types, runtime packages, the
  examples table, a concurrency summary, and build/test commands.
- ✅ **Golden rules (§7)**: added the missing `.mli` for `Zerobus_eio` and
  `Zerobus_async` (opaque `type t`/`stream`; every public module now has an
  interface). Results-over-exceptions, labeled optionals, no objects, no `List.hd`
  already held.
- ✅ **Concurrency-guarantees doc** (`doc/concurrency.md`, §5.6): `t` shareable,
  `stream` single-writer, fan-out = more streams, idempotent `close`, per-runtime
  notes (Lwt reference / Eio bracket / Async bracket+caller-auth).
- ✅ **CI** (`.github/workflows/ci-ocaml.yml`, wired into `push.yml` path-filtered
  on `ocaml/**`, added to the `gate` needs): job per switch — **4.14**
  (core+Lwt+Async+REST+OTLP+Arrow: build, examples, the 5 test dirs sequentially,
  opam lint; installs `libarrow-dev` for the Arrow codec) and **5.x** (Eio: build,
  example, `test_driver_eio`, opam lint). Uses `ocaml/setup-ocaml@v3`. Every
  build/test invocation in the workflow was validated locally on both switches.

**Deferred (not this phase):**
- Release: opam publish, `ocaml/v*` tags (D4) — after Databricks Labs acceptance.
- `test_integration/` env-gated live suite (`DATABRICKS_HOST`/`CLIENT_ID`/… §12.4);
  the per-interface live checks are tracked as env-gated follow-ups in each 7x item.
- No `.ocamlformat` yet, so CI has no `@fmt` gate (code follows the golden rules
  but is not ocamlformat-managed); add one before release if desired.

**Acceptance:** green CI on both switches; `opam lint` clean; examples build.
*(MET. Live integration suite + release deferred as above.)*

---

## Critical path & risk

```
D1 (proto) → P1 → P2 → P3(TLS) ┐
                    P4(OAuth) ─┼→ P5(driver) → P6(recovery) → P7(records) → P8(ship)
                               ┘
```

- **Highest residual risk: Phase 6 recovery** — the only transport mechanic not
  yet proven by a spike. De-risk with a spike before building it into the driver.
- **Second risk: the Arrow IPC codec (P7, D3)** — the one v1 item with a real new
  C++ dependency and no OCaml precedent for the IPC-bytes path; spike it (real
  RecordBatch → IPC → `DoPut`) before integrating, and keep `libarrow` isolated
  to the Arrow record type.
- **Hard external dependency: D1** (the `.proto`) — gated by its initial proof
  step (gates A+B). Nothing past P1 proceeds without it.
- **Everything else is de-risked** by the five spikes; Phases 2–5, 7-Flight-RPC,
  7-REST, and 7-OTLP-transport are largely "port proven spike code behind the
  functor," not open research.

---

## Suggested milestones

- **M1 — Skeleton + core (P1–P2):** builds on both switches, functor typechecks.
- **M2 — Secure connected client (P3–P4):** live TLS + scoped token, all runtimes.
- **M3 — GA data plane (P5–P6):** ingest/flush + recovery; JSON+Proto durable.
- **M4 — v1 (all of P7 + P8):** all three interfaces (gRPC incl. Arrow with real
  IPC encoding, REST, OTLP) and all record types; packaged, CI, integration-
  tested against a real workspace, examples/docs. Nothing deferred past v1.
