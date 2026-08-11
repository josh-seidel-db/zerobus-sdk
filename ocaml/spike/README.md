# Transport-risk spike: client-initiated bidirectional gRPC streaming

**Purpose.** De-risk the single biggest technical unknown in `DESIGN.md` (§8.7):
*can the OCaml gRPC stack sustain a long-lived, client-initiated **bidirectional**
streaming RPC where the client pushes many requests while concurrently draining
acknowledgments?* This is exactly the shape of the Zerobus ingest hot path
(`ingest` in a loop, acks arriving out of band, `flush` at the end), and it is
the **least-exercised** corner of `grpc-lwt`/`grpc-eio` — most examples in the
wild are unary or server-streaming.

TLS, ALPN `h2`, and unary gRPC are known-good; this spike deliberately does **not**
re-test those. It isolates the risky bit: **bidi client streaming with
interleaved acks**.

## RESULT: PASS (verified, not asserted)

This spike was **compiled and run** on `grpc-lwt 0.2.0` + `h2 0.12.0` (OCaml
4.14.1, dune 3.22.2, macOS/arm64). All four tests pass, and the measured facts
are captured in [`EVIDENCE.txt`](EVIDENCE.txt) (regenerate with `dune runtest
--force`). Headline numbers from a run:

```
records_sent           : 200
acks_received          : 200
sends_done_at_first_ack: 3 (of 200)   <- first ack arrived after only 3 sends
offsets_contiguous     : true
```

The `sends_done_at_first_ack` value (3–8 across runs, varying with the
scheduler) is the key datum: acks stream back **while the client is still
sending**, so `grpc-lwt`'s bidi client API genuinely supports concurrent
send/receive on one stream. **Decision: GO on the native `grpc-lwt` path** — the
raw-h2 fallback below was not needed, but is kept (and self-tested) in case a
future version regresses.

### API drift the spike surfaced (the other reason it exists)

Compiling against the real libraries corrected several guesses in the design
sketch — exactly what a spike is supposed to catch before the SDK is built on
them:

- Client bidi handler is `~f:((string option -> unit) -> string Lwt_stream.t -> 'a Lwt.t)`
  — `write (Some s)` sends, `write None` half-closes. (Not a polymorphic-variant
  `` `Send``/`` `Close`` push.)
- Server bidi handler is `string Lwt_stream.t -> (string -> unit) -> Grpc.Status.t Lwt.t`,
  registered as a `Grpc_lwt.Server.Rpc.Bidirectional_streaming` variant and wired
  via `Service.v |> add_rpc |> handle_request` then `Server.v |> add_service`.
- `Grpc_lwt.Client.call` returns `('a * Grpc.Status.t, H2.Status.t) result Lwt.t`
  and takes `~do_request:(H2_lwt_unix.Client.request connection ~error_handler)`.
- `ocaml-protoc` needs `--make` to emit `make_ingest_request ?payload ?client_seq ()`
  builders; the record type is `private` with a `_presence` bitfield.
- The generated module is exposed as `Ingest_proto.Ingest` (wrapped library),
  not a bare `Ingest`.

These are now reflected in `bidi_spike.ml`; `DESIGN.md` §6.2's `Grpc_transport.S`
should adopt the real `grpc-lwt` shapes when the driver is built.

### Scope of this evidence (what it does and does NOT prove)

To keep the record honest, the Lwt spike proves **bidi transport mechanics on an
in-process loopback socket** and nothing more. It specifically does **not**
exercise:

- **TLS / ALPN `h2`** — the exchange is cleartext h2c on loopback (`~scheme:"http"`).
- **Real OAuth / scoped `authorization_details` tokens** — no auth headers.
- **Mid-stream reconnect / recovery** — the happy path only; no fault injection.
- **The actual Zerobus service** — the server is an in-process mock, not Databricks.
- **OCaml 5.x / Eio** — this ran on **OCaml 4.14.1 / Lwt only**.

(Unary-over-TLS with ALPN is separately known-good in the ecosystem; it is not
the risky part and was intentionally out of scope here.) The Eio 5.x
instantiation is proven by the **companion spike** below.

---

## Eio (OCaml 5.x) companion spike — `bidi_spike_eio.ml`

`DESIGN.md` §6.3 ships a second headline package (`zerobus-eio`) on the Eio
runtime, which only exists on OCaml 5.x. Because Eio is **direct-style**
(effects, not a monad), it is the runtime most likely to break the concurrency
model — finding #1/#2 in the design review were entirely about Eio. So it gets
its own compiled-and-run spike proving the *same three properties* on
`grpc-eio` + `h2-eio` under OCaml 5.x.

The Eio spike lives in `../spike-eio/` (its own dune-project, because it needs a
different opam switch — OCaml 5.x with `grpc-eio`/`h2-eio` — than the Lwt spike's
4.14 switch). It uses the same `ingest.proto` and the same three assertions,
written direct-style: fibers under a `Switch`, `Eio.Fiber.both` for the
concurrent send/ack pair, no `Lwt.t`.

**RESULT: PASS (verified, compiled and run).** OCaml 5.2.0, `grpc-eio 0.2.0`,
`h2-eio 0.12.0`, `eio 1.3`. 200 sent, 200 acked, first ack at send **#10–13 of
200** (interleaved), offsets 0–199 contiguous. Evidence:
[`../spike-eio/EVIDENCE_EIO.txt`](../spike-eio/EVIDENCE_EIO.txt); regenerate with
`dune runtest --force` on the 5.x switch. This confirms the `IO`-functor
abstraction (§6.2) is realizable on *both* headline runtimes — the whole premise
of the two-package split.

#### Eio finding: `grpc-eio 0.2.0` client omits an h2 flush → in-process deadlock

Getting the Eio spike to pass surfaced a real, precisely-located issue, worth
recording because it shapes how the `zerobus-eio` package must be built and
tested:

- **Symptom.** Running the grpc-eio **client and server in the same process**
  (even across separate Eio *domains*) deadlocks. Tracing showed the server RPC
  handler is entered but its request `Seq` never yields the first message; the
  client writes all records and closes, but the bytes never reach the wire.
- **Root cause (confirmed in the library source).** In
  `grpc-eio/connection.ml`, the client sender `grpc_send_streaming_client` calls
  `H2.Body.Writer.write_string` per message with **no `H2.Body.Writer.flush`** —
  whereas the *server* sender (`grpc_send_streaming`) flushes after every write.
  Upstream's own `master` HEAD is a commit titled *"flush body writer before
  closing"* (unreleased; opam only has 0.2.0), and even it only fixes the Lwt
  side. So with a single shared event loop the client's buffered request bytes
  are never pumped to the socket. This affects **unary and bidi alike** (unary is
  built on the bidi primitive) — a minimal in-process unary call hangs too.
- **Resolution / proof.** The same client and server **in separate OS processes**
  interoperate perfectly (verified independently: a standalone unary call and a
  standalone 50-record bidi call both complete). So the Eio spike spawns the
  server as a subprocess (`ingest_server_eio.exe`, OS-assigned port reported via
  a `READY <port>` line) and runs the client in the test process. Separate
  processes is also the *real* Zerobus topology, so this is not a workaround that
  hides a real defect — the client-and-server-in-one-process case simply never
  occurs for the actual SDK.
- **Second gotcha.** The client must call `H2_eio.Client.shutdown` after the
  call, or the `Switch` never resolves and `Eio_main.run` won't return.
- **Implication for `zerobus-eio`.** When the SDK's `Grpc_transport.S` is built
  on grpc-eio, either (a) depend on a grpc-eio version whose client path flushes,
  (b) carry a small vendored/patched transport adding the client-side flush, or
  (c) drive the h2 writer directly (the raw-h2 fallback). Track the upstream fix;
  pin until it is released.

## What it proves

The spike stands up an **in-process** mock ingest server and a client, connected
over a real HTTP/2 (`h2`) loopback socket, and asserts three properties that the
real SDK depends on:

1. **All N records get acknowledged** — send half and receive half both complete.
2. **Acks are interleaved with sends** (true concurrency): the client observes at
   least one ack *before* it has finished sending all N records. If the runtime
   forced send-then-receive sequentially, this assertion fails — which is exactly
   the bug that finding #1 in the design review was about (eager `both`/thunking).
3. **Per-stream ordering** is preserved: ack offsets arrive monotonically.

No Databricks credentials, no network egress — it runs entirely on localhost, so
it belongs in CI.

## Layout

```
spike/
├── README.md            # this file
├── dune-project
├── proto/
│   └── ingest.proto     # minimal Zerobus-shaped bidi service (Ingest.Ingest)
├── dune                 # ocaml-protoc codegen rule + the spike executable/test
└── bidi_spike.ml        # in-process server + client + alcotest assertions
```

## Running it

```bash
cd ocaml/spike
opam install grpc-lwt h2-lwt-unix ocaml-protoc lwt alcotest   # one-time
dune runtest
```

Expected output (abbreviated):

```
Testing bidi-transport-spike.
  bidi streaming    all records acked          [OK]
  bidi streaming    acks interleave with sends [OK]
  bidi streaming    offsets are monotonic       [OK]
```

A green run is the **go** signal for the native gRPC path (design next-step 3).
A red run — or if `grpc-lwt`'s client API can't express "send while receiving" —
triggers the workaround below.

## The risk, concretely

`grpc-lwt`'s client helpers are organized around the four RPC shapes. For
**bidirectional** streaming the client must hold *two* handles to the same HTTP/2
stream at once — a writer (`f_send : request -> unit Lwt.t`, plus a half-close)
and a reader (a `Lwt_stream.t` / pull function of responses) — and pump both
concurrently with `Lwt.join` / `Lwt.both`. The specific failure modes we're
checking for:

- The client API only exposes a *fold over all responses after the request
  stream is closed* (i.e. it won't let you read acks until you've stopped
  sending). That would make interleaving impossible and assertion #2 fail.
- Backpressure / flow-control on the send half deadlocks against the read half
  under `h2`.
- Trailers (`grpc-status`) can't be read cleanly at end-of-stream after a
  long-lived exchange.

## Workaround if the spike fails

If `grpc-lwt` cannot sustain interleaved bidi, we do **not** abandon the native
design — we drop one layer down and **frame gRPC over raw `h2` ourselves**. gRPC's
wire format on top of HTTP/2 is small and fully specified:

- **Request/response body** = repeated `5-byte prefix` + message:
  1 byte compressed-flag, 4 bytes big-endian length, then the protobuf bytes.
- **Headers** we send: `:method POST`, `:path /Ingest.Ingest/IngestStream`,
  `content-type application/grpc+proto`, `te trailers`, plus our auth +
  `x-databricks-zerobus-table-name`.
- **Status** comes back in HTTP/2 **trailers**: `grpc-status` (0 = OK) and
  `grpc-message`.

`h2` already exposes exactly what we need for this: `H2.Client_connection.request`
returns a request body we can `Body.write_string` to repeatedly and then close
(the send half), while the response handler hands us a response body we read
incrementally (the receive half) — two independent handles on one stream, pumped
concurrently. That is strictly more control than `grpc-lwt` gives us, at the cost
of ~150 lines of framing code living in `lib/core/grpc_transport.ml`.

`bidi_spike.ml` includes a second module, `Raw_h2_framing`, sketching this
fallback (the length-prefix codec + the send/recv split) so the workaround is
proven on the same in-process socket, not just asserted in prose. The decision
rule is explicit:

> **If `grpc-lwt` passes all three assertions → use `grpc-lwt` (less code to own).
> If it fails assertion #2 or deadlocks → adopt `Raw_h2_framing` in the core
> transport.** Either way the *rest* of the SDK (the `IO` functor, the send/ack
> fiber pattern, recovery) is unchanged, because both sit behind the
> `Grpc_transport.S` signature.

## Relationship to the design

- Exercises the `Grpc_transport.S` boundary from `DESIGN.md` §6.2 / §9.
- The send-fiber + ack-drain-fiber + mailbox structure here is the prototype of
  the real driver (§6.4).
- Both `grpc-lwt` and `Raw_h2_framing` implement the same signature, so the
  functorized core (`Make(Io)`) is agnostic to which one wins — the point of the
  abstraction.
