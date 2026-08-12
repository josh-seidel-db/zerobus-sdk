# Zerobus SDK for OCaml — Design Document

**Status:** ⚠️ **Historical design proposal (2026-08-08), since IMPLEMENTED.** This
document is preserved as the original design record — it was written before any
code existed and describes what the SDK "would" look like. The SDK has since been
built and exceeds this proposal: all three runtimes (Lwt/Eio/Async) have working
transports with live TLS, built-in OAuth, recovery, Proto/JSON/Arrow encodings,
and the REST + OTLP interfaces — all pure ocaml-tls (the Async OAuth token POST is
a hand-rolled HTTP/1.1 over `Tls_async.connect`, not cohttp-async/OpenSSL). Every
runtime × interface has been verified live against a real workspace, and OTLP
wire-compatibility is proven against the canonical `opentelemetry` opam package's
protos. Some spike-era details below (e.g. gluten-based TLS, "next steps",
per-runtime "deferred" notes) reflect the design-time picture,
NOT the shipped code — for current capabilities and status see [`README.md`](README.md),
[`doc/concurrency.md`](doc/concurrency.md), and [`doc/arch/tls_async_status.md`](doc/arch/tls_async_status.md).
The goals, architecture (runtime-agnostic core + thin runtime packages), and
protocol design in this document remain the accurate description of the SDK's shape.

This document outlines a 100% native OCaml Zerobus Ingest SDK: idiomatic OCaml, no
FFI to the Rust core, supporting both OCaml 4.14 and 5.x through a runtime-abstracted
package split.

**Author:** Josh Seidel
**Date:** 2026-08-08

---

## 1. Goals and non-goals

### Goals

- **100% native OCaml.** Speak the Zerobus gRPC protocol directly from OCaml over
  HTTP/2 + TLS. No `cgo`-style binding, no PyO3/NAPI/JNI, no linking the shared
  Rust core (`rust/sdk`) that every other wrapper SDK in this monorepo wraps.
  The OCaml SDK is a **peer implementation of the protocol**, not a wrapper.
- **Feels like OCaml.** Results over exceptions, opaque `type t`, `.mli` on every
  module, labeled optional arguments, no objects, no global mutable state.
  Mirror the conventions already established in
  [`databricks-sdk-ocaml`](https://github.com/databricks-field-eng/databricks-sdk-ocaml)
  (see §7) so the two SDKs feel like one family.
- **All three Zerobus interfaces** from the
  [Choose an interface](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#choose-an-interface)
  docs: gRPC SDK (primary), REST, and OTLP (see §4).
- **All record encodings:** Protobuf (production default), JSON (schemaless), and
  Arrow Flight (beta, columnar/batch).
- **Correct ingestion ergonomics.** The API must make the *loop-then-flush*
  pattern the obvious one and per-record waiting the awkward one — the cardinal
  Zerobus performance rule (see §5).
- **OCaml 4.14 and 5.x both first-class**, and all three concurrency runtimes
  (Lwt, Eio, Async) shipped as release packages over one shared codebase (see §6).

### Non-goals

- Not wrapping the Rust core. (Explicitly rejected — see §3.1.)
- Not building a general Databricks REST client — that is
  `databricks-sdk-ocaml`'s job. We *depend on* its auth/config concepts (or the
  library itself) rather than reimplement OAuth discovery.
- Not the Zerobus control plane / table management. Tables must pre-exist
  (Zerobus does not auto-create them).

---

## 2. What Zerobus is (grounding the design)

From the
[Zerobus overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
and [ingest docs](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest):

- **Push-based ingestion** that writes directly into Unity Catalog Delta tables.
  Serverless — no brokers, no partitions.
- **Stream** = a direct client↔server connection targeting **one** table, with
  per-stream ordering guarantees. More streams = more tables or more throughput.
- **Server** validates each record against the target table schema, materializes
  it in Delta, and returns a **durability acknowledgment**. Records that fail
  schema validation are rejected (and dropped to
  `_zerobus/table_rejected_parquets/` in table storage).
- **Offsets:** each ingested record gets a monotonically increasing offset per
  stream. The offset is a *handle* — you can wait on it later.
- **Acknowledgments** are asynchronous and pipelined. Sending and acking happen
  in the background; `ingest` returns as soon as the record is queued.
- **Recovery:** streams transparently recover from retryable failures
  (reconnect, replay un-acked records) up to a configured retry budget.
- **Auth:** OAuth 2.0 client-credentials (service principal) with UC table
  privileges. A bearer token plus a table-name header on the stream.

The key data-plane concepts an OCaml API must model: **SDK handle → Stream →
(ingest record → offset) → wait/flush/ack-callback → close**, plus stream
**configuration** and **recovery**.

---

## 3. Native-vs-FFI decision

### 3.1 Why native, not a binding to the Rust core

The monorepo's other non-Rust SDKs (Go, C++, Java, Python, TS) all bind the
single Rust core across an FFI boundary. An OCaml binding *could* do the same
via the C FFI layer (`rust/ffi/zerobus.h`) using OCaml's C stubs or `ctypes`.
The user asked specifically for **100% native**, and it is also the better fit
for OCaml, for concrete reasons:

| Concern | FFI-to-Rust | Native OCaml |
|---|---|---|
| Build story | Must ship/compile a per-platform static lib; cross-compilation and macOS runners are already a known pain in this repo | Pure opam install, `dune build`, works everywhere OCaml + the HTTP/2 stack builds |
| OCaml 4.14 **and** 5.x | FFI works on both but the runtime bridging (blocking Rust tokio vs. OCaml scheduler) is awkward and identical work regardless | Runtime abstraction is native and clean (see §6) |
| Memory safety | Manual: opaque pointers, explicit free, GC-vs-Rust-ownership hazards (the exact class of bug the repo's CLAUDE.md warns about for Go pinning / cgo.Handle) | GC-managed; no manual free, no pinning |
| Effects/`eio` | Blocked on Rust runtime; effects can't cross FFI cleanly | Native fibers/effects supported directly |
| Debuggability | Stack traces stop at the FFI wall | Pure-OCaml stack traces end to end |

The cost of native is that we re-implement gRPC framing, recovery, and the OAuth
token dance in OCaml — but the OCaml gRPC/HTTP-2 ecosystem is now mature enough
to carry that (see §8), and the protocol surface Zerobus needs is small
(essentially one bidirectional streaming RPC plus token auth).

### 3.2 What "native" concretely requires

1. **HTTP/2 transport** with **ALPN `h2`** and TLS.
2. **gRPC** framing over that HTTP/2 stream (length-prefixed protobuf messages,
   `grpc-status` trailers).
3. **Protobuf** encode/decode for the Zerobus wire messages *and* for user
   records (Proto record type).
4. **OAuth2 client-credentials** token acquisition + refresh.
5. A **bidirectional streaming** RPC driver that pipelines sends and consumes
   acks concurrently — the heart of the SDK.

---

## 4. The three interfaces ("choose an interface")

The docs describe three ways to reach Zerobus. The OCaml SDK exposes all three,
but with clearly different weight — gRPC is the real SDK; REST and OTLP are
thin, optional helpers.

### 4.1 gRPC streaming SDK — `Zerobus` (primary)

The high-throughput, persistent-connection interface. This is 90% of the design
(§5). Supports Proto / JSON / Arrow-Flight record types. This is what
"the OCaml Zerobus SDK" means.

### 4.2 REST — `Zerobus_rest` (optional sub-library)

Stateless request/response for low-frequency / edge cases (the docs' "throughput
tax" tradeoff). One `POST` per batch:

```
POST <endpoint>/zerobus/v1/tables/<catalog>.<schema>.<table>/insert
Authorization: Bearer <oauth-token>
Content-Type: application/json
[ {record}, {record}, ... ]
```

Tiny module — just OAuth + an HTTP client + JSON array serialization. No streams,
no offsets, no recovery. Reuses `databricks-sdk-ocaml`'s HTTP client + auth
rather than adding new deps.

```ocaml
val Zerobus_rest.insert :
  t -> table:string -> Yojson.Safe.t list ->
  (unit, Error.t) result io
```

### 4.3 OTLP — `Zerobus_otlp` (optional sub-library)

For callers already emitting OpenTelemetry traces/logs/metrics. OTLP has **two**
transports — `otlp/grpc` and `otlp/http` (protobuf or JSON over HTTP/1.1) — and
its own service protos (`opentelemetry.proto.collector.{logs,metrics,trace}.v1`),
which are separate from Zerobus's own wire protos and must be vendored
independently. The `otlp/grpc` variant can share the §4.1 h2+TLS transport; the
`otlp/http` variant just needs the plain HTTP client from `Zerobus_rest` (§4.2).
This is *not* a trivial reuse of §4.1 — it is a distinct exporter with its own
proto surface. Lowest priority; ship after the core lands. Realistic v1 scope: an
`otlp/grpc` `LogsService`/`MetricsService` exporter pointed at the Zerobus OTLP
endpoint.

### 4.4 Decision matrix (mirrors the docs)

| Interface | Module | Throughput | Latency | Use when |
|---|---|---|---|---|
| gRPC streaming | `Zerobus` | Very high | Low | High-volume streams, long-lived producers |
| REST | `Zerobus_rest` | Low | High | Infrequent/edge device reporting, webhooks |
| OTLP | `Zerobus_otlp` | High | Low | Already instrumented with OpenTelemetry |

---

## 5. The gRPC SDK API surface

### 5.1 Type overview

```ocaml
(* Top-level handle: holds endpoint, workspace URL, OAuth config, connection
   pool. Immutable outside its synchronized token cache. *)
type t

(* A single stream to one table. Opaque; owns the bidi RPC + recovery state. *)
type stream

(* Per-record handle returned by ingest. Cheap; wraps the assigned offset. *)
type offset = private int64

type record_type = Proto | Json | Arrow

type table_properties = {
  table_name : string;                 (* catalog.schema.table *)
  descriptor : descriptor option;      (* required for Proto, None for JSON *)
}

(* See §5.5. A typed wrapper over the serialized proto descriptor the server
   needs, so callers can't confuse a DescriptorProto with a FileDescriptorSet. *)
type descriptor

type stream_options = {
  record_type : record_type;                 (* default: Proto *)
  max_inflight_requests : int;                (* default: 1_000_000 *)
  recovery : bool;                            (* default: true *)
  recovery_timeout_ms : int;                  (* default: 15_000 *)
  recovery_backoff_ms : int;                  (* default: 2_000 *)
  recovery_retries : int;                     (* default: 4 *)
  server_lack_of_ack_timeout_ms : int;        (* default: 60_000 *)
  flush_timeout_ms : int;                     (* default: 300_000 *)
  ack_callback : ack_callback option;         (* default: None *)
}

type ack_callback = {
  on_ack   : offset -> unit;
  on_error : offset -> string -> unit;
}
```

These defaults are lifted directly from the Go SDK's
`DefaultStreamConfigurationOptions` so behavior matches across the family.

### 5.2 Construction and streams

```ocaml
val create :
  ?application_name:string ->
  endpoint:string ->          (* https://<shard>.zerobus.databricks.com *)
  workspace_url:string ->
  unit -> (t, Error.t) result io

val create_stream :
  t ->
  table_properties ->
  client_id:string ->
  client_secret:string ->
  ?options:stream_options ->
  unit -> (stream, Error.t) result io

(* Custom auth (e.g. federation) — mirrors Go's HeadersProvider. *)
val create_stream_with_headers :
  t -> table_properties ->
  headers_provider:(unit -> (headers, Error.t) result io) ->
  ?options:stream_options -> unit -> (stream, Error.t) result io

val close : stream -> (unit, Error.t) result io
```

### 5.3 The three ingestion styles (matching the docs)

The docs enumerate three SDK ingestion methods: **offset-based**,
**callback-based (async)**, and **fire-and-forget** (with a legacy per-record
future). OCaml expresses all three:

**(a) Offset-based — the default, correct pattern:**

```ocaml
val ingest         : stream -> bytes  -> (offset, Error.t) result io
val ingest_records : stream -> bytes list -> (offset, Error.t) result io  (* batch, hot path *)
val wait_for_offset: stream -> offset -> (unit, Error.t) result io
val flush          : stream -> (unit, Error.t) result io
```

Canonical usage — queue in the loop, wait **once**:

```ocaml
let* () = Lwt_list.iter_s (fun r -> ingest stream r >|= ignore) records in
let* () = flush stream in
```

**(b) Callback-based (async acks):** register `ack_callback` in `stream_options`;
`on_ack`/`on_error` fire from the ack-reader fiber. Best for continuous streams.

**(c) Fire-and-forget:** just `ingest` and never wait (rely on `close`/periodic
`flush`). The offset return is ignored.

> **Deliberately awkward:** there is no `ingest_and_wait`. Per-record durability
> is `ingest` immediately followed by `wait_for_offset` — two calls, so the
> round-trip cost is visible in the source. This encodes the repo's cardinal
> rule: **never wait for an ack after every ingest.** README/examples lead with
> loop-then-`flush`.

### 5.4 Errors

Reuse the `databricks-sdk-ocaml` `Error.t` shape, extended with a retryable flag
so callers can distinguish transient (SDK auto-recovers) from fatal:

```ocaml
type Error.t =
  | Stream_error of { message : string; retryable : bool }
  | Auth_error of string
  | Config_error of string
  | Transport_error of string      (* HTTP/2 / TLS *)
  | Timeout_error of string        (* ack / flush / recovery *)
  | Serialization_error of string  (* proto / JSON / arrow *)
  | Schema_rejected of string      (* server rejected record vs table schema *)
```

### 5.5 Proto descriptors (the `descriptor` type)

For the **Proto** record type the server must know the message schema, supplied
as a serialized protobuf descriptor on stream creation (mirrors the Go SDK's
`TableProperties.DescriptorProto`). There are two distinct protobuf messages in
play and it is easy to conflate them, so the SDK models the distinction in the
type system rather than accepting a bare `bytes`:

- **`DescriptorProto`** — describes *one* message type. This is what the Zerobus
  wire protocol ultimately carries.
- **`FileDescriptorSet`** — a self-contained bundle of `.proto` files (a message
  plus everything it transitively imports). This is what tooling (`protoc
  --descriptor_set_out`, `ocaml-protoc`) naturally emits, and what a user is most
  likely to have on hand.

The abstract `descriptor` type (from §5.1) is constructed through explicit
smart constructors, so the caller states which artifact they have and the SDK
extracts/validates the single `DescriptorProto` the server needs:

```ocaml
module Descriptor : sig
  type t = descriptor

  (* From a generated module: `ocaml-protoc` emits, per message, a function
     returning that message's descriptor bytes. This is the ergonomic path for
     users who compile their .proto with the SDK's toolchain. *)
  val of_message_descriptor : bytes -> t

  (* From a full FileDescriptorSet (e.g. `protoc --descriptor_set_out`,
     or `buf build -o`), naming which top-level message is the row type.
     The SDK resolves imports and pulls out that message's DescriptorProto. *)
  val of_file_descriptor_set : bytes -> message_name:string -> (t, Error.t) result

  (* Escape hatch: caller already has the exact serialized DescriptorProto. *)
  val of_descriptor_proto_bytes : bytes -> t
end
```

This resolves the earlier inconsistency (§5.1 said `DescriptorProto`, §8.2 said
`FileDescriptorSet`): **both are accepted, via distinct constructors, and the
type prevents mixing them up.** JSON and Arrow record types pass
`descriptor = None`.

### 5.6 Concurrency guarantees

The monorepo's CLAUDE.md documents each SDK's thread-safety contract (Go: safe
for concurrent goroutines; Java: not thread-safe, external sync required; Python:
async safe, sync single-threaded per instance). The OCaml SDK states its own:

- **`t` (the SDK handle) is safe to share.** It is immutable apart from a
  mutex-guarded OAuth token cache; creating streams from one `t` concurrently is
  fine.
- **A `stream` is single-writer.** `ingest`/`ingest_records`/`flush` on one
  stream must come from a single logical writer — one Lwt promise chain, one Eio
  fiber, or one Async job. This matches Zerobus's per-stream ordering guarantee:
  interleaving writers would make offset ordering meaningless. Internally the
  send side and the ack-reader daemon *are* concurrent (that's the whole design),
  but they are the SDK's fibers, coordinated through the mailbox — not the
  caller's.
- **Fan-out = more streams, not more writers per stream.** To parallelize, open
  multiple streams (the documented Zerobus throughput lever) and write to each
  from its own fiber. Streams are cheap-ish but count against connection quotas
  (the docs' "connection tax"), so this is a deliberate tradeoff, not free.
- **`close` is idempotent and cancels the stream's scope** (§6.2); calling
  `ingest` after `close` returns `Error (Stream_error …)` rather than raising.

If genuinely concurrent writers to one logical table are needed, callers
serialize behind their own mutex or shard across streams — the SDK does **not**
add an internal lock on the hot `ingest` path (it would tax the common
single-writer case). This is stated in the README and the `stream` doc comment.

---

## 6. Runtime abstraction: Lwt vs Eio vs Async, and 4.14 vs 5.x

This is the crux the user called out. The problem: `Lwt`, `Async`, and `Eio` are
three incompatible concurrency runtimes; `Eio` needs OCaml **5.x** effects and
does not exist on 4.14; `Lwt`/`Async` run on both. We want one codebase, two
release packages (4.14 and 5.x), and idiomatic feel on each.

### 6.1 Precedent from `databricks-sdk-ocaml`

That SDK is currently **Lwt-only** (`cohttp-lwt-unix`, returns `_ Lwt.t`
everywhere; CLAUDE.md's "Async & I/O Layer" rules say "all network calls return
`Lwt.t`"). It uses a **pluggable `transport` record** in `http_client.mli` — a
first hint of the right abstraction (inject the effectful function), but it is
not yet runtime-generic. We take that idea further with a functor.

### 6.2 The `IO` signature (functorization)

Define the minimal effect surface the SDK needs and functorize the whole SDK
over it — the standard OCaml answer to "abstract over the scheduler."

Two subtleties drive the exact shape of the signature, and both come from the
fact that our core is *concurrent bidirectional streaming*, not request/response:

- **Concurrency combinators must take thunks, not values.** In a direct-style
  runtime (Eio) `'a t` is a bare `'a`, so an *already-evaluated* argument has
  already run — sequentially. If `both`/`fork` took `'a t` values, Eio would run
  the send-loop to completion *before* the ack-loop ever started, defeating the
  entire point. Every combinator that must introduce concurrency therefore takes
  a suspended `unit -> 'a t`, so the runtime — not the call site — decides when
  each side runs. (Lwt/Async don't strictly need the thunk, since their `'a t` is
  already a suspended computation, but accepting the thunk is harmless there and
  keeps one signature for all three.)
- **Background fibers need a scope.** Eio forbids unscoped background fibers:
  `Fiber.fork` requires a `Switch.t`. So the *core* — not the façade — must own a
  scope, and `fork_daemon` forks the ack-reader into that scope. Structured
  concurrency is a property of the streaming driver itself; a `stream`'s scope
  opens at `create_stream` and is cancelled/closed at `close`. On Lwt/Async the
  scope is a lightweight cancellation token wrapping `Lwt.async` / `Deferred`.

```ocaml
module type IO = sig
  type 'a t                                   (* monad (Lwt/Async) or 'a (Eio) *)
  val return : 'a -> 'a t
  val bind   : 'a t -> ('a -> 'b t) -> 'a t

  (* A concurrency scope. Owns background fibers and their cancellation.
     Eio: wraps Switch.t. Lwt/Async: a cancellation token + fiber registry. *)
  module Scope : sig
    type t
    (* Run [f] with a fresh scope; when [f] returns (or raises) every daemon
       forked into the scope is cancelled and joined before [with_scope]
       returns. This is the structured-concurrency boundary. *)
    val with_scope : (t -> 'a io) -> 'a io
  end
  (* thunked so the runtime — not the caller — sequences the two sides *)
  val both : (unit -> 'a t) -> (unit -> 'b t) -> ('a * 'b) t
  (* fork a long-lived background fiber INTO a scope (the ack reader).
     Never unscoped — satisfies Eio's Switch requirement. *)
  val fork_daemon : Scope.t -> (unit -> unit t) -> unit

  module Mutex   : sig type t val create : unit -> t
                       val with_lock : t -> (unit -> 'a t) -> 'a t end
  (* Bounded, blocking mailbox between the send side and the ack reader.
     Renamed from `Stream` to avoid colliding with the Zerobus `stream` type. *)
  module Mailbox : sig type 'a t val create : capacity:int -> 'a t
                       val put  : 'a t -> 'a -> unit t   (* blocks when full *)
                       val take : 'a t -> 'a option t    (* None once closed & drained *)
                       val close : 'a t -> unit end

  (* HTTP/2 client capable of a bidi gRPC stream, over TLS with ALPN h2. *)
  module H2_client : Grpc_transport.S with type 'a io = 'a t
  val sleep : float -> unit t                 (* recovery backoff *)
end
where "io" abbreviates the enclosing module's [t].
```

The core SDK becomes a functor. Every stream operation runs inside the stream's
`Scope`, opened by `create_stream` and torn down by `close`:

```ocaml
module Make (Io : IO) : sig
  type t
  type stream
  val create : ... -> (t, Error.t) result Io.t
  val create_stream : ... -> (stream, Error.t) result Io.t  (* opens Scope, forks ack reader *)
  val ingest : stream -> bytes -> (offset, Error.t) result Io.t
  val close : stream -> (unit, Error.t) result Io.t         (* cancels + joins Scope *)
  (* ... entire §5 API, with `io` = `Io.t` ... *)
end
```

Then thin per-runtime packages instantiate it:

```ocaml
(* zerobus       *) module Zerobus = Zerobus_core.Make (Zerobus_io_lwt)
(* zerobus-eio   *) module Zerobus = Zerobus_core.Make (Zerobus_io_eio)
(* zerobus-async *) module Zerobus = Zerobus_core.Make (Zerobus_io_async)
```

`bind`/`return`/`both`/`fork_daemon`/`Mailbox`/`Scope` cover exactly what the
bidi streaming driver needs: one fiber pushing records, one daemon draining acks
into a mailbox, a scope that cancels both on `close`, and a mutex around the
token cache. Nothing runtime-specific leaks into the core.

> **Eio nuance (corrected).** Eio is direct-style (effects), not monadic. We
> model `'a t = 'a` and `bind f x = f x`, but because `both`/`fork_daemon` take
> **thunks** and fork **into a `Scope`** (Eio's `Switch`), concurrency and
> structured cancellation are preserved — an eager `'a t = 'a` with value-taking
> combinators would *not* work (it would run each side sequentially and can't
> satisfy Eio's switch requirement). On top of the functor, direct-style callers
> get a thin `Zerobus_eio` façade that returns bare values (`'a` instead of
> `'a t`), takes `env`/`~sw`, and threads the caller's `Switch` into the core
> `Scope`. So Eio users are not forced to write monadic code, and the SDK never
> leaks a background fiber past the caller's switch.

### 6.3 Package / version split

Two *release* packages as requested, plus the shared core and per-runtime shims —
all one repo, one `dune-project`:

| opam package | OCaml constraint | Runtime | Depends on |
|---|---|---|---|
| `zerobus-core` | `>= 4.14` | none (functor + protocol) | h2, ocaml-protoc-*, tls |
| **`zerobus`** (Lwt) | `>= 4.14` | **Lwt** (default) | `zerobus-core`, `grpc-lwt`, `h2-lwt-unix`, `lwt` |
| **`zerobus-eio`** (Eio) | `>= 5.0` | **Eio** | `zerobus-core`, `grpc-eio`, `h2-eio`, `eio` |
| **`zerobus-async`** (Async) | `>= 4.14` | **Async** | `zerobus-core`, `grpc-async`, `h2-async`, `async` |

All three runtime packages are **required, validated v1 scope** (each has a
compiled-and-run transport spike — §6.5).

> **On the version bounds.** "4.14 target" / "5.x target" describes which runtime
> a user on that compiler would *reach for*, not a hard compiler ceiling. `zerobus`
> (Lwt) has **no upper bound** — Lwt builds and runs fine on 5.x, so a 4.14 user
> who later upgrades is never stranded (the whole point of choosing Lwt as the
> conservative default). Only `zerobus-eio` carries a real floor (`>= 5.0`),
> because Eio needs OCaml-5 effects and cannot exist on 4.14.

Rationale for the three runtime packages (all v1):

- **`zerobus` on Lwt.** Lwt is the ubiquitous runtime, it's what
  `databricks-sdk-ocaml` already uses, and it runs on both 4.14 and 5.x — the
  conservative, maximum-compatibility default a 4.14 user reaches for.
- **`zerobus-eio` on Eio.** Eio is the modern OCaml-5 direct-style runtime and the
  reason to be on 5.x at all; it can't exist on 4.14. Gives 5.x adopters the
  idiomatic effects-based experience.
- **`zerobus-async` on Async.** Jane-Street shops standardize their whole stack on
  `core`/`async`; a Zerobus client that speaks `Deferred.t`/`Pipe` natively drops
  into those codebases without an `Lwt`↔`Async` bridge. Now first-class v1 scope,
  proven in §6.5. Runs on both 4.14 and 5.x.

The `IO` functor (§6.2) is what makes three packages cheap: one `zerobus-core`,
three ~small instantiation shims. Dune enforces the OCaml bound per package via
`(enabled_if ...)` and the opam `ocaml` constraint; CI builds a 4.14 switch
(Lwt/Async) and a 5.x switch (Eio/Lwt/Async).

### 6.4 Why a functor over `cohttp`-style pluggable transport alone

`databricks-sdk-ocaml`'s injected-`transport`-record works for *request/response*
REST. Zerobus's core is **bidirectional streaming with concurrent send/ack
fibers**, which needs `fork`, `both`, and a mailbox — control-flow the runtime
must supply, not just an `execute` function. So the transport record is enough
for `Zerobus_rest` (§4.2) but the streaming core needs the full `IO` functor.

### 6.5 Both headline runtimes proven (compiled + run evidence)

Each of the **three** runtime packages has a **compiled-and-run** transport
spike, not just an asserted design. Each pushes 200 records through a
client-initiated bidi stream and measures that acks interleave with sends.
Evidence files are reproducible with `dune runtest --force`.

| Runtime | OCaml | Libraries | Result | Evidence |
|---|---|---|---|---|
| **Lwt** (`zerobus`) | 4.14.1 | `grpc-lwt 0.2.0`, `h2 0.12.0` | ✅ 200/200 acked; 1st ack at send #3–8; offsets contiguous | `spike/EVIDENCE.txt` |
| **Eio** (`zerobus-eio`) | 5.2.0 | `grpc-eio 0.2.0`, `h2-eio 0.12.0`, `eio 1.3` | ✅ 200/200 acked; 1st ack at send #10–13; offsets contiguous | `spike-eio/EVIDENCE_EIO.txt` |
| **Async** (`zerobus-async`) | 4.14.1 | `grpc-async 0.2.0`, `h2-async 0.12.0`, `async v0.16.0` | ✅ 200/200 acked; 1st ack at send #11–14; offsets contiguous | `spike-async/EVIDENCE_ASYNC.txt` |

This confirms the `IO`-functor abstraction (§6.2) is realizable on **all three**
packages the split promises — the core premise of §6.3. The Async spike reused
the Eio finding directly (below): it was built server-in-separate-process from
the start, so the in-process client/server scheduler hazard never bit.

**Scope of the evidence (all three spikes, stated for honesty).** These prove
*bidi transport mechanics on an in-process loopback socket* and nothing more.
They do **not** exercise TLS/ALPN `h2` (cleartext h2c on loopback), real OAuth /
scoped `authorization_details` tokens, mid-stream reconnect/recovery, or the
actual Zerobus service (the server is a mock). TLS/ALPN and scoped OAuth are
instead proven live in `spike-live/` (§12.6, Lwt). Recovery remains a next-step
(§11).

**Eio finding — `grpc-eio 0.2.0` client omits an h2 flush.** Proving the Eio path
surfaced a real, located defect that shapes how `zerobus-eio` must be built:
`grpc-eio`'s client sender (`connection.ml`'s `grpc_send_streaming_client`) calls
`H2.Body.Writer.write_string` per message with **no `flush`**, while the server
sender flushes. With a single shared event loop the client's request bytes never
reach the wire, so an **in-process** client+server (even across Eio domains)
deadlocks — for unary and bidi alike. Across **separate OS processes** (the real
Zerobus topology) it works correctly, which is how the spike is structured
(server spawned as a subprocess). Upstream `master` has an unreleased
*"flush body writer before closing"* commit that only fixes the Lwt side; opam
still ships 0.2.0. **Implication:** when building `zerobus-eio`'s
`Grpc_transport.S`, either depend on a grpc-eio whose client flushes, vendor a
one-line-patched transport (proven sufficient), or drive the h2 writer directly
(the raw-h2 fallback, §8.7). Also: the client must `H2_eio.Client.shutdown` after
the call or the enclosing `Switch` never resolves. Full diagnosis and the
supporting runs are in `spike/README.md` ("Eio finding").

---

## 7. Idiom checklist (inherited from `databricks-sdk-ocaml` CLAUDE.md)

The SDK must obey the same "golden rules" so the two feel like one family:

- Every module has an `.mli`; service handles are opaque `type t`.
- Results over exceptions for expected failures; exceptions only for
  "can't happen." Never exceptions for control flow.
- Labeled optional arguments for config knobs (mirror Python kwargs, type-safe).
- No objects/classes; modules + records only.
- No `List.hd`/`nth`, no `Obj.magic`, no `;;`, no global `open` of big modules.
- Proto/JSON records derive with `{ strict = false }` so unknown server fields
  don't break decode (forward-compat).
- No hand-rolled TLS/HTTP — use established libraries (§8).
- Enums carry an `Unknown of string` fallback variant.
- `dune-project` with `(generate_opam_files true)`; never hand-edit `.opam`.

---

## 8. Dependencies

Legend: **Required** = no realistic alternative; **Choice** = real options exist;
**Optional** = only for a sub-feature.

### 8.1 gRPC + HTTP/2 transport (Required, with a choice of TLS)

| Dep | Role | Status | Notes |
|---|---|---|---|
| `grpc` + `grpc-lwt` / `grpc-eio` / `grpc-async` | gRPC framing over h2, per runtime | **Required** | The dialohq `grpc` stack; runtime backends match §6.3. Actively maintained; Eio branch reported production-used. |
| `h2` + `h2-lwt-unix` / `h2-eio` / `h2-async` | HTTP/2 implementation + runtime adapters | **Required** | anmonteiro's `h2`. `grpc-*` depends on the matching `h2-*`. |
| `gluten` (+ `gluten-lwt`/`-eio`/`-async`) | runtime glue for h2/httpaf | **Required (transitive)** | Pulled in by `h2-*`; the mechanism that lets one protocol impl run on 3 runtimes. |

**TLS backend — CONFIRMED REQUIRED, a choice of backend (ALPN `h2` mandatory):**

| Option | Dep | Pick when |
|---|---|---|
| Pure-OCaml TLS (**recommended default**) | `tls-lwt` / `tls-eio` (mirleft `ocaml-tls`) + `ca-certs` | Portable, no C deps, Mirage-friendly, auditable. `~alpn_protocols` negotiates `h2`. |
| OpenSSL bindings | `lwt_ssl` / `ocaml-ssl` | Need system OpenSSL, hardware crypto, or specific OpenSSL behavior. Supports `set_alpn_protos`. |

> **Confirmed on this machine (§12.1), not assumed:** the installed
> `gluten-lwt-unix 0.5.2` exposes `Client.TLS.create_default ?alpn_protocols`,
> but its `Tls_io` compiled from a **dummy** (`` [ `Tls_not_available ] ``)
> because no TLS backend was present at build time — it links yet cannot
> negotiate TLS. gluten's TLS `depopts` are exactly `tls-lwt` **or** `lwt_ssl`
> (Eio: `tls-eio`/`ocaml-ssl`); installing one and rebuilding gluten swaps in the
> real backend. So a TLS backend is a **required** addition — the one dependency
> the "no new deps" rule cannot avoid — confined to the runtime shim packages
> (`zerobus`/`zerobus-eio`), not `zerobus-core`. Default `ocaml-tls` to avoid a
> C/OpenSSL build dep; `ocaml-ssl` is an opt-in flavor. Full analysis in §12.1.

### 8.2 Protobuf codegen (Required, with a choice of generator)

Needed both for Zerobus's own wire protos and for user Proto records / descriptors.

| Option | Dep | Trade-off |
|---|---|---|
| OCaml-native (**recommended**) | `ocaml-protoc` | OCaml-first generator, no external `protoc` at build time, updated late 2025. Simplest opam-only build. |
| protoc plugin | `ocaml-protoc-plugin` | More idiomatic mappings (oneof → polymorphic variants) but requires Google `protoc` in the build env. |

We also need to supply the message descriptor at stream creation for the Proto
record type (the `descriptor` field, §5.5). `ocaml-protoc` can emit a per-message
`DescriptorProto`; for tooling-produced or dynamic schemas the SDK also accepts a
`FileDescriptorSet` and extracts the named message. Both routes funnel through
the `Descriptor` smart constructors in §5.5, so the two artifact shapes never get
confused.

### 8.3 Auth / config (Choice: reuse vs. minimal)

| Option | Dep | Trade-off |
|---|---|---|
| Reuse (**recommended**) | `databricks-sdk-ocaml` | Get OAuth M2M, `.databrickscfg`, cloud detection, `Config.t`, `Error.t` for free and stay consistent with the sister SDK. Heavier dep. |
| Minimal | `cohttp-lwt-unix`/`cohttp-eio` + `uri` + `base64` + `yojson` | Just the client-credentials token POST. Lighter, but reinvents auth. |

Recommend reusing `databricks-sdk-ocaml`'s auth+config so users configure both
SDKs the same way. Zerobus only needs the OAuth M2M (client-credentials) path
plus a custom-headers provider, so we depend on it narrowly.

**Scoped authorization (important).** Zerobus tokens are not plain workspace
tokens — the token request carries an `authorization_details` claim that scopes
the grant to a specific table and privilege (the REST docs show this
`--data-urlencode "authorization_details=..."` on the `/oidc/v1/token` call). The
gRPC stream needs the equivalent table-scoped token plus the
`x-databricks-zerobus-table-name` header (see the Go SDK's `HeadersProvider`).
So the auth layer must support **requesting a token for a given
`authorization_details`/table scope** (plus `scope=all-apis` and the
`resource=api://databricks/workspaces/<id>/zerobusDirectWriteApi` parameter), not
just the generic client-credentials grant. This is designed concretely in
**§12.2**: a pure `Oauth.token_request_body` builder in `zerobus-core` feeds the
sister SDK's existing `Auth.with_cached_token` + `fetch_oauth_token` (its
`Lwt_mutex`-guarded expiring cache), so we add **no new dependency** — only the
scoped-body assembly. Custom auth still bypasses via
`create_stream_with_headers` (§5.2).

### 8.4 Serialization / util (Required)

| Dep | Role |
|---|---|
| `yojson` + `ppx_deriving_yojson` | JSON record type + config/typed models |
| `ppx_deriving` (`show`, `eq`) | debugging / tests, matches sister SDK |
| `uri`, `base64` | endpoint/URL building, token encoding |

### 8.5 Arrow Flight + IPC codec — v1-blocking with scoped C++ dependency

An earlier draft called Arrow "the one dependency that may not exist." Inspecting
this repo's own C++/Rust SDKs corrects that — the problem splits cleanly into two
independent halves, and only one has an ecosystem gap:

**How Zerobus actually does Arrow (verified from `cpp/`, `rust/sdk/src/stream/arrow/`):**

- The wire protocol is **Arrow Flight `DoPut`** — and *only* `DoPut` (a `grep` of
  the Rust core shows `do_put` and nothing else: no `do_get`/`do_exchange`/
  `get_schema`). `DoPut` is a **client-initiated bidirectional gRPC streaming
  RPC** carrying `FlightData` messages; the per-batch offset rides in
  `FlightData.app_metadata` (JSON) and acks come back in `PutResult.app_metadata`.
- The **caller contract is Arrow IPC bytes**, not a live Flight object: the C++
  `ArrowStream::ingest_batch(ipc_bytes, len)` takes "a self-contained Arrow IPC
  stream (schema message + one record batch message)." The SDK frames those bytes
  into `FlightData`.

So the OCaml SDK needs two things, which are **not** the same dependency:

| Half | What it is | OCaml status | v1 Approach |
|---|---|---|---|
| **Flight `DoPut` RPC** | protobuf `FlightData`/`PutResult` over a bidi gRPC stream | ✅ **PROVEN (`spike-flight/`)** | Just another bidi RPC over the transport from §6.5. Real `Flight.proto` (`data_body=1000`; Zerobus uses only `DoPut`) → `ocaml-protoc`. Spike ran 200 batches with `data_body` as opaque bytes, offset in `app_metadata`, verified byte-exact — **no Arrow library linked** (§8.5.1). |
| **Arrow IPC encode/decode** | build/parse the schema+batch IPC byte blob | ⚠ **requires v1 binding** | Minimal ctypes binding to Arrow C++ `RecordBatchStreamWriter`/`RecordBatchStreamReader`. See §8.5.2 design. Confined to Arrow record-type module in runtime packages (never `zerobus-core`); Proto/JSON builds unaffected. |

**Architecture: scoped Arrow C++ dependency (v1-required)**

We do **not** wrap the C++ *Zerobus* SDK (that would make OCaml depend on the C++ wrapper — the
thing to avoid, and it also drags in the whole FFI-to-Rust-core objection from
§3.1). We also do **not** need a C++ Arrow *Flight* client: since Zerobus only
uses `DoPut` and the wire is plain gRPC, Flight is done natively in OCaml on our
own transport. The **only** place a C++ dependency is warranted is the narrow
**Arrow IPC codec** — turning columnar batches into the IPC byte blob — where no
native OCaml implementation exists (§8.5.2).

**Design principle:** Arrow support is v1-required scope, but the C++ dependency
is rigorously isolated. The codec lives in a narrowly-scoped module (`Arrow_ipc`)
inside the Arrow record-type implementation in the runtime packages
(`zerobus`/`zerobus-eio`/`zerobus-async`). `zerobus-core` has no Arrow dependency
whatsoever; Proto and JSON record types never see `libarrow`. This isolation is
enforced by `.mli` visibility and opam `optional` / conditional `depends` (see
§8.5.2 and the depext story below).

#### 8.5.2 The Arrow IPC codec design (v1-blocking, D3 scoped)

**The gap:** The Arrow C++ library ships `RecordBatchStreamWriter` and
`RecordBatchStreamReader` (stable, well-documented APIs in `arrow/ipc/api.h`).
No pure-OCaml implementation exists. `ocaml-arrow` (Laurent Mazare) binds Arrow
C++ data/Parquet I/O but does **not** expose the IPC codec in its public API.

**Chosen path: minimal v1 ctypes binding** (4–6 hours effort, estimated)

1. **C++ wrapper layer** (`~200 lines; `arrow_ipc_stubs.cc`):
   - `RecordBatchStreamWriter`: serialize a RecordBatch → IPC byte buffer
   - `RecordBatchStreamReader`: deserialize IPC bytes → RecordBatch
   - Memory/lifetime management (Arrow's `Result<T>` unwrapping)
   - No business logic — pure encoding/decoding

2. **OCaml FFI layer** (`ctypes` interface, `~80 lines; arrow_ipc_stubs.mli`):
   ```ocaml
   type record_batch  (* opaque handle to arrow::RecordBatch* *)
   type buffer        (* opaque handle to arrow::Buffer* *)
   val batch_to_ipc : record_batch -> buffer  (* fails via results, not exceptions *)
   val ipc_to_batch : buffer -> record_batch
   val buffer_to_bytes : buffer -> bytes
   val bytes_to_buffer : bytes -> buffer
   ```

3. **Integration point** — the Arrow record type's `ingest` path:
   - Caller passes Arrow columnar data (as bytes or a Polars/Arrow OCaml library if one exists)
   - `Arrow_ipc.batch_to_ipc` encodes it → IPC bytes
   - IPC bytes feed `FlightData.data_body` (the proven native Flight path)

4. **Build wiring** (Dune):
   ```lisp
   (foreign_objects arrow_ipc_stubs.o)
   (flags (:standard -I{libarrow-include-path}))
   (libraries ctypes ctypes_foreign)
   (c_library_flags (-L{libarrow-lib-path} -larrow -larrow_boost_regex))
   ```
   Activated only when Arrow record type is built (a conditional package);
   Proto/JSON packages have no Dune rule referencing it.

**Why this approach over alternatives:**
- **vs. ocaml-arrow upstream contribution:** That is a 2–3 week parallelism; v1
  needs the codec shipping now. Upstream is still an option for v1.1+ "nice to
  have" improvements.
- **vs. Arrow C Data Interface:** More stable ABI, but also more complex for
  schema + batch round-trips. IPC is the simpler, narrower path and is
  well-supported in Arrow C++ v25+.
- **vs. pure-OCaml IPC:** Would be the "no C dep" win but is a 4+ week project
  to reverse-engineer the IPC framing spec; not realistic for v1.

**Depext + CI story:**
- **macOS:** `brew install apache-arrow` (~122 MB, one-shot install; included in
  Homebrew)
- **Linux (Debian/Ubuntu):** `apt install libarrow-dev` (or `libarrow` on RHEL/CentOS)
- **Windows:** Not tested (not a Databricks priority for OCaml SDK v1)
- **opam metadata:** depext declared as `optional` in `zerobus.opam`/`zerobus-eio.opam`
  (the runtime packages that build Arrow); never in `zerobus-core.opam`. CI runs
  the depext install in the build matrix for Arrow tests; Proto/JSON CI is
  unaffected.
- **Isolation:** The dune `foreign_objects` rule that links `libarrow` is scoped
  to the Arrow module. If `libarrow` cannot be found at build time, the build
  **fails with a clear error** (not a silent no-op), making the C++ dependency
  explicit and intentional.

#### 8.5.1 The native Flight `DoPut` half — proven (compiled + run)

`spike-flight/` closes option 1's "the transport can be built and tested
natively" claim with a real spike, on the same pattern as §6.5. It generates the
**real** Arrow Flight proto (`arrow.flight.protocol` `FlightData`/`PutResult`,
including the authentic `data_body = 1000` field) with `ocaml-protoc`, stands up
a mock `FlightService/DoPut` server in a separate process, and runs a native
`grpc-lwt` client that streams 200 `FlightData` batches while draining
`PutResult` acks.

| Aspect | Result |
|---|---|
| RPC | `arrow.flight.protocol.FlightService/DoPut`, bidi, on `grpc-lwt`/`h2` |
| Arrow library linked | **none** — `data_body` carried as opaque bytes |
| Batches | 200 sent / 200 acked; schema message correctly not acked |
| Interleave | first ack mid-send (≈#60–100 of 200) — real bidi |
| Ordering | offsets 0–199 contiguous |
| Payload fidelity | every batch's opaque `data_body` verified **byte-exact** (server echoes `len:checksum`); offset carried in `app_metadata` |

Evidence: `spike-flight/EVIDENCE_FLIGHT.txt`; details in `spike-flight/README.md`.
This confirms the RPC half needs no Arrow dependency — the SDK carries arbitrary
Arrow IPC bytes through `DoPut` without understanding them, exactly as
`rust/sdk/src/stream/arrow/` does. The only remaining Arrow work is the IPC
*codec* (options 2/3 above).

### 8.6 Test / dev (Required for CI)

| Dep | Role |
|---|---|
| `alcotest` + `alcotest-lwt` (and an eio test harness) | unit tests, mirrors sister SDK |
| `odoc` | docs (`with-doc`) |
| a mock gRPC server harness | test recovery/ack logic without network |

### 8.7 Dependency risk summary

- **Present & maintained:** `h2`, `grpc-*`, `gluten`, `tls`/`ssl`,
  `ocaml-protoc`, `yojson` — all on opam as of 2026. TLS/h2/unary-gRPC are
  well-exercised.
- **✅ Primary technical risk — client-initiated bidirectional streaming — NOW
  DE-RISKED.** The SDK's entire hot path is a *long-lived, client-driven bidi
  streaming RPC* with interleaved acks. That is the least-exercised corner of the
  OCaml gRPC libraries, so it was proven first. All three runtimes have
  compiled-and-run spikes that sustain it (200 records, acks interleaved during
  sends): **Lwt** on `grpc-lwt`/`h2` (4.14), **Eio** on `grpc-eio`/`h2-eio` (5.2),
  and **Async** on `grpc-async`/`h2-async` (4.14). See §6.5 and `spike/`,
  `spike-eio/`, `spike-async/`. TLS/ALPN + scoped OAuth are additionally proven
  **live** (`spike-live/`, §12.6). Remaining unknown: reconnect/recovery and the
  real `DoPut`/`IngestStream` RPC against the live service — tracked as next-steps.
- **⚠ grpc-eio 0.2.0 client flush defect** (§6.5): the client send path omits an
  h2 `flush`; fine across separate processes (the real topology) but requires a
  version bump / small vendored patch / raw-h2 writer when building `zerobus-eio`.
  Pin grpc-eio and track the upstream fix.
- **Evolving maintainership:** the gRPC/Eio stack has active but small
  maintainership; expect occasional opam pins. Acceptable, not blocking.
- **Arrow Flight + IPC codec (§8.5, v1-blocking D3).** Zerobus's Arrow path is
  Flight `DoPut`, which is a plain bidi gRPC RPC (native, proven, no Arrow lib).
  The IPC encoder is a narrowly-scoped `libarrow` ctypes binding on the Arrow
  record type (§8.5.2, ~4–6 hours), isolated from `zerobus-core` and Proto/JSON.
  Depext on macOS/Linux; clear fail if missing. No C++ dependency leaks into the
  core or non-Arrow record types.

---

## 9. Proposed repository layout

```
ocaml/
├── DESIGN.md                      # this document
├── dune-project                   # (generate_opam_files true), all packages
├── zerobus-core.opam              # generated
├── zerobus.opam                   # generated (Lwt, 4.14 target)
├── zerobus-eio.opam               # generated (Eio, 5.x target)
├── zerobus-async.opam             # generated (Async, v1)
├── lib/
│   ├── core/                      # zerobus-core: runtime-agnostic
│   │   ├── io.ml/.mli             #   IO signature (§6.2)
│   │   ├── grpc_transport.ml/.mli #   bidi RPC driver, over Io.H2_client
│   │   ├── proto/                 #   generated + hand wire types, descriptors
│   │   ├── stream.ml/.mli         #   Make(Io): stream, ingest, flush, recovery
│   │   ├── options.ml/.mli        #   stream_options + defaults
│   │   ├── offset.ml/.mli
│   │   ├── error.ml/.mli
│   │   └── zerobus_core.ml/.mli   #   Make functor entry point
│   ├── lwt/    (zerobus)          # Zerobus_io_lwt + Make(...) instantiation
│   ├── eio/    (zerobus-eio)      # Zerobus_io_eio + direct-style façade
│   ├── async/  (zerobus-async)    # Zerobus_io_async
│   ├── rest/                      # Zerobus_rest (§4.2)
│   └── otlp/                      # Zerobus_otlp (§4.3)
├── examples/                      # loop-then-flush FIRST; then callback; then rest
├── spike/                         # Lwt transport spike (§6.5/§8.7): bidi gRPC PoC + raw-h2 fallback
├── spike-eio/                     # Eio (5.x) transport spike (§6.5): bidi over grpc-eio, server subprocess
├── spike-async/                   # Async transport spike (§6.5): bidi over grpc-async, server subprocess
├── spike-flight/                  # Arrow Flight DoPut spike (§8.5.1): real Flight proto over bidi, no Arrow lib
├── spike-arrow-ipc/               # Arrow IPC codec investigation (§8.5.2): proof-of-concept for v1-blocking binding
├── spike-live/                    # Live spike (§12.6): native TLS+ALPN h2 & scoped OAuth vs real workspace
├── test/                          # alcotest + mock gRPC server
└── docs/                          # per-feature design docs (sister-SDK style)
```

---

## 10. Example (the pattern users see first)

```ocaml
(* Lwt / 4.14 flavor — the README's opening example *)
let () =
  Lwt_main.run begin
    let open Lwt_result.Syntax in
    let* sdk =
      Zerobus.create
        ~endpoint:"https://<shard>.zerobus.databricks.com"
        ~workspace_url:"https://<workspace>.cloud.databricks.com" ()
    in
    let* stream =
      Zerobus.create_stream sdk
        { table_name = "main.iot.telemetry";
          descriptor = Some (Zerobus.Descriptor.of_message_descriptor Telemetry.descriptor) }
        ~client_id:(Sys.getenv "DATABRICKS_CLIENT_ID")
        ~client_secret:(Sys.getenv "DATABRICKS_CLIENT_SECRET") ()
    in
    (* Queue in the loop; DO NOT wait per record. *)
    let* () =
      Lwt_list.iter_s
        (fun r -> Zerobus.ingest stream r >|= function Ok _ -> () | Error _ -> ())
        records
    in
    let* () = Zerobus.flush stream in          (* wait once for all acks *)
    Zerobus.close stream
  end
  |> function Ok () -> () | Error e -> prerr_endline (Zerobus.Error.to_string e)
```

The Eio flavor is the same shape without `Lwt_main.run`/`>|=`, using direct-style
calls under a `Switch` — provided by the `Zerobus_eio` façade (§6.2).

---

## 11. Open questions / next steps

**Open questions**

1. **Zerobus `.proto` source.** We need the authoritative Zerobus gRPC service
   definition (`.proto`). It lives in the Rust core / service repo — confirm we
   can vendor it and that its license permits redistribution in `ocaml/`.
2. **Arrow Flight** (§8.5, now scoped): the Flight `DoPut` RPC is native/free —
   the only open call is *when* to add the optional `libarrow` IPC-bytes codec
   (v1 Proto+JSON-only vs. build the scoped ctypes/`ocaml-arrow` binding now).
   Recommended: v1 without it; add on the Arrow record type when Beta is
   prioritized. **Not** the C++ Zerobus SDK — the Arrow C++/IPC library directly.
3. **Auth dependency** (§8.3): take the full `databricks-sdk-ocaml` dep, or a
   trimmed auth-only subset to keep the closure small?
4. ~~**Async package**: ship `zerobus-async` in v1 or defer?~~ **RESOLVED — in
   v1, validated.** All three runtimes (Lwt/Eio/Async) are required v1 scope, each
   with a passing transport spike (§6.5); the Async spike is `spike-async/`.
5. **Versioning:** independent semver per the monorepo's `<sdk>/v<semver>` tag
   scheme — pick `ocaml/v*` tags; decide whether `zerobus`, `zerobus-eio`, and
   `zerobus-async` version in lockstep or independently.

**Next steps (suggested order)**

1. Vendor the Zerobus `.proto`; generate types with `ocaml-protoc`; stand up a
   throwaway Proto encode/decode round-trip test.
2. **✅ DONE — transport spikes on BOTH runtimes (de-risked §6.5/§8.7).**
   - **Lwt** (`spike/`): compiled+run on `grpc-lwt 0.2.0` + `h2 0.12.0` (OCaml
     4.14.1). 200 records, first ack at send **#3–8 of 200**, offsets contiguous,
     raw-h2 framing fallback round-trips. Evidence: `spike/EVIDENCE.txt`.
   - **Eio** (`spike-eio/`): compiled+run on `grpc-eio 0.2.0` + `h2-eio 0.12.0` +
     `eio 1.3` (OCaml 5.2.0). 200 records, first ack at send **#10–13 of 200**,
     offsets contiguous. Server runs as a subprocess (grpc-eio client-flush
     defect — §6.5). Evidence: `spike-eio/EVIDENCE_EIO.txt`.
   **Verdict: GO on the native gRPC path for both `zerobus` and `zerobus-eio`.**
   API-drift the spikes corrected is recorded in `spike/README.md`.
3. **✅ DONE — TLS/ALPN + scoped-OAuth, verified LIVE (§12.6).** `spike-live/`
   proved both natively against the real workspace
   `adb-984752964297111.11.azuredatabricks.net`: `tls-lwt` negotiated TLS 1.3 +
   **ALPN `h2`** (cert-verified via `ca-certs`) to the Zerobus gRPC endpoint, and
   native OCaml obtained a **table-scoped** Zerobus token via
   `authorization_details`. Installing `tls-lwt` rebuilt gluten's real `Tls_io`,
   confirming §12.1's dependency finding. Evidence: `spike-live/EVIDENCE_LIVE.txt`.
4. Build the **bidi streaming driver** (`grpc_transport.ml`) with the send/ack
   fiber pair and the `IO` mailbox; get `ingest` → `wait_for_offset` → `flush`
   working against a mock server. For `zerobus-eio`, resolve the grpc-eio client
   flush (§6.5: version bump / vendored patch / raw-h2 writer).
5. Add **recovery** (reconnect + replay un-acked, honoring the retry budget);
   this is the last transport unknown the spikes did not cover.
6. Extract the **`IO` functor**; instantiate `zerobus` (Lwt). Confirm it builds
   on both a 4.14 and a 5.x switch.
7. Add the **`zerobus-eio`** (direct-style façade; validate on 5.x) and
   **`zerobus-async`** (`Deferred`/`Pipe`) instantiations. All three transports
   are already proven (§6.5); this is wiring them onto the real driver.
8. Add `Zerobus_rest`; then `Zerobus_otlp`.
9. Examples (loop-then-flush first), README, `docs/` design notes, opam lint, CI
   wiring into the monorepo's path-filtered `push.yml`.

---

## 12. The four remaining unknowns — design (TLS/ALPN, OAuth, recovery, live endpoint)

These are the pieces the transport spikes (§6.5) deliberately left out. Each is
designed below with an explicit **dependency budget**, because the hard
constraint is **add no new runtime dependency we don't already need**. Where the
sister SDK (`databricks-field-eng/databricks-sdk-ocaml`) already solves the
problem, we lift its pattern rather than invent one.

> **What the sister SDK gives us for free (verified by reading its source).** It
> is Lwt-only and its whole networking stack is `cohttp-lwt-unix` + `lwt` +
> `yojson` + `uri` + `base64` — the same libraries the REST/OTLP side and the
> OAuth token fetch need. Its `Auth` module already implements the exact
> client-credentials grant, an `Lwt_mutex`-guarded expiring **token cache**
> (`with_cached_token`, refresh-30s-early), secret scrubbing, and a
> pluggable-provider chain; its `Retry` module already implements
> exponential-backoff-with-jitter and an overall wall-clock budget. We reuse both
> designs directly (see §12.2, §12.3).

### 12.1 TLS + ALPN `h2` — the one genuine new dependency

**Finding (verified on this machine, not assumed).** The spikes ran cleartext
`h2c` on loopback. Neither switch had any TLS library installed, and the gluten
runtime that h2 uses ships its TLS glue as an **optional backend that was
compiled to a dummy**:

- `gluten-lwt-unix 0.5.2` exposes `Client.TLS.create_default ?alpn_protocols`
  and `Client.SSL.create_default ?alpn_protocols` — so the ALPN hook the SDK
  needs *is* in the API surface. But with no TLS backend present at gluten's
  build time, its `Tls_io`/`Ssl_io` modules compiled from `tls_io.dummy.ml`,
  whose socket type is literally `` [ `Tls_not_available ] ``. It links but does
  nothing. (Confirmed: a probe calling `Gluten_lwt_unix.Client.TLS.create_default`
  builds fine yet cannot actually negotiate TLS.)
- gluten's declared TLS `depopts` are exactly **`tls-lwt`** (pure-OCaml
  `ocaml-tls`) **or `lwt_ssl`** (OpenSSL bindings). Installing one and rebuilding
  gluten swaps the dummy for the real `Tls_io`/`Ssl_io`. On the Eio side the
  parallel is `gluten-eio` with `tls-eio` / `ocaml-ssl`.

**So TLS is the single place the "no new dependency" rule cannot hold** — h2
over TLS with ALPN `h2` (mandatory for gRPC) requires a TLS backend that is not
in today's closure, and there is no way around it in a native SDK. The design
minimizes and contains the addition:

- **Choose `ocaml-tls` (`tls-lwt` / `tls-eio`) as the default** rather than
  OpenSSL: it is pure OCaml, adds no C/system-lib build dependency, is what
  gluten/h2/mirage are primarily tested against, and pulls a well-maintained
  closure (`mirage-crypto`, `x509`, `ca-certs`). `ca-certs` gives us the system
  trust anchors without hand-rolling certificate handling (the sister SDK's
  golden rule: *never implement custom TLS*). Offer `ocaml-ssl` as an opt-in
  build flavor for shops that must use system OpenSSL.
- **It is one dependency, confined to the runtime shim packages** (`zerobus` /
  `zerobus-eio`), not `zerobus-core`. The core stays transport-pure behind
  `Grpc_transport.S`; only the Lwt/Eio instantiations link the TLS backend.
- The `H2_client` connect path becomes: resolve host → TCP connect →
  `Client.TLS.create_default ~alpn_protocols:["h2"] socket` → hand the gluten
  runtime to `H2_eio.Client`/`H2_lwt_unix.Client`. ALPN **must** offer exactly
  `["h2"]`; if the server does not negotiate `h2`, fail fast with
  `Transport_error` rather than silently downgrading to HTTP/1.1.
- `zerobus_core.opam` unchanged; `zerobus.opam` gains `tls-lwt`,
  `zerobus-eio.opam` gains `tls-eio` (each with `ca-certs`). This is disclosed in
  §8.1 as the expected TLS dependency; §12 records that it is now *confirmed
  required*, not optional.

**✅ Spike closed — verified live (§12.6).** `spike-live/tls_alpn.ml` performs a
native `tls-lwt` handshake to the real Zerobus endpoint
`984752964297111.zerobus.eastus2.azuredatabricks.net:443` and asserted **TLS 1.3,
cert chain verified against `ca-certs` system trust anchors, ALPN negotiated
`h2`**. Installing `tls-lwt`+`ca-certs` also **rebuilt `gluten-lwt-unix` with its
real `Tls_io`** (dummy→real), confirming the mechanism above end-to-end. Evidence:
`spike-live/EVIDENCE_LIVE.txt`.

### 12.2 Scoped OAuth (`authorization_details`) — reuse, zero new deps

**Verified Zerobus specifics (from the ingest docs):**

- Token endpoint: `<workspace_url>/oidc/v1/token`, client-credentials grant with
  the service-principal id/secret — **identical** to the sister SDK's
  `oauth_m2m_provider`.
- Two Zerobus-specific extras beyond a vanilla workspace token:
  1. **`scope=all-apis`** and a **`resource`** parameter
     `api://databricks/workspaces/<workspace_id>/zerobusDirectWriteApi`.
  2. **`authorization_details`** — a JSON array scoping the grant to the exact
     table, one entry per UC level:
     ```json
     [ {"type":"unity_catalog_privileges","privileges":["USE CATALOG"],
        "object_type":"CATALOG","object_full_path":"<catalog>"},
       {"type":"unity_catalog_privileges","privileges":["USE SCHEMA"],
        "object_type":"SCHEMA","object_full_path":"<catalog>.<schema>"},
       {"type":"unity_catalog_privileges","privileges":["MODIFY","SELECT"],
        "object_type":"TABLE","object_full_path":"<catalog>.<schema>.<table>"} ]
     ```
- Tokens expire hourly and must be refreshed. Both the gRPC stream and the REST
  interface carry the resulting `Authorization: Bearer <token>` (plus, for
  streams, the table-name metadata header the Go SDK sends via `HeadersProvider`).

**Design — a thin Zerobus token provider built on the sister SDK's machinery:**

```ocaml
(* zerobus-core: pure builder, no I/O, no new deps — just string/JSON assembly *)
module Oauth : sig
  (* Build the form body for a table-scoped client-credentials grant. *)
  val token_request_body :
    client_id:string -> client_secret:string ->
    workspace_id:string -> table:string (* catalog.schema.table *) ->
    string  (* application/x-www-form-urlencoded, incl. scope/resource/authorization_details *)

  val token_endpoint : workspace_url:string -> string
end
```

The actual fetch + cache is **not reimplemented**: the Lwt package reuses the
sister SDK's `Auth.with_cached_token` (the `Lwt_mutex`-guarded, refresh-early
cache) and its `fetch_oauth_token` shape — a single `cohttp-lwt-unix` POST
parsing `access_token`/`expires_in`, already written and tested there. Two ways
to consume it, decided by the auth-dependency choice in §8.3:

- **If we take the `databricks-sdk-ocaml` dep** (recommended, §8.3): call its
  `Auth`/token-cache directly, extended with a Zerobus provider that supplies the
  `scope`/`resource`/`authorization_details` body from `Oauth.token_request_body`.
  Users configure both SDKs identically (`DATABRICKS_CLIENT_ID/SECRET`,
  `.databrickscfg`). **No new dependency** — its closure is a subset of ours.
- **If we stay minimal:** re-express the ~40-line token-cache + POST using
  `cohttp-lwt-unix`/`yojson`/`uri` (Lwt) or `cohttp-eio` (Eio) — libraries the
  REST/OTLP side already pulls. Still **no new dependency**.

Custom-auth users bypass all of this via `create_stream_with_headers` (§5.2): the
`headers_provider` returns the `Authorization` + table-name headers however they
like (federation, pre-fetched token, sidecar). The stream stamps the table-name
metadata header on every reconnect.

**✅ Verified live (§12.6).** `spike-live/oauth_token.ml` built exactly this
`authorization_details` body (3 UC entries) with `scope=all-apis` and the
`zerobusDirectWriteApi` resource, POSTed it natively to a real workspace's
`/oidc/v1/token` with a service-principal id+secret, and received a **table-scoped
token** (`expires_in 3600`). Every §12.2 parameter is confirmed against the live
OIDC endpoint. Evidence: `spike-live/EVIDENCE_LIVE.txt`.

### 12.3 Mid-stream recovery — reuse the retry design, add stream replay

Recovery is what turns a raw bidi RPC into a durable ingest stream. The Go SDK's
knobs already live in `stream_options` (§5.1): `recovery`, `recovery_timeout_ms`,
`recovery_backoff_ms`, `recovery_retries`, `server_lack_of_ack_timeout_ms`. The
backoff/jitter/budget policy is **exactly** the sister SDK's `Retry` module
(exponential base·2^n capped at max, jitter fraction, overall wall-clock
`retry_timeout_s`) — we reuse that algorithm; no new dependency (pure `Unix`
time + the runtime's `sleep`, both already in `IO`).

**What is Zerobus-specific and must be built in `grpc_transport.ml` / `stream.ml`:**

- **Un-acked replay buffer.** The stream retains every ingested record from the
  last server-acked offset forward (bounded by `max_inflight_requests`). On a
  retryable transport/stream failure it opens a fresh bidi RPC (new token via
  §12.2 if expired, table-name header re-stamped) and **re-sends the un-acked
  tail in order**, preserving Zerobus's per-stream ordering guarantee. Offsets
  the caller already holds stay valid — recovery is invisible above `ingest`.
- **Failure classification.** `Grpc.Status` trailers + `H2`/TLS transport errors
  map onto `Error.t` (§5.4) with a `retryable` flag: `Unavailable`,
  `Deadline_exceeded`, connection reset, and the server's `CloseStreamSignal`
  are retryable (auto-recover); `Invalid_argument`, `Permission_denied`,
  `Schema_rejected`, and exhausted `recovery_retries` are fatal and surface to
  the caller.
- **Graceful server-initiated pause.** Honor `StreamPausedMaxWaitTimeMs`
  (§5.1 note): on a server `CloseStreamSignal`, wait up to
  `min(configured, server_duration)` before reconnecting.
- **Ack-timeout watchdog.** `server_lack_of_ack_timeout_ms` bounds how long an
  un-acked record may sit before the stream treats silence as a failure and
  recovers. Implemented with the `IO.Scope`/`sleep` already in the functor — no
  new dependency.
- **Structured teardown.** Recovery fibers live in the stream's `Scope` (§6.2),
  so `close` cancels an in-flight reconnect cleanly.

**Spike to close it:** drive the mock server (the spikes' `ingest_server_*`) to
drop the connection mid-stream after N acks; assert the client transparently
reconnects, replays the un-acked tail, and every offset is ultimately acked
exactly once with ordering preserved. Runs on the existing mock — **no new dep**.

### 12.4 Talking to the live Zerobus endpoint — integration test, env-gated

The sister SDK's pattern (its `test_integration/`, env-gated on
`DATABRICKS_HOST`+`DATABRICKS_TOKEN`) is the model. We mirror it:

- **Endpoint derivation.** `workspace_url` is the base; the gRPC endpoint is
  `<workspace-id>.zerobus.<region>.cloud.databricks.com` (per the docs). Add a
  `Config`-style resolver that derives it (or accepts an explicit `~endpoint`,
  as `create` already does in §5.2), reusing the sister SDK's cloud/host parsing
  rather than new URL logic.
- **`test_integration/` suite**, gated on `DATABRICKS_HOST`,
  `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, and a
  `ZEROBUS_TEST_TABLE` (a pre-created UC table — Zerobus never auto-creates).
  Skips cleanly when unset, so CI and offline dev are unaffected. It exercises
  the *real* stack the loopback spikes could not: TLS+ALPN handshake (§12.1),
  scoped-token acquisition (§12.2), a JSON and a Proto ingest, `wait_for_offset`
  + `flush`, and a forced-recovery case if the harness can induce one.
- **Dependencies:** none beyond the SDK itself plus `alcotest` (already a
  `with-test` dep). No live-service dependency leaks into the published package.

### 12.5 Dependency budget summary for §12

| Concern | New runtime dependency? | Basis |
|---|---|---|
| TLS + ALPN `h2` | **Yes — `tls-lwt`/`tls-eio` (+`ca-certs`)**, confined to the runtime shim packages | gluten's TLS backend is a dummy unless a backend is installed; unavoidable for native gRPC-over-TLS. `ocaml-tls` chosen to avoid a C/OpenSSL dep. |
| Scoped OAuth | **No** | Reuses sister SDK `Auth` token-cache + `cohttp`/`yojson`/`uri` already needed by REST/OTLP. |
| Mid-stream recovery | **No** | Reuses sister SDK `Retry` algorithm + `IO.Scope`/`sleep`/`Unix` time. |
| Live-endpoint integration | **No** | Env-gated `test_integration/`, `alcotest` only (already `with-test`). |

Net: **one** new dependency (`ocaml-tls`, plus `ca-certs`), and it is required by
the protocol, not by convenience. Everything else is reuse.

### 12.6 Live verification against a real workspace (compiled + run)

Next-step 3 is **done, against a real Azure Databricks workspace**
(`adb-984752964297111.11.azuredatabricks.net`, eastus2). Two native-OCaml
programs in `spike-live/` (OCaml 4.14.1, `tls-lwt 0.17.5`, `ca-certs 0.2.3`,
`cohttp-lwt-unix 6.0.0`) prove the two unknowns the loopback spikes could not:

| Program | Concern | Live result |
|---|---|---|
| `tls_alpn.ml` | §12.1 TLS + ALPN | ✅ TLS 1.3 to the real `…zerobus…:443`; cert verified vs `ca-certs`; **ALPN negotiated `h2`** |
| `oauth_token.ml` | §12.2 scoped OAuth | ✅ SP client-credentials + `authorization_details` → **table-scoped token**, `expires_in 3600` |

Evidence (toolchain, timestamps, measured facts, resource ids): `spike-live/EVIDENCE_LIVE.txt`.
The `ocaml-tls` dependency finding was also confirmed operationally — installing
`tls-lwt` rebuilt `gluten-lwt-unix` from a dummy `Tls_io` to the real one.

**Still not covered (the honest boundary):** the actual gRPC `IngestStream` RPC /
a row landing in Delta. That needs the authoritative Zerobus `.proto` vendored
(next-step 1) to frame real ingest messages; the transport (TLS/ALPN/h2, proven
here) and the bidi mechanics (proven in `spike/`) underneath it are both green,
so the remaining work is wiring the real proto onto the proven bidi path
(next-step 4). Recovery (§12.3) remains designed-but-unspiked.

**Live resources** (created in `azure-field-eng-eastus2` for §12.2; teardown
steps in `spike-live/README.md`): a throwaway service principal
`zerobus-ocaml-spike-<ts>` + OAuth secret, and table
`main.default.zerobus_spike_<ts>` granted `USE CATALOG`/`USE SCHEMA`/`MODIFY,SELECT`.

---

## Appendix A — research notes captured this session

- **Zerobus docs read:** overview (push-based, serverless, stream=1 table,
  per-stream ordering, schema validation, rejected→`_zerobus/...parquets`,
  durability acks, OAuth SP) and ingest "choose an interface" (gRPC / REST /
  OTLP; Proto / JSON / Arrow-beta; offset/callback/fire-and-forget ingestion
  styles; loop-then-flush batch pattern).
- **Sister SDK examined:** `databricks-field-eng/databricks-sdk-ocaml` — Lwt-only,
  `cohttp-lwt-unix`, OCaml `>= 4.14`, pluggable `transport` record in
  `http_client.mli`, `Error.t` variant taxonomy, `Iterator.t` pagination,
  `create_and_wait` waiters, strict CLAUDE.md golden rules (results-over-
  exceptions, `.mli` everywhere, no objects, `{strict=false}` derivers, no
  hand-rolled TLS). Its design docs (`docs/*.md`) use a "Status / Verified
  against / Scope / The gap / Design" structure this doc loosely follows.
- **Go SDK examined (protocol/feature reference):** `StreamConfigurationOptions`
  defaults (max_inflight 1M, recovery on, 15s/2s/4 retries, 60s ack, 5m flush,
  Proto default), `IngestRecordOffset`/`WaitForOffset`/`Flush`, deprecated
  per-record `RecordAck.Await`, `HeadersProvider` custom auth, retryable-vs-fatal
  `ZerobusError`. These informed §5.
- **OCaml ecosystem (web search, 2026):** gRPC via dialohq `grpc` + `grpc-lwt`/
  `grpc-eio`/`grpc-async` over anmonteiro `h2` (+ `h2-lwt-unix`/`h2-eio`/
  `h2-async`) glued by `gluten`; TLS via pure-OCaml `tls` **or** OpenSSL `ssl`,
  both with ALPN `h2`; protobuf via `ocaml-protoc` (native) or
  `ocaml-protoc-plugin` (protoc plugin). Eio needs OCaml 5.x; Lwt/Async run on
  both 4.14 and 5.x.
- **Arrow investigation (2026-08-10, corrected §8.5).** Verified from this repo's
  own SDKs: Zerobus Arrow = Flight **`DoPut` only** (bidi gRPC; offset in
  `FlightData.app_metadata`, ack in `PutResult.app_metadata`), and the caller
  contract is **Arrow IPC bytes** (`cpp/.../arrow_stream.hpp
  ingest_batch(ipc_bytes,len)`; `rust/sdk/src/stream/arrow/` uses `do_put` and
  nothing else). Web search: `ocaml-arrow` (Laurent Mazare) binds Arrow **C++**
  data/Parquet via ctypes but has **no Flight**; no OCaml Flight client exists on
  opam. Conclusion: the Flight RPC half is native OCaml (small public
  `Flight.proto` → `ocaml-protoc`, over the proven bidi transport); only the Arrow
  **IPC codec** is a real gap, addressed by an optional scoped `libarrow`/ctypes
  binding on the Arrow record type — never by wrapping the C++ Zerobus SDK.
- **§12 research (2026-08-10):**
  - **gluten TLS is a dummy without a backend (verified on `fl414`).**
    `gluten-lwt-unix 0.5.2` exposes `Client.TLS.create_default ?alpn_protocols`
    but compiled `tls_io.dummy.ml` (`type descriptor = [ `Tls_not_available ]`)
    because no TLS backend was installed; a probe calling it links but cannot do
    TLS. gluten's TLS `depopts` are `tls-lwt` **or** `lwt_ssl` (Eio: `tls-eio`/
    `ocaml-ssl`), both present in opam. ⇒ TLS is a *required* new dep (§12.1).
  - **Sister SDK reuse (read `auth.ml`/`auth.mli`/`retry.mli`).** `oauth_m2m_provider`
    already does the client-credentials POST; `with_cached_token` is an
    `Lwt_mutex`-guarded cache refreshing 30s early; `Retry.with_retry` is
    exp-backoff+jitter with a wall-clock budget. Reused wholesale by §12.2/§12.3
    ⇒ no new deps there. Sister SDK's whole net stack is `cohttp-lwt-unix`+`lwt`+
    `yojson`+`uri`+`base64` (a subset of ours).
  - **Zerobus OAuth specifics (ingest docs):** endpoint `<workspace>/oidc/v1/token`;
    `scope=all-apis`; `resource=api://databricks/workspaces/<workspace_id>/zerobusDirectWriteApi`;
    `authorization_details` = JSON array of `unity_catalog_privileges` for
    CATALOG(USE CATALOG)/SCHEMA(USE SCHEMA)/TABLE(MODIFY,SELECT); hourly expiry;
    endpoint `<workspace-id>.zerobus.<region>.cloud.databricks.com`.
