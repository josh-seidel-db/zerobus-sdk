# Async TLS transport — status & blockers

**Status: RESOLVED — Async live TLS works and is LIVE-VERIFIED.** The fix was to
stop fighting gluten-async and drive the runtime-agnostic `h2` core
(`H2.Client_connection`) over a `tls-async` `Reader`/`Writer` duplex ourselves —
exactly what the "recommended real path" note below predicted. Last updated:
2026-08-11 (third pass — solved).

## Resolution (2026-08-11, third pass)

The Async transport (`lib/async/`) no longer depends on `gluten-async` / `h2-async`
at all. It uses:

- **`lib/async/h2_pump.ml`** — drives `H2.Client_connection` over an Async
  `Reader.t`/`Writer.t`: a write loop (`next_write_operation` → `Writer.write_bigstring`
  the IOVecs → `report_write_result`; `` `Yield`` → `yield_writer`) and a read loop
  (`Reader.read_one_chunk_at_a_time` → `Client_connection.read`; EOF → `read_eof`).
- **`lib/async/tls_connect.{real,dummy}.ml`** — a dune `select` on `tls-async`:
  the real one does `Tls_async.connect` with `Ca_certs` + peer-name verification +
  `~alpn_protocols:["h2"]`, checks the negotiated ALPN off `Session.epoch`, and
  returns a cleartext `Reader`/`Writer` duplex; the dummy (no tls-async) returns an
  honest error. Cleartext h2c uses a plain `Tcp.connect` duplex — always available.
- **`zerobus_io_async.ml` `connect`** builds the duplex (TLS or cleartext), creates
  the h2 core, starts the pump, and wraps it as the existing `connection` record.

**Live proof:** `spike-async-tls/` (a standalone version of the pump) ingested 5
JSON rows into a real Delta table over TLS 1.3 + ALPN h2 against
`984752964297111.zerobus.eastus2...:443` — CreateStreamResponse received,
grpc-status=0, rows VERIFIED PERSISTED. Done on a throwaway `fl414-asyncspike`
switch (h2 + tls-async + async, NO gluten-async), removed after.

**Packaging reality (why it's a `select`, not a hard dep):** `tls-async` on the
throwaway switch pulled the async **v0.15** world + **h2 0.13**; fl414 (the
canonical switch, all other green tests) is async **v0.16** + **h2 0.12**, and the
two h2 versions differ (`Body.Writer.flush` callback is `unit -> unit` on 0.12 vs
`` [`Written|`Closed] -> unit`` on 0.13). So the *integrated lib* targets fl414's
h2 0.12 and does NOT install tls-async on fl414 (that still cascades the downgrade
documented below). The `select` means: on fl414, cleartext h2c works and live TLS
returns an honest error; on any switch with a compatible `tls-async` + `h2`, live
TLS lights up automatically with zero code change. The transport code is the same
either way — only the connect duplex differs.

**In-tree test (2026-08-11, added):** there is an in-tree Async live-TLS test —
`test_driver_async_tls/` (`tls_server_async.ml` self-signed h2 mock +
`test_tls_async.ml`), the Async counterpart of `test_driver_eio/test_tls_eio.ml`.
It drives the `Zerobus_async` façade over a REAL TLS 1.3 + ALPN-h2 handshake
against a self-signed mock — no live service — and includes a **negative test**
(a wrong cert fingerprint is rejected), so it proves real certificate
verification, matching Eio. 4/4 green. Evidence:
`test_driver_async_tls/EVIDENCE_TLS_ASYNC.txt`.

There is, however, **no single switch with fl414's *exact* deps AND tls-async** —
the upstream triangle forbids it: the lib needs `h2 = 0.12`, pinning h2 0.12
forces `tls-async 0.17.0`, which pulls `async/core v0.15` (fl414 is v0.16) and
drags `grpc-async` down to `0.1.0` (no `Grpc_async.Server` — so the mock drives
`H2.Server_connection` by hand). So the test lives on its own throwaway switch
(`fl414-tls-intree`), exactly as the Eio/spike dirs are switch-specific, and its
dune is `(optional)` so it vanishes on switches without tls-async. The **one**
change to the shipped lib to make this buildable: the Async transport now uses
`Clock_ns`/`Time_ns` (present on both async v0.15 and v0.16) instead of
`Time_float` (v0.16-only), making it async-version-portable — verified not to
regress the 6 Async mock tests on fl414 (v0.16).

**Built-in OAuth on Async (2026-08-11, added):** the façade now has `mint_token`
+ `with_stream_oauth` (client-credentials grant, token cache, mirrors the Lwt
reference). Like live TLS, the HTTP backend is a dune `select` — on `cohttp-async`
+ yojson/uri/base64: present → `lib/async/oauth.real.ml` does the real token POST;
absent → `oauth.dummy.ml` returns an honest error and the caller supplies a bearer
via `with_stream`'s `headers_provider`. `Zerobus_async.oauth_available : bool`
reports which. Kept optional (maintainer decision) so the canonical fl414 switch is
unchanged — installing cohttp-async forces `cohttp 6.0 → 5.3` (proven benign: Lwt
3/3, REST 8/8, async 3/3 on 5.3), and gRPC-only users shouldn't pay that. Test:
`test_driver_async_oauth/test_oauth_async.ml` 6/6 (mock token endpoint; incl. cache
+ error-path). Evidence: `test_driver_async_oauth/EVIDENCE_OAUTH_ASYNC.txt`.

**Remaining (optional):** none tracked for Async parity — live TLS, in-tree TLS
test, and built-in OAuth are all done. (REST/OTLP remain Lwt-only by design.)

---

## Historical: the gluten-async dead-end (why we bypassed it)

**Status (pre-resolution):** DEFERRED (packaging-blocked, confirmed DEEPER than
first thought). Kept for the record — the `h2`-core-over-`tls-async` path above
supersedes all of this.

## 2026-08-11 follow-up (second attempt) — the blocker is worse than "dummy tls_io"

Retried the doc's revival recipe on a **throwaway clone switch** `fl414-asynctls`
(never touched fl414; removed after). Findings that go beyond the original writeup:

- Installing the async + `tls-async` + `gluten-async` + `h2-async` + `grpc-async`
  stack in **one solve** still (a) built `gluten-async`'s **dummy** `tls_io`
  (7× `failwith "TLS not available"`) AND (b) **downgraded the Jane Street stack to
  v0.15** (async v0.15.0, core v0.15.1) pulling `tls 0.17.0`, and `grpc-async`
  wouldn't even resolve. Confirms opam can't be coaxed into the consistent set.
- Forcing `gluten-async` to rebuild with `tls-async` present (so the `(select
  tls_io.ml from (tls-async -> real))` picks `tls_io.real.ml`) then **fails to
  compile**: `gluten-async 0.5.2`'s `async/tls_io.real.ml:114` does
  `Tls.Config.client …` expecting a plain value, but **`tls 0.17.0` changed
  `Tls.Config.client` to return `(_, _) result`** — a hard API break. So even with
  the select resolved, gluten-async 0.5.2 ✗ tls 0.17.0.
- Net: this is an **upstream version-incompatibility triangle** (gluten-async ↔ tls
  ↔ the async v0.16 world fl414 needs), not just a build-time select-dep opam can't
  see. Plain opam / pinning cannot fix it without patching gluten-async or an exact
  multi-package pin nobody has worked out.

**Recommended real path if revisited (avoids gluten-async's TLS entirely):**
`tls-async` DID build (0.17.0) and exposes exactly what Lwt uses —
`Tls_async.connect : Tls.Config.client -> … -> (Session.t * Reader.t * Writer.t)
Deferred.Or_error.t` (ALPN via `Tls.Config`, cleartext Async `Reader`/`Writer` out).
Mirror the Lwt transport: TLS via `Tls_async.connect` directly, then drive the
`h2` core state machine over those Reader/Writer — **without** gluten-async's
`tls_io` at all. Blocker to still solve: get a compatible `h2-async` (its build
here was dragged down with gluten-async) or run the `h2` core (`gluten`/`h2` are
runtime-agnostic) over the tls-async duplex by hand. Non-trivial but it sidesteps
both walls. Do it on a throwaway switch; keep fl414 pristine (it holds all green
tests). Everything else in the SDK — including live JSON/Proto/Arrow on Lwt+Eio —
is done and verified; Async remains mock-only (`tls:false`).

---

## Original writeup (first attempt)

**Status:** DEFERRED (packaging-blocked). The code is written and type-checks; it
cannot be *run* on the current toolchain. Lwt and Eio are the live-verified TLS
references. Last investigated: 2026-08-11.

The experimental (compiling) implementation lives on branch
**`feat/async-tls-experimental`** — `lib/async/zerobus_io_async.ml` `connect` TLS
branch + the `gluten-async` dep in `lib/async/dune`. On `main`, the TLS branch
returns an honest `Transport_error` and points here.

---

## TL;DR

Async live-TLS is blocked by **library packaging**, not by our code. Two distinct
walls:

1. **`gluten-async` is built with a dummy TLS backend** on our switches — every TLS
   IO op is `failwith "TLS not available"`. Rebuilding it against a real
   `tls-async` proved intractable through opam.
2. Even with a working build, **h2-async's only reachable TLS client path does no
   certificate verification** (null authenticator) and its convenience constructor
   hardcodes the wrong ALPN protocol. So Async TLS could at best prove
   handshake + ALPN-h2 + framing, never cert validation (unlike Eio, which has a
   negative test proving a bad cert is rejected).

If the ecosystem improves (a `gluten-async` that exposes a configurable TLS
connect, or ships real-tls by default), revisit using the revival recipe below.

---

## Wall 1: gluten-async builds the DUMMY tls_io

`gluten-async`'s dune selects its TLS backend at build time:

```
(select tls_io.ml from
  (tls-async  -> tls_io.real.ml)
  (!tls-async -> tls_io.dummy.ml))
```

This keys on the **`tls-async` findlib library being resolvable when gluten-async
compiles**. On the `fl414` switch, `tls-async` was never installed, so gluten-async
compiled `tls_io.dummy.ml`:

```ocaml
type _ descriptor = [ `Tls_not_available ]
let make_default_client ?alpn_protocols:_ ?host:_ _ _ = failwith "TLS not available"
```

`H2_async.Client.TLS.*` therefore exists and *type-checks* (the dummy exposes the
same signatures) but throws at runtime. Our code compiling on `fl414` is not
evidence it works.

### Why opam couldn't produce a real-tls gluten-async

The `(select)` is a **build-time optional dependency invisible to opam's dependency
graph** — opam does not know gluten-async "wants" tls-async. Consequences observed:

- Installing `tls-async` then `gluten-async` separately: opam's ordering built
  gluten-async before tls-async's library existed → dummy again.
- `opam reinstall gluten-async` after tls-async is present: produces the **real**
  backend — but installing/reinstalling `h2-async` (which depends on gluten-async)
  then **removes `tls-async`** (opam sees it as an unneeded leaf) and rebuilds
  gluten-async as the dummy.
- Requesting `tls-async gluten-async h2-async grpc-async` together as roots: opam's
  solver cascaded into a **full downgrade of the Jane Street + crypto stack to
  v0.15** (async_kernel v0.15, core v0.15.1, tls 0.17.0, mirage-crypto 0.11.3,
  grpc 0.1.0) — a different, incompatible world.

`tls-async.2.0.4` itself only constrains `async >= v0.16`, so it *is* compatible
with the installed v0.16 in principle; the problem is purely the solver's freedom to
re-pick versions around the invisible select-dep.

## Wall 2: h2-async's TLS client can't verify certs or set ALPN=h2

Even with a real gluten-async, the reachable public API is inadequate for gRPC:

- `H2_async.Client.TLS.create_connection_with_default` hardcodes
  `~alpn_protocols:["http/1.1"]` (gRPC needs `h2`) **and** a null authenticator.
- The lower-level `Gluten_async.Client.TLS.create_default ?alpn_protocols` lets us
  pass `["h2"]` (correct), but still uses `null_auth` internally — **no peer cert
  verification**.
- The fully-configurable path, `Gluten_async.Tls_io.connect ~config:(Tls.Config.client …)`
  (which would accept a real authenticator like `Ca_certs` or a pinned fingerprint),
  is **not exported** — it's an internal of gluten-async, and its `descriptor`
  constructor isn't public either.

So our experimental code uses `Gluten_async.Client.TLS.create_default
~alpn_protocols:["h2"]` — correct protocol, but `null_auth`. That means **no
negative test is possible** (a wrong/self-signed cert would be accepted). Eio, by
contrast, threads a custom authenticator and has a passing negative test
(`test_tls_eio` wrong-fingerprint → handshake rejected).

---

## Revival recipe (if revisiting)

1. On a **throwaway switch** (never the working one), get a real-tls gluten-async:
   `opam install tls-async`, then `opam reinstall gluten-async`, then reinstall
   `h2-async grpc-async` **without** letting opam drop tls-async. Verify:
   `grep -c 'TLS not available' $(ocamlfind query gluten-async)/tls_io.ml` → `0`.
   Pinning exact versions of the whole async+crypto+ppxlib set in one manifest is
   likely required to stop the solver from downgrading to v0.15.
2. Restore the experimental transport from `feat/async-tls-experimental`
   (re-add `gluten-async` to `lib/async/dune`).
3. For a *verifying* client (to match Eio), you likely need a newer gluten-async
   that exposes a configurable TLS connect, OR to call `Tls_async.connect` directly
   and adapt its `Reader.t/Writer.t` into something `H2_async.Client` accepts —
   non-trivial, since h2-async wants a `Tls_io.descriptor` whose constructor is
   private.
4. Verify against a TLS h2 mock. The Eio `tls_server_eio` (self-signed, prints
   `READY <port> <cert-fp>`) can serve as a cross-process TLS backend; the Async
   client with `null_auth` will connect without needing the fingerprint. A passing
   run proves handshake + ALPN-h2 + framing (but not verification — see Wall 2).

## What "done" would look like

- Handshake + ALPN-h2 + 200 records acked over TLS: achievable once Wall 1 is
  cleared.
- Certificate verification + negative test: needs Wall 2 cleared (ecosystem work).

Until both are cleared, **use the Lwt runtime for live-TLS Async-adjacent needs**,
or the Eio runtime (which has full verified TLS incl. the negative test).
