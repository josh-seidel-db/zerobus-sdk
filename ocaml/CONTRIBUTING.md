# Contributing to the Zerobus OCaml SDK

Please read the [top-level CONTRIBUTING.md](../CONTRIBUTING.md) first for general
contribution guidelines, the pull request process, signed-commit / DCO requirements,
and commit-message conventions.

This document covers OCaml-specific development setup and workflow.

## Architecture in brief

Unlike the wrapper SDKs, the OCaml SDK is a **native, pure-OCaml peer implementation**
of the Zerobus gRPC protocol — it does not bind the Rust core over FFI. One
runtime-agnostic core (`zerobus-core`) is instantiated by three thin runtime packages:

- **`zerobus` (Lwt)** — the reference runtime, OCaml 4.14+.
- **`zerobus-eio` (Eio)** — OCaml 5.x (needs effects).
- **`zerobus-async` (Async)** — OCaml 4.14+.

The REST and OTLP interfaces ship as separate per-runtime packages
(`zerobus-rest[-eio/-async]`, `zerobus-otlp[-eio/-async]`), and the Arrow record codec
as `zerobus-arrow`. See [`README.md`](README.md) and [`DESIGN.md`](DESIGN.md) for the
full picture.

## Development Setup

### Prerequisites

- Git
- [opam](https://opam.ocaml.org/) (the OCaml package manager) and `dune`
- Two opam switches, because the runtimes span the OCaml 4/5 boundary:
  - a **4.14** switch — Lwt + Async, plus the REST / OTLP / Arrow packages
  - a **5.x** switch (e.g. 5.2) — the Eio runtime (needs OCaml 5 effects)
- Apache Arrow C++ (`libarrow`) for the Arrow codec / its tests. On macOS
  (Homebrew): `export PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig`; on Debian/Ubuntu:
  `apt-get install libarrow-dev pkg-config`.

### Setting up your environment

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/ocaml

# Create the two switches (names are examples):
opam switch create 4.14 4.14.1
opam switch create 5.2  5.2.0

# Install each package's dependencies on the matching switch, e.g. on 4.14:
opam install --deps-only ./zerobus-core.opam ./zerobus.opam ./zerobus-async.opam \
  ./zerobus-rest.opam ./zerobus-otlp.opam ./zerobus-arrow.opam
opam install ocaml-protoc ocamlformat.0.29.0 odoc opentelemetry \
  opentelemetry-client-cohttp-lwt
```

> **Note:** `dune build` of the *whole tree* only succeeds on a switch that has both
> runtime stacks installed. In practice you build and test the per-runtime
> directories on the matching switch (see below). This is expected, not a bug — the
> Eio directories don't build on 4.14 and `lib/async/` doesn't build on 5.x.

## Building and testing

Run everything from `ocaml/`. Select the switch with `opam switch set <name>` (or
`eval $(opam env --switch=<name>)`).

**4.14 switch (Lwt + Async, plus REST / OTLP / Arrow):**

```bash
dune build lib/core/ lib/lwt/ lib/async/ lib/arrow/ lib/rest/ lib/otlp/proto/ lib/otlp/
dune test  test/ test_driver/ test_driver_async/ test_driver_arrow/ \
           test_driver_async_arrow/ test_rest/ test_otlp/ test_otlp_otel/
```

**5.x switch (Eio):**

```bash
dune build lib/core/ lib/eio/ lib/rest/eio/ lib/otlp/proto/ lib/otlp/eio/ lib/arrow/
dune test  test_driver_eio/ test_driver_eio_arrow/ test_rest_eio/ \
           test_otlp_eio/ test_otlp_otel_eio/
```

Several test directories spin up a separate-process mock server. Run each directory
on its own (as above / as CI does) rather than `dune test` over the whole tree, which
can hit mock-port contention.

### The bespoke Async switch

The Async live-TLS / OAuth / REST / OTLP tests (`test_driver_async_tls/`,
`test_driver_async_oauth/`, `test_rest_async/`, `test_otlp_otel_async/`) need
`tls-async` (and, for the mock server, `cohttp-async`) layered on the 4.14 stack.
These are `(optional)` and vanish on a switch lacking those deps; the shipped Async
libraries themselves build on the plain 4.14 switch. Build the combined switch by
cloning your 4.14 switch and adding `opam install tls-async.0.17.0 cohttp-async`
(pinning `tls-async` to 0.17.0 keeps the switch on OCaml 4.14). See
[`doc/arch/tls_async_status.md`](doc/arch/tls_async_status.md) for the full rationale.

### Live integration suite

`test_integration/` runs the full runtime × auth-mechanism × interface matrix against
a real workspace. It is env-gated and a clean no-op success when the variables are
unset, so it never blocks an offline build. To run it live, set `DATABRICKS_HOST`,
`DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, and `ZEROBUS_TEST_TABLE` (and
optionally `ZEROBUS_ENDPOINT`), then `dune exec test_integration/test_integration_lwt.exe`
(and the `_eio` / `_async` variants on their switches).

## Coding style

### Formatting

Code style is enforced by a formatter check in CI. We use
[ocamlformat](https://github.com/ocaml-ppx/ocamlformat) 0.29.0 (`.ocamlformat`,
`profile = default`). Only OCaml sources are formatted — `dune` files keep their
hand-formatting.

```bash
dune build @fmt                # check
dune build @fmt --auto-promote # apply
```

### API docs

Public modules carry `.mli` interfaces with doc comments. CI compiles the docs with
odoc, which catches malformed markup:

```bash
dune build @doc   # output under _build/default/_doc/_html/
```

### Interface conventions

- Every public module has an `.mli`.
- Follow the **cardinal ingestion rule** in all examples and docs: queue records in a
  loop, then `flush` once — never wait for an ack after every `ingest`.

## Continuous Integration

CI runs a job per switch (see [`.github/workflows/ci-ocaml.yml`](../.github/workflows/ci-ocaml.yml)):

- **4.14 job** — format (`@fmt`), docs (`@doc`), build, examples, and the 4.14 test
  directories.
- **5.x job** — the same gates for the Eio packages and tests.
- A separate **non-blocking** job
  ([`ci-ocaml-async-bespoke.yml`](../.github/workflows/ci-ocaml-async-bespoke.yml))
  builds the combined `tls-async` + `cohttp-async` switch and runs the Async TLS/OAuth
  tests.

All PR-blocking checks must pass before merge. Run `dune build @fmt` and the relevant
test directories locally first.

## Versioning

Each package is versioned independently under strict [SemVer](https://semver.org/).
Changing a public `.mli` in a non-additive way, or altering user-facing behavior, is a
breaking change and requires a major bump. See the top-level guide and the monorepo
`CLAUDE.md` for the release process.
