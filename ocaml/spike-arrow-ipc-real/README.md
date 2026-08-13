# Arrow IPC codec spike (Phase 7b / D3) — REAL, compiled-and-run

**Purpose.** Close the one genuine gap in native OCaml Arrow support
(DESIGN.md §8.5.2): turning columnar data into the **Arrow IPC byte blob** and
back. The Flight `DoPut` RPC that carries those bytes is already native OCaml
(`spike-flight/`), so this codec is all that stands between the SDK and full v1
Arrow support. This is the one v1 component with a real new C++ dependency
(`libarrow`).

## RESULT: ✅ COMPILED, LINKED against real libarrow, and RUN green

```
rows                   : 5
ipc_bytes              : 504
ipc_stream_magic       : 0xFFFFFFFF (stream continuation)
roundtrip              : FAITHFUL (ids + utf8 strings, incl. empty + unicode)
ALL CHECKS PASSED
```

- The OCaml executable **actually links libarrow** —
  `otool -L …/test_arrow_ipc.exe` shows `libarrow.2500.dylib` (Apache Arrow
  25.0.1). Verify yourself; do not take it on faith.
- The round-trip crosses the real boundary: **OCaml → C++ → Arrow
  `RecordBatchStreamWriter` → IPC bytes → `RecordBatchStreamReader` → C++ →
  OCaml**, and asserts schema + every value (an int64 `id` column and a UTF-8
  `name` column, including an empty string and a non-ASCII value).
- The IPC magic asserted is the **real** Arrow *stream* continuation marker
  `0xFFFFFFFF` — not merely "bytes are non-empty".
- Stable across repeated runs. Evidence: `EVIDENCE_ARROW_IPC.txt`.

> Note: two earlier agent-built attempts (`spike-arrow-ipc/`,
> `spike-arrow-ipc-codec/`) did **not** prove this — the first was investigation
> only, the second linked no libarrow and crashed on its first line while claiming
> success. This directory is the real one.

## Files

```
spike-arrow-ipc-real/
├── README.md
├── dune-project
├── dune                  # foreign_stubs (cxx, -std=c++20) + pkg-config arrow flags
├── arrow_ipc_stubs.cc    # REAL Arrow C++ IPC: encode/decode + OCaml-runtime stubs
├── arrow_ipc.ml          # OCaml binding (external) + results-over-exceptions API
├── test_arrow_ipc.ml     # round-trip test with schema+value + magic-byte assertions
└── EVIDENCE_ARROW_IPC.txt
```

## How to build & run

```sh
eval $(opam env --switch=fl414)
export PKG_CONFIG_PATH="/opt/homebrew/lib/pkgconfig:$PKG_CONFIG_PATH"
cd ocaml
dune build ./spike-arrow-ipc-real/test_arrow_ipc.exe
# run (macOS has no `timeout`; use a bg PID + kill -9 watchdog if you want a cap):
$(find _build -name test_arrow_ipc.exe | head -1)
```

Prereqs: `brew install apache-arrow` (25.x; provides headers + `libarrow.dylib`
and the `arrow.pc` pkg-config file). Linux: `apt install libarrow-dev`.

## API drift corrected (things only a real build catches)

- **Arrow 25.0.1 headers require C++20.** `arrow/buffer.h` uses `std::span` and
  `arrow/util/bit_util.h` uses `std::log2p1` — both C++20. `-std=c++17` fails to
  compile; `-std=c++20` is required. (The fake spike "compiled" under c++17 only
  because it never included Arrow.)
- **ocamlopt link flags:** bare `-L…/-larrow` are not ocamlopt options; each must
  be passed as `-cclib <tok>`. The `dune` rule emits the pkg-config `--libs` output
  as `(-cclib … -cclib …)` for exactly this reason.
- **Homebrew path is version-specific** (`…/apache-arrow/25.0.1/…`), so the `dune`
  captures pkg-config output into `*.sexp` at build time and `(:include)`s it,
  rather than hardcoding a Cellar path.
- Arrow IPC **stream** format (`MakeStreamWriter` / `RecordBatchStreamReader`) is
  what Zerobus's `ArrowStream::ingest_batch(ipc_bytes)` contract wants (schema
  message + record-batch message in one self-contained blob); its first 4 bytes
  are the `0xFFFFFFFF` continuation marker (the *file* format's `ARROW1` magic is a
  different framing we do not use).

## What this de-risks, and integration next steps

- **De-risks the one v1 C++ dependency.** `libarrow` links and the IPC codec works
  from OCaml. §8.5.2's binding approach (minimal C-ABI stub over Arrow C++ IPC) is
  proven, not just designed.
- **Integration (Phase 7b):** move `arrow_ipc_stubs.cc` + `arrow_ipc.ml` into an
  `Arrow_ipc` module inside the runtime packages, isolated behind `.mli` so
  Proto/JSON builds never pull `libarrow` (opam depext `optional`); feed the IPC
  bytes into `FlightData.data_body` on the proven native Flight `DoPut` path
  (`spike-flight/`); real API takes a caller-supplied Arrow schema rather than the
  fixed int+string schema this spike uses to prove the mechanism.
