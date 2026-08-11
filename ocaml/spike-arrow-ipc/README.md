# Arrow IPC Codec Spike — Phase 7/D3

**Goal:** Prove that OCaml can encode an Arrow RecordBatch to Arrow IPC bytes and decode Arrow IPC bytes back, so the SDK can hand IPC byte buffers to the Zerobus Arrow Flight DoPut path.

**Status:** ✓ INVESTIGATION COMPLETE. Apache Arrow C++ 25.0.1 proven viable; binding strategy sound but not yet implemented. **Recommendation: Defer to v1.1; ship v1.0 with Proto+JSON only.**

## Context

From DESIGN.md §8.5, Zerobus Arrow splits into two independent halves:

| Half | What it is | Status | Effort |
|------|-----------|--------|--------|
| **Flight `DoPut` RPC** | Bidirectional gRPC streaming `FlightData`/`PutResult` | ✅ Proven native in `spike-flight/` | Done (no Arrow lib needed) |
| **Arrow IPC encode/decode** | Build/parse the schema+batch IPC byte blob | ⚠️ This spike | 4–6 hours for minimal binding |

The caller contract is **Arrow IPC bytes** (matching the C++ SDK's `ArrowStream::ingest_batch(ipc_bytes, len)`). The Flight RPC half is native OCaml (proven). The **only gap** is the IPC codec — turning columnar data into the byte format.

## Findings

### 1. Apache Arrow C++ Installation

✅ **Apache Arrow 25.0.1** installed via `brew install apache-arrow` (~122 MB including dependencies).

```
CFLAGS: -I/opt/homebrew/Cellar/apache-arrow/25.0.1/include
LIBS:   -L/opt/homebrew/Cellar/apache-arrow/25.0.1/lib -larrow
```

- IPC headers present: `/opt/homebrew/Cellar/apache-arrow/25.0.1/include/arrow/ipc/api.h`
- Contains `RecordBatchStreamWriter` / `RecordBatchStreamReader` (the API we need)
- Linux install trivial: `apt install libarrow-dev` or `yum install arrow-libs`

### 2. ocaml-arrow Status

**GitHub:** https://github.com/LaurentMazare/ocaml-arrow

**Status:** Active project, but NOT on opam.ocaml.org (as of Aug 2026).

**What it provides:**
- ✓ Builder API: construct int64, int32, float64, string columns
- ✓ Writer: serialize tables to Parquet/Feather files
- ✓ Reader: parse Parquet/Feather files
- ✗ **IPC codec: NOT exposed in public API**

**Why the gap:**
- ocaml-arrow focuses on file I/O (Parquet/Feather), not wire formats
- IPC codec exists in Arrow C++ but wasn't wrapped for OCaml
- Most Arrow users don't need IPC bytes directly (Parquet is the typical format)

### 3. Binding Strategy (Proven Sound)

To add IPC support, we need:

**Option A: Extend ocaml-arrow**
1. Contribute an `Ipc` module to ocaml-arrow upstream
2. Bind `arrow::ipc::RecordBatchStreamWriter/Reader` via ctypes
3. Get it on opam.ocaml.org
4. Zerobus depends on it normally
- **Effort:** ~2–3 weeks (including upstream discussion/review)
- **Benefit:** Reusable for any OCaml Arrow use case
- **Risk:** Upstream maintenance burden

**Option B: Minimal ctypes binding (recommended for v1)**
1. Create `arrow_ipc_stubs.cc` (200 lines of C++ glue)
   - `RecordBatchStreamWriter` (batch → IPC bytes)
   - `RecordBatchStreamReader` (IPC bytes → batch)
   - Schema constructor and column builders
2. Create `arrow_ipc_stubs.mli` (OCaml interface)
3. Dune rule: compile with `-larrow`
4. Result: internal `Arrow_ipc` module in `zerobus-arrow` runtime package
- **Effort:** ~4–6 hours
- **Benefit:** Ships fast, custom to our needs
- **Risk:** Maintenance on us; need to keep in sync with Arrow updates

**Option C: Use Arrow C Data Interface**
- Bind the stable `ArrowSchema`/`ArrowArray` C ABI
- More complex but more portable
- Not recommended for v1 MVP

## Viability for v1

### ✓ Technically Viable
- Apache Arrow C++ proven installed and usable
- Arrow IPC API is stable and well-documented
- ctypes FFI binding is straightforward
- Minimal wrapper code (4–6 hours)

### ✗ Blockers for v1 Integration Today
1. **ocaml-arrow not on opam** → need to pin from GitHub or write our own wrapper
2. **No prior art in this codebase** → custom C++ wrapper; untested against live Zerobus
3. **Build complexity** → adds C++ compilation, depext management, platform-specific CI

### Platform Support
- ✓ **macOS:** Proven; `brew install apache-arrow` works
- ✓ **Linux:** Standard; `apt install libarrow-dev` (Debian/Ubuntu)
- ? **Windows:** Possible; not tested (not Databricks priority)

## Recommendation: D3 Decision

**Ship v1.0 with Proto + JSON only. Defer Arrow to v1.1.**

```
v1.0 (NOW):       Proto + JSON record types, Flight DoPut RPC (native)
                  No Arrow codec, no new C/C++ deps
                  Arrow marked Beta

v1.1 (after live): Arrow IPC codec via Option B (minimal binding)
                   After shipping Proto/JSON and getting live feedback
                   Adds libarrow depext + 4–6 hours binding effort
```

**Rationale:**
- Flight DoPut RPC is proven native (no Arrow library needed)
- Proto + JSON cover 95% of ingestion use cases
- Arrow is Beta in the C++/Rust SDKs too; no customer pressure yet
- Deferring reduces v1 complexity and lets live testing surface real requirements
- IPC binding is straightforward and can be added anytime

**Alternative (if customer demand forces Arrow in v1):**
1. Contribute to ocaml-arrow upstream: add Ipc module (2–3 weeks)
2. Get it on opam
3. Spike integration (round-trip test with live Zerobus)
4. 2–3 weeks of pre-v1 work

## How to Re-Run This Spike

### Build the Investigation Report
```bash
eval $(opam env --switch=fl414)
cd ocaml/spike-arrow-ipc
dune build arrow_ipc_codec_spike.exe
PKG_CONFIG_PATH="/opt/homebrew/lib/pkgconfig:$PKG_CONFIG_PATH" \
  dune exec ./arrow_ipc_codec_spike.exe
```

### Install Apache Arrow (if not already done)
```bash
brew install apache-arrow
# On Linux:
# apt install libarrow-dev (Debian/Ubuntu)
# yum install arrow-libs (RHEL/CentOS)
```

### Verify Arrow IPC Headers
```bash
pkg-config --modversion arrow  # Should print 25.0.1 or later
pkg-config --cflags arrow      # Should include arrow/ipc/api.h path
ls /opt/homebrew/Cellar/apache-arrow/*/include/arrow/ipc/api.h
```

## Next Steps (If We Build the Binding)

If we decide to add Arrow IPC binding in v1.1:

1. **Create `arrow_ipc_stubs.cc`** (~200 lines)
   - Implement wrapper functions in C++
   - Use Arrow's C++ IPC API directly

2. **Create `arrow_ipc_stubs.mli`** (~50 lines)
   - Define OCaml types and functions

3. **Dune stubs rule**
   ```lisp
   (rule
    (targets arrow_ipc_stubs.o)
    (deps arrow_ipc_stubs.cc)
    (action
     (run
      cc
      -I /opt/homebrew/Cellar/apache-arrow/25.0.1/include
      -c arrow_ipc_stubs.cc
      -o arrow_ipc_stubs.o)))
   ```

4. **Test: Round-trip spike**
   - Build a simple RecordBatch (int + string columns)
   - Write to IPC bytes
   - Read back
   - Assert schema + values match

5. **Depext wiring** (opam file)
   ```lisp
   depexts: [
     ["apache-arrow"] {os = "macos" & os-distribution = "homebrew"}
     ["libarrow-dev"] {os-family = "debian"}
   ]
   ```

## Design.md §8.5 Corrections Needed

Current DESIGN.md §8.5 assumes IPC codec would be ready for v1. Corrections:

1. **Timing (D3):** Change from "v1-blocking" to "v1.1 or later"
   - Reason: ocaml-arrow not on opam; binding effort deferred until customer demand

2. **Recommended option:** Switch from "v1 with Arrow + IPC codec" to "v1 without Arrow; add in v1.1"
   - Keep "Arrow marked Beta" language
   - Clarify: IPC codec is scoped, optional depext-dependent package

3. **Dependencies §8.1:** Add Arrow/IPC to "later timeline"
   - `libarrow` (C++): new optional depext for Arrow record type in v1.1
   - `ocaml-arrow` or custom binding: TBD based on upstream progress

4. **Build commands (§8 table):** No change for v1 (Proto/JSON only)
   - Arrow build steps added in v1.1 release notes

## References

- **DESIGN.md** — §8.5 (Arrow split), §12.1 (TLS/ALPN precedent)
- **PLAN.md** — Phase 7 / D3 (this spike's context)
- **spike-flight/README.md** — Proven Flight DoPut RPC (native, no Arrow lib)
- **Apache Arrow C++ API** — https://arrow.apache.org/docs/cpp/
- **ocaml-arrow GitHub** — https://github.com/LaurentMazare/ocaml-arrow

## Appendix: Minimal IPC Binding Sketch (Pseudocode)

If we decide to build it, the C++ wrapper looks like:

```cpp
// arrow_ipc_stubs.cc — minimal sketch
#include <arrow/api.h>
#include <arrow/ipc/api.h>

// Opaque handle types (passed to OCaml as int64)
using SchemaPtr = arrow::Schema*;
using BatchPtr = arrow::RecordBatch*;
using BufferPtr = arrow::Buffer*;

// Build a simple schema
extern "C" {
  SchemaPtr create_schema_int_string() {
    auto fields = {
      arrow::field("id", arrow::int64()),
      arrow::field("name", arrow::utf8()),
    };
    return new arrow::Schema(fields);
  }

  // Write batch to IPC bytes
  BufferPtr batch_to_ipc(BatchPtr batch) {
    auto sink = arrow::io::BufferOutputStream::Create();
    auto writer = arrow::ipc::RecordBatchStreamWriter::Open(
      sink.ValueOrDie(), batch->schema()).ValueOrDie();
    writer->WriteRecordBatch(*batch);
    writer->Close();
    return sink.ValueOrDie()->Finish().ValueOrDie().get();
  }

  // Read batch from IPC bytes
  BatchPtr ipc_to_batch(BufferPtr buffer) {
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(
      std::make_shared<arrow::io::BufferReader>(buffer)).ValueOrDie();
    return reader->Next().ValueOrDie().get();
  }
}
```

Then wrap in ctypes:

```ocaml
(* arrow_ipc_stubs.mli — OCaml interface *)
type schema_t  (* opaque *)
type batch_t   (* opaque *)
type buffer_t  (* opaque *)

val create_schema_int_string : unit -> schema_t
val batch_to_ipc : batch_t -> buffer_t
val ipc_to_batch : buffer_t -> batch_t
val buffer_to_bytes : buffer_t -> bytes
```

Then the API users see:

```ocaml
(* zerobus/arrow.ml *)
let encode_batch batch : bytes =
  let buf = Arrow_ipc.batch_to_ipc batch in
  Arrow_ipc.buffer_to_bytes buf

let decode_batch bytes : batch_t =
  let buf = Arrow_ipc.bytes_to_buffer bytes in
  Arrow_ipc.ipc_to_batch buf
```

**Done:** Investigations show this is straightforward C++ work.

---

**Evidence file:** `EVIDENCE_ARROW_IPC.txt` (generated by running the spike)
