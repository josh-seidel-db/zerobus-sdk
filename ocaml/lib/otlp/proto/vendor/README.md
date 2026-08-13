# Vendored OpenTelemetry protos

These `.proto` files are vendored **verbatim** from the upstream
[`opentelemetry-proto`](https://github.com/open-telemetry/opentelemetry-proto)
repository so `Zerobus_otlp` (DESIGN §4.3) generates its OTLP wire types from the
canonical schema. They are *separate* from the Zerobus wire protos
(`../../core/proto/vendor/`) and vendored independently, as §4.3 requires.

**Upstream:** `open-telemetry/opentelemetry-proto` tag **`v1.3.2`**, path
`opentelemetry/proto/...`. **License:** Apache-2.0 (each file carries the
OpenTelemetry Authors Apache-2.0 header).

| File | Upstream path |
|------|---------------|
| `opentelemetry/proto/common/v1/common.proto` | `opentelemetry/proto/common/v1/common.proto` |
| `opentelemetry/proto/resource/v1/resource.proto` | `opentelemetry/proto/resource/v1/resource.proto` |
| `opentelemetry/proto/logs/v1/logs.proto` | `opentelemetry/proto/logs/v1/logs.proto` |
| `opentelemetry/proto/metrics/v1/metrics.proto` | `opentelemetry/proto/metrics/v1/metrics.proto` |
| `opentelemetry/proto/collector/logs/v1/logs_service.proto` | `.../collector/logs/v1/logs_service.proto` |
| `opentelemetry/proto/collector/metrics/v1/metrics_service.proto` | `.../collector/metrics/v1/metrics_service.proto` |

The `collector/*` files define the two unary RPCs the exporter uses:
`LogsService.Export` and `MetricsService.Export`. The other four are their
transitive dependencies (common → resource → logs/metrics → collector).

**Keep these byte-identical to upstream.** To refresh after an upstream bump,
re-fetch each file from the pinned tag:

```sh
BASE=https://raw.githubusercontent.com/open-telemetry/opentelemetry-proto/v1.3.2/opentelemetry/proto
curl -fsSL "$BASE/common/v1/common.proto" -o opentelemetry/proto/common/v1/common.proto
# ...and the other five, preserving the paths above.
```

## Codegen notes

Unlike the Zerobus proto2 schema (whose string-form `reserved "name";`
statements `ocaml-protoc` 4.1 cannot parse), these proto3 files use only numeric
`reserved` statements, which `ocaml-protoc` 4.1 parses directly — so **no
transform is applied**; the `dune` rule in `../dune` runs `ocaml-protoc` straight
against the vendored files with `-I vendor` to resolve imports.

`common.proto` defines `AnyValue`/`ArrayValue`/`KeyValueList` with a shared
`values` record label across types, so the generated `Common` module trips
OCaml's warning 30 (duplicate-definitions); the proto library disables `-30`.
Proven at codegen time: all six files generate and compile together as one
library.
