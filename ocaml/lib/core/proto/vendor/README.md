# Vendored Zerobus wire schema

These `.proto` files are vendored **verbatim** from their upstream sources so the
OCaml SDK generates its wire types from the same schema every other SDK uses.

| File | Upstream source | License |
|------|-----------------|---------|
| `zerobus_service.proto` | `rust/sdk/zerobus_service.proto` (the repo's single source of truth for the Zerobus gRPC schema) | Apache-2.0 (repo root `LICENSE`) |
| `google/protobuf/duration.proto` | Google well-known type; imported by `zerobus_service.proto` | BSD-3-Clause (Google) |

**Keep these byte-identical to their sources.** To refresh after an upstream
change:

```sh
cp ../../../../../rust/sdk/zerobus_service.proto zerobus_service.proto
```

CI can assert `diff rust/sdk/zerobus_service.proto ocaml/lib/core/proto/vendor/zerobus_service.proto`
is empty to catch drift.

## Codegen note (D1 gate A finding)

`ocaml-protoc` 4.1 cannot parse proto2 `reserved` statements — it errors at the
first `reserved "stream_id"; reserved 2;` line. Those statements are comment-only
(they reserve retired field names/numbers) and have **no wire effect**, so the
`dune` rule in `../dune` strips `reserved` lines into a build-tree copy before
running `ocaml-protoc`. The vendored file here keeps them, remaining a faithful
mirror of the source of truth. No other transform is applied.

Proven at codegen time: both files generate and compile, and the full
`EphemeralStream` message family (oneof payloads + the imported `Duration`)
round-trips through the binary codec — see `ocaml/test/test_proto_roundtrip.ml`.
