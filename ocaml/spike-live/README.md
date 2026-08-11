# Live spike — TLS/ALPN + scoped OAuth against a real workspace

Closes **DESIGN.md next-step 3** and proves **§12.1** (TLS + ALPN `h2`) and
**§12.2** (scoped OAuth) *natively in OCaml* against a real Azure Databricks
workspace — the two transport unknowns the in-process loopback spikes (`spike/`,
`spike-eio/`) deliberately could not cover.

**RESULT: PASS (both).** Captured in [`EVIDENCE_LIVE.txt`](EVIDENCE_LIVE.txt).
Verified on OCaml 4.14.1 (switch `fl414`) with `tls-lwt 0.17.5` + `ca-certs 0.2.3`
+ `cohttp-lwt-unix 6.0.0`.

## What each program proves

| Program | Design ref | Proves | Needs a secret? |
|---|---|---|---|
| `tls_alpn.ml` | §12.1 | Native `tls-lwt` opens TLS 1.3 to the real Zerobus gRPC endpoint, verifies the cert chain against **system trust anchors** (`ca-certs`), and negotiates **ALPN `h2`** (the gRPC precondition). | No — endpoint is public |
| `oauth_token.ml` | §12.2 | Native OCaml (`cohttp-lwt-unix`, TLS via the same backend) performs the client-credentials grant with `scope=all-apis`, the `zerobusDirectWriteApi` resource, and the UC-privilege `authorization_details` array, and receives a real **table-scoped** token. | Yes — SP id+secret (env only) |

Measured facts from the run (see `EVIDENCE_LIVE.txt` for the full capture):

```
§12.1  endpoint 984752964297111.zerobus.eastus2.azuredatabricks.net:443
       tls_version TLS1.3   cert_verified true   alpn_negotiated h2      → PASS
§12.2  token_endpoint .../oidc/v1/token   scope all-apis
       access_token received (len 3318)   expires_in 3600                 → PASS
```

## The §12.1 dependency finding, confirmed live

Installing `tls-lwt`+`ca-certs` into the switch **rebuilt `gluten-lwt-unix` from
`0.5.2`→`0.5.1` with its real `Tls_io`** (`tls_io.real.ml`) in place of the
`tls_io.dummy.ml` (`[ `Tls_not_available ]`) that shipped when no TLS backend was
present. This is the exact mechanism §12.1 predicted: TLS is the one genuinely
required new dependency, and adding it flips gluten's TLS glue from dummy to real.
`ocaml-tls` was chosen (over OpenSSL) so no C/system-lib dep is pulled.

## Running it

Credentials come **only** from env vars — nothing is hard-coded or logged (the
OAuth program prints the token *length*, never its value).

```bash
eval $(opam env --switch=fl414)
cd ocaml/spike-live
dune build

# §12.1 — no credential needed
dune exec ./tls_alpn.exe -- <workspace-id>.zerobus.<region>.azuredatabricks.net 443

# §12.2 — service principal with MODIFY+SELECT on the target table
DATABRICKS_HOST="https://<workspace-host>" \
ZEROBUS_WORKSPACE_ID="<workspace-id>" \
ZEROBUS_CLIENT_ID="<sp-application-id>" \
ZEROBUS_CLIENT_SECRET="<sp-oauth-secret>" \
ZEROBUS_TABLE="catalog.schema.table" \
dune exec ./oauth_token.exe
```

## Scope — what this does and does NOT prove

- **Proves (native, live):** DNS→TCP→TLS1.3 handshake with cert verification and
  ALPN `h2` to the real Zerobus endpoint; and the full scoped-OAuth token grant.
- **Does NOT yet prove:** the actual gRPC `IngestStream` RPC / a real row landing
  in Delta. That needs the authoritative Zerobus `.proto` vendored (DESIGN.md §11
  next-step 1) so the bidi driver can frame real ingest messages. The transport
  and auth *underneath* that RPC are now both proven end-to-end; wiring the real
  proto onto the `grpc-lwt` bidi path already proven in `spike/` is the remaining
  work (next-step 4).

## Live resources created for this spike

These were created in the `azure-field-eng-eastus2` workspace to run §12.2:

- **Service principal:** `zerobus-ocaml-spike-<ts>` (application id `d2cb41fd-…`)
  with an OAuth secret.
- **Table:** `main.default.zerobus_spike_<ts>` (schema `device_name STRING,
  temp INT, ts TIMESTAMP`), granted `USE CATALOG`/`USE SCHEMA`/`MODIFY,SELECT`.

### Teardown

```bash
P=azure-field-eng-eastus2
WH=148ccb90800933a1   # a running SQL warehouse
# drop the table
databricks api post /api/2.0/sql/statements -p $P --json \
  '{"warehouse_id":"'$WH'","wait_timeout":"30s","statement":"DROP TABLE IF EXISTS main.default.zerobus_spike_<ts>"}'
# delete the OAuth secret + service principal
databricks service-principal-secrets-proxy delete <sp-id> <secret-id> -p $P
databricks service-principals delete <sp-id> -p $P
```

(The exact ids for this run are in `EVIDENCE_LIVE.txt`.)
