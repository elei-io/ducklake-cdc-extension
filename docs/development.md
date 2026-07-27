# Development

Short local loop for working on the C++ extension.

## First setup

```bash
git submodule update --init --recursive
```

The root `Makefile` includes `extension-ci-tools/makefiles/duckdb_extension.Makefile`. The `duckdb/` and `extension-ci-tools/` directories are Git submodules pinned to versions compatible with this extension.

## Supported DuckDB targets

The active development target is **DuckDB v1.5.5**, with SQL coverage validated
across DuckDB **v1.5.0**, **v1.5.1**, **v1.5.2**, **v1.5.4**, and **v1.5.5**.
Keep the version tuple explicit:

- `duckdb/` submodule: DuckDB `v1.5.5`
- `extension-ci-tools/` submodule and reusable workflows: `v1.5.5`
- `extension_config.cmake`: loads the official DuckLake runtime binary built
  for DuckDB `v1.5.5`

Validated 1.5.x DuckLake catalog formats:

- DuckDB `v1.5.0` and `v1.5.1`: DuckLake catalog format `0.4`
- DuckDB `v1.5.2` and `v1.5.4`: DuckLake catalog format `1.0`

DuckDB extension binaries are version-specific, so "supported" means a full
tuple has been validated. Do not mark another DuckDB version supported just
because the C++ compiles locally.

### Validating an older DuckDB version

Add older DuckDB targets only after the active target is green. For a candidate
version `XX`:

1. Create a short-lived branch from `main`, for example
   `feature_support_duckdb_XX`.
2. Pin `duckdb/` to DuckDB `XX` and `extension-ci-tools/` to the matching
   `XX` branch documented by DuckDB's extension CI tools.
3. Pin `extension_config.cmake` to a DuckLake commit that compiles against
   DuckDB `XX`; never leave this dependency floating.
4. Run a clean local build and test loop:
   `make clean`, `make debug`, `make test_debug`, and `make test`.
5. Exercise the loadable extension in the built DuckDB shell:
   `LOAD ducklake; LOAD parquet; LOAD ducklake_cdc;`, then attach a real
   DuckLake catalog and run the CDC smoke path.
6. Run the Python smoke probes in `e2e/smoke/` for behaviours SQLLogicTest
   cannot express, especially notices, leases, waits, and backend-specific
   catalog behaviour. Run `e2e/smoke/enumerate_changes_map.py --check` when the
   DuckLake dependency changes.
7. Add a CI matrix leg for `XX` and require full CI to pass on the branch.
8. Update the compatibility docs and release notes with the exact tuple:
   DuckDB `XX`, extension-ci-tools `XX`, DuckLake commit, observed DuckLake
   catalog format, supported platforms, and any caveats.

Only after those steps pass should `XX` appear in a supported-version matrix.
If the candidate needs source changes, keep them behind the same validation
branch and merge them through the normal PR gate.

## Build

```bash
make debug
```

Configures and builds DuckDB plus the `ducklake_cdc` extension under `build/debug/`.

Use the bundled DuckDB binary for ad-hoc SQL:

```bash
./build/debug/duckdb
```

One-shot example:

```bash
./build/debug/duckdb -unsigned -c "SELECT cdc_version();"
```

(`-unsigned` matches typical local extension loads; adjust if your setup differs.)

## Test

Run the full local SQLLogicTest set against the **release** build:

```bash
make test_local_full
```

Run the sanitizer-friendly debug smoke:

```bash
make test_local_sanitizer
```

Debug builds enable ASan/UBSan and cannot load the official prebuilt
`ducklake.duckdb_extension`, so full DuckLake SQL coverage runs through the
release target.

Run a single SQLLogicTest file:

```bash
build/release/test/unittest --test-dir . "test/version_and_load.test"
```

Swap the path for any file under `test/`.

## Formatting

CI runs the DuckDB extension-template formatter gate:

```bash
make format-check
```

Apply the same formatter locally before committing:

```bash
make format-fix
```

The formatter requires `clang-format 11.0.1` on `PATH` and the Python
formatter dependencies used by DuckDB's `duckdb/scripts/format.py`.

To enable the local pre-commit hook:

```bash
make install-git-hooks
```

The hook runs `make format-check` with a formatter toolchain bootstrapped into
the ignored `.cache/pre-commit/` directory. That venv provides
`clang_format==11.0.1`, `black==24.10.0`, and `cmake-format`, so the check does
not depend on the developer's system `clang-format` version.

## Python smoke probes

Some behaviours are easier to smoke-test from Python (stderr notices,
multi-connection leases, explicit interrupts, etc.). Dependencies live in the root `pyproject.toml` (Python **3.14+**, for `ducklake-client`); run from the repository root:

```bash
uv run python e2e/smoke/lease_multiconn_smoke.py
uv run python e2e/smoke/enumerate_changes_map.py --check
```

See [`e2e/smoke/README.md`](../e2e/smoke/README.md) for the full lists (extension smoke scripts plus the DuckLake MAP-key contract probe).

## Branch and CI flow

The branch flow is:

```text
feature_* -> main -> manual release
```

- Work on a short-lived branch created from `main`.
- Feature branches run day-to-day CI for fast feedback.
- Open the PR into `main`.
- `main` is protected and requires the required CI gate before merge.
- Releases are manual actions from `main` or the relevant maintenance branch.
- `release/0.x` branches are created only when an already-published line needs
  patch maintenance after `main` has moved on.

### Immutable release binaries

Every non-dry-run release attaches the exact full-matrix binaries to its GitHub
Release. Asset names include the extension version, DuckDB version, and DuckDB
platform, for example:

```text
ducklake_cdc-v0.6.1-duckdb-v1.5.4-linux_arm64.duckdb_extension
```

`SHA256SUMS` in the same release pins their contents. These maintainer release
assets are unsigned: production images that use them must verify the digest,
enable `allow_unsigned_extensions`, load the fixed path, and prevent the Python
client from reinstalling the moving community build. The community repository
remains the signed latest-version distribution channel.

`cdc_version()` reports the stable extension semantic version.
`cdc_build_revision()` reports the source revision stamped by the build. Treat
the release URL plus SHA-256 digest—not either SQL string alone—as the immutable
artifact identity.

## Where to start

- `src/ducklake_cdc_extension.cpp` is the extension entry point. It registers
  `cdc_version()` / `cdc_build_revision()` and calls into the per-domain
  `Register*Functions`.
- `src/include/ducklake_metadata.hpp` is the shared facts layer (catalog
  table-name builders, snapshot lookups, JSON / quoting helpers, state-table
  DDL, lazy bootstrap). `src/ducklake_metadata.cpp` implements it.
- `src/include/consumer.hpp` + `src/consumer.cpp` own the consumer state
  machine (DDL/DML create, reset, drop, list, force-release, heartbeat) and the
  cursor primitives (`cdc_window` / `cdc_commit`), plus the lease, audit-writer,
  listen/wait helpers, and notices.
- `src/include/ddl.hpp` + `src/ddl.cpp` own the schema-change reads
  (`cdc_ddl_changes_*`, `cdc_ddl_ticks_*`, and stateless DDL queries) and the
  per-snapshot DDL extraction / column-diff machinery.
- `src/include/dml.hpp` + `src/dml.cpp` own the row-level reads
  (`cdc_dml_changes_*`, typed table DML reads, DML ticks, and stateless DML
  queries).
- `src/include/stats.hpp` + `src/stats.cpp` own the observability surface
  (`cdc_consumer_stats`, `cdc_audit_events`, `cdc_doctor`).
- `test/version_and_load.test` is the smallest extension smoke test.
- `test/consumer_lifecycle.test` covers create/list/reset/lease/drop primitives.
- `test/dml_ticks.test` and `test/dml_changes.test` cover the DML metadata and
  row payload surfaces.
- `test/dml_schema_shape_pinning.test` is the best behavioural spec for the
  DML cursor and schema-boundary contract.
- `test/ddl_ticks.test`, `test/ddl_changes.test`, and `test/schema_diff.test`
  cover the DDL metadata, parsed change, and column diff surfaces.
- `test/observability.test` covers stats, audit log, and doctor surfaces.
- `test/retention_gap.test` covers `CDC_GAP` and reset recovery after DuckLake
  snapshot expiry.
- `e2e/smoke/README.md` documents Python smoke tools that do not fit
  SQLLogicTest well, including the DuckLake `snapshots().changes` MAP contract
  probe (`enumerate_changes_map.py`).

For a minimal SQL walk-through of primitives, use the quickstart block in the
root [`README.md`](../README.md).
