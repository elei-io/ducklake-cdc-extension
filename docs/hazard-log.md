# Hazard Log

This log records known project risks. It is deliberately lightweight: a hazard
can be handled by tests/docs/code, partially handled, not handled yet, or
accepted as a known limitation.

The hazard log is not the roadmap. The roadmap says where the project wants to
go; this file says what can hurt users or maintainers on the way there.

## Status Values

- `handled` - covered by shipped behavior, tests, and/or clear docs.
- `partially handled` - there is meaningful coverage, but known gaps remain.
- `not handled` - known risk with no real mitigation yet.
- `accepted` - intentionally left as a limitation for now.

## Hazards

### H-001: Catalog Backend Parity

- Risk: DuckDB, SQLite, and PostgreSQL DuckLake catalogs may diverge in metadata
  encoding, transaction behavior, or extension-visible semantics.
- Status: partially handled.
- Handling: SQL tests, smoke probes, and the user-facing demo CI gates exercise
  the primary catalog paths. Targeted backend probes should be added when a
  concrete portability bug appears.
- Next action: Add targeted backend tests only when a concrete portability bug
  appears or a user-reported workflow depends on it.

### H-002: Lease Correctness

- Risk: Two readers could process or commit the same consumer window if the
  owner-token lease is wrong.
- Status: partially handled.
- Handling: `e2e/smoke/lease_multiconn_smoke.py`,
  `test/consumer_lifecycle.test`, and `test/dml_schema_shape_pinning.test`
  cover same-connection idempotence, second-reader rejection, force release,
  and stolen-lease commit failure.
- Notes: `cdc_consumer_force_release` is only for a holder that is
  demonstrably dead. A longer `lease_interval_seconds` gives long batches more
  room, but also makes dead holders take longer to clear.
- Next action: Keep new lease tests focused on real bugs: timeout precision,
  backend-specific timestamp behavior, and recovery after process death.

### H-003: Schema Boundary Ordering

- Risk: A consumer can apply rows under the wrong downstream schema if DDL and
  DML are delivered in the wrong order or a window crosses a schema boundary
  unexpectedly.
- Status: handled for the SQL extension surface.
- Handling: `cdc_window` exposes `schema_changes_pending`,
  `cdc_ddl_changes_read` emits typed DDL events, and
  `test/always_breaks.test` plus
  `test/ddl_stage2.test` cover schema boundaries and DDL-before-DML
  ordering.
- Next action: Revisit when a client library interleaves DDL and DML into one
  stream.

### H-004: Typed DDL Extraction Edge Cases

- Risk: Rename, nested-column, or combined schema changes can produce confusing
  or duplicate DDL events.
- Status: partially handled.
- Handling: `test/ddl_stage2.test` covers the `snapshots().changes` MAP
  source, rename deduplication, snapshot-bound object lookup, and nested-column
  parent/child ordering.
- Next action: Add examples before adding more exhaustive cross-products. The
  goal is confidence in common migrations, not a full DuckLake spec clone.

### H-005: Compaction Gap Recovery

- Risk: A lagging consumer may point at snapshots expired by DuckLake
  maintenance and silently miss changes.
- Status: handled for the main gap path.
- Handling: `cdc_window` raises `CDC_GAP`, `cdc_consumer_reset` supports
  recovery, and `test/retention_gap.test` covers the compaction gap path.
  Planned stateless range helpers must apply the same explicit gap
  handling when `from_snapshot` is older than the oldest available snapshot.
- Next action: Keep operator docs clear that retention must exceed expected
  consumer lag.

### H-006: Listen Connection Starvation

- Risk: Long-polling holds a DuckDB connection and can starve shared pools or
  interactive sessions.
- Status: handled by docs and warning.
- Handling: listen functions emit `CDC_WAIT_SHARED_CONNECTION`, clamp excessive
  timeouts, use level-triggered checks before waiting, and
  `e2e/smoke/cdc_wait_interrupt_smoke.py` covers interruptibility.
- Notes: SQL users should hold a dedicated connection for listen calls. Batch
  jobs that wake up on a schedule should call `cdc_window` directly instead of
  long-polling.
- Known false positive (high-level Python client): `DMLConsumer` and
  `DDLConsumer` derive and own a dedicated DuckDB connection on
  `__enter__` (via `lake.connection.cursor()`) and hold it for the
  consumer's lifetime, so the operator-facing assumption baked into
  the warning ("you might be sharing this connection") is already
  false on that code path. The advisory still fires because
  `MaybeEmitWaitSharedConnectionWarning` in `src/consumer.cpp` is
  unconditional on first listen per connection -- the extension has
  no way to introspect that the caller has guaranteed dedication.
  Operators running `e2e/01_pipeline_dag` see one advisory per stage
  on startup (3 in v2, growing with stage count); the message is
  noise, not a problem signal.
- Next action: Client libraries should hide this by using dedicated wait
  connections (already done for the high-level Python client). To make
  the advisory match reality, add a per-connection opt-out the high-
  level client can set on the dedicated connection it owns -- e.g. a
  session-local pragma `SET cdc_dedicated_listen_connection = true`
  that suppresses `MaybeEmitWaitSharedConnectionWarning` for that
  `connection_id`. Alternative: make the warning conditional on
  having seen a non-CDC query on the same connection (requires
  per-connection bookkeeping in the extension and is the heavier
  fix).

### H-007: Sink Failure Semantics

- Risk: Failed sink writes, especially failed DDL, need a durable operator flow;
  otherwise a single bad event can either halt work or create a flood of bad
  downstream writes.
- Status: intentionally client-owned.
- Handling: The extension exposes at-least-once windows and only advances a
  cursor when `cdc_commit` succeeds. It does not persist a generic failure
  queue because only the client/sink can classify whether a failure belongs to
  a window, snapshot, table, DDL event, row, or user callback.
- Next action: Let the Python client and reference sinks prove retry,
  idempotency, validation, and quarantine patterns before adding any shared
  failure persistence.

### H-008: No Client Libraries or Reference Sinks

- Risk: Users must compose the SQL primitives themselves, including commit
  timing, DDL/DML ordering, heartbeats, and sink failure behavior.
- Status: accepted.
- Handling: The SQL API and examples are the supported surface today.
- Next action: Let actual usage decide whether the first client should be a
  Python package or a smaller command-line helper.

### H-009: Performance Claims

- Risk: Early benchmark numbers can be mistaken for production promises.
- Status: partially handled.
- Handling: `e2e/ci_demo_assertions.py` applies conservative performance floors
  to demos whose story depends on throughput or latency, while docs describe
  those numbers as smoke signals rather than contracts.
- Next action: Publish numbers as observations with commit/hardware context;
  avoid hard performance contracts until repeated runs justify them.

### H-010: Public Docs Drift

- Risk: The README, roadmap, examples, and actual community release state can
  drift, giving users a false first impression.
- Status: partially handled.
- Handling: The roadmap now points to this hazard log, and the README should be
  refreshed whenever a release changes what users can install or try.
- Next action: Treat stale public status text as a release bug.

### H-011: Inlined DML Discovery

- Risk: DuckLake's inlined-data path uses `inlined_insert` /
  `inlined_delete` keys instead of the regular DML map keys. Consumers that
  discover work from `snapshots().changes` and forget those keys can silently
  skip small batches.
- Status: handled for the extension surface.
- Handling: The extension's discovery logic checks the inlined keys, and
  `e2e/smoke/enumerate_changes_map.py` tracks the observed DuckLake key
  set.
- Next action: Keep any new discovery/filtering code covered by the upstream
  key probe or a focused SQL test.

### H-012: Inline-on-Inline Delete Visibility

- Risk: DuckLake can represent deletes against inlined rows by updating catalog
  row visibility rather than advertising a normal DML key. A consumer that uses
  DML ticks alone as "is there row payload work?" can miss rows that DML changes
  reads would still return.
- Status: partially handled.
- Handling: DML changes reads remain the source of truth for row payloads. The
  conservative rule is: do not skip a DML changes read just because a tick
  stream looks empty.
- Next action: Add a targeted test only if this shows up as a real user-facing
  confusion point.

### H-013: Inlining Limit Assumptions

- Risk: Tests or examples copied from old prototypes may assume
  `DATA_INLINING_ROW_LIMIT = 100`, but DuckLake's default is 10. That can make
  tests miss the inlined-data path entirely.
- Status: handled in upstream probes.
- Handling: `e2e/smoke/enumerate_changes_map.py` sets
  `DATA_INLINING_ROW_LIMIT = 10` explicitly.
- Next action: Set `DATA_INLINING_ROW_LIMIT` explicitly in any test that cares
  about inlined-vs-materialized behavior.

### H-014: Dropped Columns in Same-Snapshot DML

- Risk: If one DuckLake commit both drops a column and deletes or updates rows
  in the same table, typed table DML reads use the end-snapshot schema and do
  not include the dropped column's old values.
- Status: accepted.
- Handling: This is DuckLake `table_changes` behavior, not an extension bug.
  The recovery path for audit use cases is DuckLake time travel: read the table
  at the snapshot before the drop and join the preserved column values back to
  the affected DML events.
- Notes: Schema-boundary handling does not help when the DDL and DML are in the
  same snapshot. Consumers that need the old values must recover them before
  retention expires the pre-drop snapshot.
- Next action: Keep this as a known limitation unless a real audit workflow
  needs a helper around the time-travel recipe.

### H-015: Sibling Metadata Schema State

- Risk: Moving CDC persistence out of DuckLake-managed tables and into a
  sibling schema in the metadata catalog removes DuckLake snapshot overhead, but
  it also means CDC state is no longer protected by DuckLake's table DDL,
  catalog-versioning, or data-type translation layer.
- Status: partially handled.
- Handling: CDC state now lives in the metadata catalog, using
  `__ducklake_metadata_<catalog>.__ducklake_cdc` where schemas are supported and
  a prefixed-table fallback for SQLite. Repeated string fields use portable JSON
  text because SQLite does not preserve DuckDB LIST columns through the scanner
  layer.
- Notes: DuckDB and SQLite have focused smoke coverage. PostgreSQL still needs
  the same focused probe before this is treated as fully portable.
- Next action: Add explicit cleanup/migration coverage for the state tables.

### H-016: CDC State and DuckLake Snapshot Atomicity

- Risk: Once CDC state lives outside DuckLake-managed tables, a `cdc_commit`
  updates metadata-catalog state without creating a DuckLake snapshot. This is
  the performance goal, but it weakens the current "all state is ordinary
  DuckLake data" consistency story and can expose ordering bugs around producer
  commits, consumer commits, rollbacks, and retries.
- Status: partially handled.
- Handling: Hot-path state writes are intentionally narrow metadata-catalog
  updates, and `cdc_window` / `cdc_commit` continue resolving committed
  snapshot ids against DuckLake before returning or advancing the cursor.
- Notes: SQLite cannot safely hold a direct metadata write transaction open
  while DuckLake reads snapshot metadata from the same backend, so lease and
  commit writes are not grouped with the later snapshot scan. This preserves the
  at-least-once SQL contract but weakens the old "one DuckLake table
  transaction" story.
- Next action: Add restart/race tests for commit idempotence, stolen-lease
  rejection, and recovery after process death.

### H-017: CDC State Discoverability and Migration

- Risk: Users and operators can currently inspect CDC state as DuckLake tables
  under `main`. A sibling metadata schema changes where state lives, which can
  break ad-hoc runbooks, backups, and upgrades from earlier releases.
- Status: partially handled.
- Handling: Preserve the public table functions (`cdc_consumer_stats`,
  `cdc_audit_events`, and lifecycle functions) as the supported inspection
  surface instead of asking users to query storage tables directly.
- Notes: Existing `__ducklake_cdc_*` DuckLake tables may exist in early user
  catalogs. The migration path must be explicit: either one-way copy into the
  sibling schema with clear ownership, or a documented reset path for pre-1.0
  catalogs.
- Next action: Add an upgrade test that starts from the old DuckLake-table
  layout.

### H-018: Metadata State Retention

- Risk: CDC-owned metadata tables are no longer DuckLake-managed data, so
  DuckLake cleanup, compaction, and snapshot expiration will not prune them.
  `__ducklake_cdc_audit` can grow without bound.
- Status: partially handled.
- Handling: `__ducklake_cdc_consumers` is bounded by the number of named
  consumers. The current unbounded table is `__ducklake_cdc_audit`, which
  appends lifecycle and lease-recovery events.
- Notes: Heartbeats and commits should remain updates to
  `__ducklake_cdc_consumers`, not append-only audit events, unless there is a
  retention policy in place. Any future shared failure-persistence path must
  ship with acknowledge/replay/prune semantics.
- Next action: Add an explicit maintenance surface, such as audit pruning by
  age and observability for CDC metadata table row counts, before treating
  long-lived catalogs as production-ready.

### H-019: Stateless Query Semantics

- Risk: `*_query` functions may appear equivalent to consumer reads, but they
  intentionally do not acquire leases, apply consumer subscription filters, move
  cursors, or enforce consumer schema-boundary policy.
- Status: not handled.
- Handling: The API docs distinguish durable replay from stateless queries.
- Next action: Add TDD coverage for query gap handling, `to_snapshot := NULL`,
  DDL/DML ordering, and proof that stateless queries do not mutate consumer
  state.

### H-020: Diagnostic False Confidence

- Risk: `cdc_doctor` can make operators trust an incomplete health report,
  especially if new hazards are added without updating doctor checks.
- Status: not handled.
- Handling: `cdc_doctor` is planned as an advisory table function, not a proof
  of correctness.
- Next action: Keep doctor checks tied to concrete hazards: gap risk, stale
  leases, dropped or renamed subscriptions, metadata presence, catalog
  compatibility, suspicious lag, and recent reset or force-release audit events.

### H-021: High-Level Auto Commit Bypasses Sink Gating

- Risk: Python high-level consumers may expose `auto_commit=True` while also
  delivering to sinks. In that mode the SQL listen/read function can commit
  before required sinks acknowledge the batch, so the sink layer no longer
  controls at-least-once delivery.
- Status: accepted.
- Handling: This is a known limitation for the first Python client shape. The
  draft documents that `auto_commit=True` passes through to SQL commit behavior
  and bypasses sink-gated commit safety.
- Next action: Revisit only if users are likely to trip over this in the high
  level API; possible mitigations include forbidding `auto_commit=True` with
  required sinks or renaming it to make the unsafe ordering obvious.

### H-022: Cross-Connection Metadata Lock Handoff on First Bootstrap Write

- Risk: The first cdc_* call against a catalog in a DuckDB process is
  the one that bootstraps `__ducklake_cdc.*` (`CREATE SCHEMA IF NOT
  EXISTS` + `CREATE TABLE IF NOT EXISTS` × 3) against the attached
  metadata database (`__ducklake_metadata_<catalog>`). When the
  immediately-preceding user statement is a read on that same attached
  metadata database — the canonical shape being `SET VARIABLE x =
  (SELECT max(snapshot_id) FROM __ducklake_metadata_<catalog>
  .ducklake_snapshot)` followed by `cdc_*_consumer_create(..., start_at
  := getvariable('x'))` — the same caller thread ends up running the
  extension's bootstrap writes on a newly-opened internal
  `duckdb::Connection` against the same attached database whose catalog
  machinery the outer `ClientContext` still holds state on. Release-matrix
  MSVC has also failed when the first bootstrap used `start_at := 'now'` with
  no user metadata read immediately before; the minimal trigger is not fully
  characterised — treat any first write-path `cdc_*` after DuckLake activity on
  the outer connection as suspicious on MSVC until stack evidence exists. The two
  platforms surface this differently:
  - Windows MSVC's error-checking `std::mutex::lock()` detects the
    resulting same-thread re-entry of a shared catalog/attachment
    mutex and throws `std::system_error(errc::
    resource_deadlock_would_occur, "resource deadlock would occur")`,
    which the test harness prints as
    `Invalid Error: resource deadlock would occur: resource deadlock
    would occur` (doubled-message shape is the MSVC STL fingerprint of
    `std::system_error`).
  - Windows MinGW's C runtime historically surfaced a related file-
    locking variant — `LockFileEx` / `ERROR_POSSIBLE_DEADLOCK` mapped
    to `EDEADLK`, printed as `Resource deadlock avoided` — on
    SQLite-backed metadata. Linux, macOS, and Wasm don't error-check
    `std::mutex` re-entry on the same thread, so the same code path
    slides past silently on those platforms.
  - macOS (libc++/libpthread) has now also been observed surfacing the
    race during DuckDB's parallel-pipeline teardown: when an exception
    from inside the bootstrapping query fires while a worker thread is
    in flight, `std::thread::join` returns either `EDEADLK` (printed
    `_duckdb.Error: thread::join failed: Resource deadlock avoided`) or
    `EINVAL` (`thread::join failed: Invalid argument`) depending on
    which join-state the racing thread reached. Both errno values are
    surfaces of the same first-bootstrap mutex re-entry — they do not
    indicate a different bug. Reproduces on inline-DuckDB catalogs by
    making `cdc_consumer_force_release(<fresh catalog>, <missing
    consumer>)` the very first cdc_* call against the lake; ~1-in-5
    attempts on darwin 25.3 / Python 3.14.
  The most plausible shared mutex is inside the auto-loaded DuckLake
  extension's catalog (it owns the attached metadata database and
  sees both the outer read and the inner bootstrap write against it);
  secondary candidates are `DatabaseManager::databases_lock` and the
  dependency-manager write lock on the same `AttachedDatabase`. A
  debug-build MSVC stack trace at the `std::system_error` throw site
  would pin which one exactly; we haven't invested in that yet because
  no real user has hit this outside the regression-test pattern
  described below.
- Status: partially handled.
- Handling (source side, kept): `CheckCatalogOrThrow` and
  `BootstrapConsumerStateOrThrow` have `Connection&` overloads, and
  every cdc_* function opens one internal connection up front
  (`duckdb::Connection conn(*context.db); ConfigureCdcInternalConnection
  (conn);`) and threads it through the compat probe, the bootstrap
  CREATEs, and all follow-up reads/writes. That closes the separate
  3-internal-connection MinGW variant (the original shape of this
  hazard). It does not close the outer-conn ↔ inner-conn handoff, which
  requires upstream (DuckDB + DuckLake) lock-ordering work to fix
  in-source.
- Handling (release matrix — temporary): release automation passes
  `skip_tests: true` to DuckDB's full extension distribution matrix.
  That matrix remains the gate that every shipped binary builds, while
  day-to-day PR CI on Linux remains the SQL test gate. This avoids
  blocking releases on Windows release-triplet surfaces of this hazard,
  including MinGW's `Resource deadlock avoided` during
  `cdc_dml_consumer_create`. Local `make test_release` and Linux PR CI
  still run the regression unchanged.
- Handling (targeted regression gate — temporary): Windows MSVC
  (`DUCKDB_PLATFORM == windows_amd64`) still does not export
  `CDC_RUN_H022_SENSITIVE_TESTS`. `dml_schema_shape_pinning.test` gates
  on `require-env CDC_RUN_H022_SENSITIVE_TESTS`, so it is skipped on
  that triplet until stabilisation captures an MSVC stack trace at the
  throw site and fixes the mutex ordering. The extension root `Makefile`
  exports `CDC_RUN_H022_SENSITIVE_TESTS := 1` on every other platform;
  developers who run `unittest` manually without Make must export
  `CDC_RUN_H022_SENSITIVE_TESTS=1` on non MSVC-Windows hosts. The test
  body keeps `SET VARIABLE x = (SELECT max(snapshot_id) FROM
  __ducklake_metadata_<catalog>.ducklake_snapshot)` + `start_at :=
  getvariable('x')` — the intentional API-shape coverage; MSVC does
  not run it via this gate (`start_at := 'now'` proved insufficient on
  MSVC CI — the deadlock still surfaced there).
- Notes: DuckLake's documented `META_JOURNAL_MODE 'WAL'` and
  `META_BUSY_TIMEOUT` ATTACH options improve concurrency on
  SQLite-backed catalogs in general (see duckdb/ducklake#128) and are
  orthogonal to this hazard.
- User-facing guidance: On Windows MSVC, avoid first write-path `cdc_*`
  against a catalog in-process until stabilisation confirms a safe recipe.
  Tentative mitigations (empirical, not validated on MSVC CI): drive a read-
  only `cdc_*` call first (`cdc_dml_ticks_query`, etc.), use a separate
  DuckDB process for the preceding metadata peek, or open the catalog on an
  already-bootstraped path (`start_at := 'now'` alone did **not** clear the
  failure in MSVC release-matrix logs).
- Python-client mitigation: `ducklake_cdc_client.retry.retry_on_transient`
  is the high-level consumers' default retry policy. It treats both
  `database is locked` (SQLite catalog under contention) and any
  `thread::join failed: <errno>` (the macOS surfaces of this hazard) as
  retryable, with a short fixed backoff and a small attempt cap. The second
  attempt always finds the catalog already bootstrapped, so the racy first-
  time mutex re-entry doesn't recur. Callers who explicitly want the raw
  exception can opt out with `retry=ducklake_cdc_client.no_retry`.
- New finding (May 2026, postgres + populated catalog): the ``retry_on_transient``
  recovery is **not** sufficient when the H-022 race fires against an
  already-bootstrapped postgres catalog (e.g. a process that ATTACHes a
  catalog containing `__ducklake_cdc.*` from a prior run, runs a few
  `CREATE TABLE IF NOT EXISTS` calls on the lake, then issues
  `cdc_dml_consumer_create`). On that path the same-connection retry
  also fails with the same ``thread::join failed`` errno, presumably
  because the parent ``ClientContext``'s catalog-machinery state is
  poisoned by the first failed attempt and a fresh internal connection
  on the same outer context re-enters the same mutex. Empirically reliable
  workaround: call :func:`ducklake_cdc_client.prewarm` (or construct
  :class:`ducklake_cdc_client.CDCClient` with default ``prewarm_connection=True``,
  which runs the same cheap ``cdc_version()`` scalar) on **each** DuckDB
  handle that will run ``cdc_*`` table functions, **immediately after**
  ``LOAD ducklake_cdc`` and before any ``CREATE TABLE`` /
  ``cdc_*_consumer_create`` / ``cdc_consumer_stats`` on that handle. This
  warms connection-side state; derived cursors that reach CDC first should
  run :func:`~ducklake_cdc_client.prewarm` on the cursor as well (see
  ``e2e/_lib/load.py``). ``e2e/_lib/config.py::load_cdc_extension`` uses
  prewarm on ``lake.connection``. A secondary
  occurrence pattern -- the parent connection runs N catalog-touching
  reads (`SELECT count(*) FROM lake.<table>`) and *then* issues a
  `cdc_consumer_stats('lake')` table function -- still trips the
  race even after the initial pre-warm; the workaround there is to
  use a derived cursor (`lake.connection.cursor()`) for the cdc_*
  call so the catalog-touched cursor and the cdc-call cursor are
  distinct (`e2e/01_pipeline_dag/inspect_lake.py` and the
  `DMLConsumer` lease connection both follow this pattern). Both
  workarounds are example-side; closing the hazard at source still
  needs the upstream lock-ordering fix the original notes describe.
- Atlas production-shaped reproduction (July 2026): Atlas surfaced bare
  `_duckdb.Error: Resource deadlock avoided` from
  `cdc_dml_consumer_create` while opening its crawl planner against a populated
  Postgres DuckLake catalog. The standalone
  `e2e/smoke/h022_atlas_setup_smoke.py` reproducer creates catalog history and
  performs concurrent planner setup. Before the local mitigation, Atlas's
  shared catalogue-connection shape failed 1/100 attempts while the client's
  dedicated derived-connection shape passed 100/100.
- Handling (populated catalogs): bootstrap now probes the state table and
  returns immediately when it exists, rather than rerunning three `CREATE TABLE
  IF NOT EXISTS` statements plus Postgres trigger DDL on every `cdc_*` setup.
  Genuine first bootstrap is serialized and rechecked within one process.
  This removes the avoidable repeated-DDL lock handoff; callers must still use
  a dedicated derived connection for CDC. After this mitigation both shapes
  passed 100/100 populated-catalog attempts in the standalone stress test.
- Handling (catchable surfaces): consumer creation and the schema-diff MAP
  scan retry the three known H-022 spellings up to five times with a short
  bounded backoff. This keeps transient lock/thread teardown errors from
  escaping normal SQL callers; it cannot recover an uncaught runtime abort.
- Remaining upstream boundary: first bootstrap from multiple processes, and
  the fundamental outer `ClientContext` to inner `Connection` catalog-lock
  ordering, cannot be made atomic solely by this extension. Escalate the
  standalone reproducer and a debug stack at the `std::system_error` throw site
  to DuckDB/DuckLake. The reproducer's `--first-bootstrap-race` mode isolates
  this boundary: the shared-connection shape surfaced `thread::join failed:
  Invalid argument` in 1/16 attempts while the derived-connection control
  passed 16/16.

### H-023: CDC changed the database-wide thread pool during query execution

- Evidence: Linux main-branch CI run 34041993086 failed consumer creation with
  `Invalid Error: Invalid argument`; the same source passed PR CI.
  `ConfigureCdcInternalConnection` executed `SET threads = 1` on every internal
  connection. DuckDB 1.5.5 implements this as `ThreadsSetting::SetGlobal`, which
  calls `TaskScheduler::SetThreads`; worker-pool resizing can join threads while
  the outer CDC query is running. The setting is not connection-local.
- Fix: remove that configuration helper and its callers. CDC inherits the
  database's configured thread budget and never resizes the worker pool.
  Applications needing a serial CDC instance must configure it before queries
  start. Do not restore the setting around a call: that also resizes the pool.
- Regression: `consumer_lifecycle.test` explicitly configures four threads before
  consumer creation and asserts the setting remains four afterward.
- This corrects an extension-owned scheduler mutation. It does not establish
  that every previously reported H-022 symptom has the same cause.
