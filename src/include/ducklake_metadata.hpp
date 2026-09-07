//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// ducklake_metadata.hpp
//
// Shared utility layer: facts about the DuckLake metadata catalog and the
// CDC state-schema layout that lives next to it. This header is a "facts vs
// policy" boundary - everything here answers a structural question about
// the lake or about the on-disk state tables (what's the current snapshot,
// what schema_version does snapshot N have, how do I quote an identifier,
// where do the per-consumer state rows live). It MUST NOT make CDC policy
// decisions (what counts as a window boundary, what a consumer should see,
// when to emit a notice). Helpers in `consumer.hpp` / `ddl.hpp` /
// `dml.hpp` / `stats.hpp` own those.
//
// Keep this rule in mind when adding to this file: if a new helper makes
// a *decision* about CDC behaviour, it belongs in one of the domain
// modules, not here. If it just answers a *fact* about the lake or the
// state tables, it belongs here.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <string>
#include <unordered_set>
#include <vector>

namespace duckdb_cdc {

//===--------------------------------------------------------------------===//
// State-schema constants and tunables
//
// Declared `extern const` instead of `inline constexpr` so the DuckDB
// extension build (which doesn't always propagate `-std=c++17` via the
// extension-ci-tools toolchain) doesn't trigger `-Wc++17-extensions`
// warnings on every translation unit that includes this header.
// Definitions live in `ducklake_metadata.cpp`.
//===--------------------------------------------------------------------===//

extern const char *const CONSUMERS_TABLE;
extern const char *const CONSUMER_SUBSCRIPTIONS_TABLE;
extern const char *const AUDIT_TABLE;
extern const char *const STATE_SCHEMA;

extern const int64_t DEFAULT_MAX_SNAPSHOTS;
extern const int64_t HARD_MAX_SNAPSHOTS;
extern const int64_t DEFAULT_WAIT_TIMEOUT_MS;
extern const int64_t HARD_WAIT_TIMEOUT_MS;
extern const int64_t WAIT_INITIAL_INTERVAL_MS;
extern const int64_t WAIT_MAX_INTERVAL_MS;

//===--------------------------------------------------------------------===//
// SQL identifier / literal helpers
//===--------------------------------------------------------------------===//

std::string QuoteIdentifier(const std::string &identifier);
std::string QuoteLiteral(const std::string &value);

//===--------------------------------------------------------------------===//
// JSON / string helpers
//===--------------------------------------------------------------------===//

std::string JsonEscape(const std::string &value);
std::string JsonValue(const duckdb::Value &value);
std::string JsonOptionalString(const duckdb::Value &v);
std::string TrimCopy(const std::string &value);
bool StartsWith(const std::string &value, const std::string &prefix);
bool TryParseInt64(const std::string &input, int64_t &out);

//===--------------------------------------------------------------------===//
// Bind-input / query-result helpers
//===--------------------------------------------------------------------===//

std::string GetStringArg(const duckdb::Value &value, const std::string &name);
int64_t SingleInt64(duckdb::MaterializedQueryResult &result, const std::string &description);
void ThrowIfQueryFailed(const duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result);
void ExecuteChecked(duckdb::Connection &conn, const std::string &sql);
std::string GenerateUuid(duckdb::Connection &conn);

//===--------------------------------------------------------------------===//
// Catalog table-name builders + state-schema introspection
//===--------------------------------------------------------------------===//

std::string MetadataDatabase(const std::string &catalog_name);
std::string MetadataTable(const std::string &catalog_name, const std::string &table_name);
std::string MetadataTable(duckdb::Connection &conn, const std::string &catalog_name, const std::string &table_name);
std::string MetadataAttachmentCacheKey(duckdb::Connection &conn, const std::string &catalog_name);

//! Resolved durable identity for one DuckLake attachment. The defaults retain
//! the original single-lake PostgreSQL layout; cdc_configure can override both
//! schemas before the first stateful CDC call for an attachment.
struct CdcCatalogState {
	std::string catalog_name;
	std::string attachment_identity;
	std::string metadata_schema;
	std::string state_schema;
};

CdcCatalogState ResolveCdcCatalogState(duckdb::Connection &conn, const std::string &catalog_name);
CdcCatalogState ConfigureCdcCatalogState(duckdb::Connection &conn, const std::string &catalog_name,
                                         const std::string &state_schema, const std::string &metadata_schema);
std::string StateTable(const std::string &catalog_name, const std::string &table_name, bool use_state_schema);
std::string StateTable(const std::string &catalog_name, const std::string &table_name, const std::string &state_schema);
bool StateSchemaExists(duckdb::Connection &conn, const std::string &catalog_name);
std::string StateTable(duckdb::Connection &conn, const std::string &catalog_name, const std::string &table_name);
std::string SnapshotNotifyChannel(duckdb::Connection &conn, const std::string &catalog_name);
bool MetadataBackendIsPostgres(duckdb::Connection &conn, const std::string &catalog_name);
std::string PostgresMetadataDsn(duckdb::Connection &conn, const std::string &catalog_name);
duckdb::unique_ptr<duckdb::MaterializedQueryResult>
QueryPostgresMetadata(duckdb::Connection &conn, const std::string &catalog_name, const std::string &sql);

//===--------------------------------------------------------------------===//
// Snapshot fact lookups
//===--------------------------------------------------------------------===//

int64_t ResolveSnapshot(duckdb::Connection &conn, const std::string &catalog_name, const std::string &literal,
                        const std::string &argument_name, const std::string &feature_name, bool null_means_oldest);
int64_t ResolveCreateSnapshot(duckdb::Connection &conn, const std::string &catalog_name, const std::string &start_at);
int64_t ResolveResetSnapshot(duckdb::Connection &conn, const std::string &catalog_name, const std::string &to_snapshot);
int64_t ResolveSchemaVersion(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id);
int64_t CurrentSnapshot(duckdb::Connection &conn, const std::string &catalog_name);
duckdb::Value SnapshotTime(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id);
int64_t FirstSnapshotAfter(duckdb::Connection &conn, const std::string &catalog_name, int64_t last_snapshot,
                           int64_t current_snapshot);
void EnsureSnapshotExistsOrGap(duckdb::Connection &conn, const std::string &catalog_name,
                               const std::string &consumer_name, int64_t snapshot_id);

//===--------------------------------------------------------------------===//
// Structural schema-change predicates
//
// These are *facts* about a snapshot's `changes_made` text or about a
// snapshot row in `ducklake_snapshot_changes`. They answer the question
// "did this snapshot do DDL?" by scanning the DuckLake-emitted token
// vocabulary, which is part of DuckLake's catalog format. Higher-level
// behaviour (whether a window should stop here, whether to emit a notice)
// lives in the domain modules and consults these.
//===--------------------------------------------------------------------===//

bool SnapshotHasSchemaChange(const std::string &changes_made);
bool SnapshotIsExternalSchemaChange(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id);
int64_t NextExternalSchemaChangeSnapshot(duckdb::Connection &conn, const std::string &catalog_name,
                                         int64_t start_snapshot, int64_t current_snapshot, int64_t base_schema_version);

//===--------------------------------------------------------------------===//
// `since_seconds` lookback helpers (shared by stateless sugar)
//===--------------------------------------------------------------------===//

int64_t SinceSecondsParameter(duckdb::TableFunctionBindInput &input, int64_t default_value);
int64_t ResolveSinceStartSnapshot(duckdb::Connection &conn, const std::string &catalog_name, int64_t since_seconds);

//===--------------------------------------------------------------------===//
// State-table DDL strings + lazy bootstrap
//===--------------------------------------------------------------------===//

std::string ConsumersDdl(const std::string &catalog_name, bool use_state_schema, const std::string &state_schema);
std::string ConsumerSubscriptionsDdl(const std::string &catalog_name, bool use_state_schema,
                                     const std::string &state_schema);
std::string AuditDdl(const std::string &catalog_name, bool use_state_schema, const std::string &state_schema);

//! Idempotently create the catalog-resident CDC state tables in a DuckLake
//! catalog. Throws `CDC_INCOMPATIBLE_CATALOG` before any write when the
//! target catalog format is outside the supported set.
//!
//! The `Connection&` overload runs the CREATE writes on the caller-provided
//! connection. Internal cdc_* callers should prefer it: chaining the
//! version probe + bootstrap + the cdc_* function's main work onto a
//! single DuckDB connection means everything goes through one SQLite
//! file handle when the metadata catalog is SQLite-backed, sidestepping
//! the Windows MinGW `LockFileEx` `ERROR_POSSIBLE_DEADLOCK` handoff
//! described in H-022 (`docs/hazard-log.md`).
void BootstrapConsumerStateOrThrow(duckdb::Connection &conn, const std::string &catalog_name);
void BootstrapConsumerStateOrThrow(duckdb::ClientContext &context, const std::string &catalog_name);

//===--------------------------------------------------------------------===//
// Row-scan glue used by every TableFunction in this extension. Each
// Init function builds a vector<vector<Value>> of materialised rows and
// parks it on a `RowScanState`; `RowScanExecute` then drains that buffer
// `STANDARD_VECTOR_SIZE` rows at a time.
//===--------------------------------------------------------------------===//

struct RowScanState : public duckdb::GlobalTableFunctionState {
	std::vector<std::vector<duckdb::Value>> rows;
	duckdb::idx_t offset = 0;
};

void RowScanExecute(duckdb::ClientContext &context, duckdb::TableFunctionInput &input, duckdb::DataChunk &output);

//! Result-backed scan glue for table functions that already run an inner
//! DuckDB query. This avoids copying the entire MaterializedQueryResult into
//! RowScanState before DuckDB asks the table function for output chunks.
struct MaterializedResultScanState : public duckdb::GlobalTableFunctionState {
	duckdb::unique_ptr<duckdb::MaterializedQueryResult> result;
	duckdb::idx_t offset = 0;
};

void MaterializedResultScanExecute(duckdb::ClientContext &context, duckdb::TableFunctionInput &input,
                                   duckdb::DataChunk &output);

} // namespace duckdb_cdc
