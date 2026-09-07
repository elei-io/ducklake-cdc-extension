//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// consumer.cpp
//
// Implementation of the consumer state machine: lifecycle (create / reset /
// drop / list / force-release / heartbeat), the in-process token cache,
// the audit log writer, and the cursor primitives (cdc_window /
// cdc_commit / listen calls). The schema-boundary policy lives here too: a
// `cdc_window` call consults `SnapshotIsExternalSchemaChange` /
// `NextExternalSchemaChangeSnapshot` (facts in `ducklake_metadata.cpp`)
// and *decides* whether to truncate the visible window, then emits the
// `CDC_SCHEMA_BOUNDARY` notice.
//
// Lease and commit state have two execution paths. DuckDB-backed state uses
// single-statement `UPDATE ... RETURNING` for the hot path; attached catalog
// backends that do not support RETURNING through DuckDB's scanner fall back to
// the multi-statement legacy path. The legacy path is the reference behavior,
// and the fast path must match its semantics. Keep `AcquireLeaseReturning` /
// `AcquireLeaseLegacy` and the matching `cdc_commit` branches in sync when
// changing lease ownership, expiration, or audit semantics.
//===----------------------------------------------------------------------===//

#include "consumer.hpp"

#include "compat_check.hpp"
#include "ducklake_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#ifndef _WIN32
#include <dlfcn.h>
#include <sys/select.h>
#include <unistd.h>
#endif

namespace duckdb_cdc {

//===--------------------------------------------------------------------===//
// CdcWindowData (declared in consumer.hpp)
//===--------------------------------------------------------------------===//

duckdb::unique_ptr<duckdb::FunctionData> CdcWindowData::Copy() const {
	auto result = duckdb::make_uniq<CdcWindowData>();
	result->catalog_name = catalog_name;
	result->consumer_name = consumer_name;
	result->max_snapshots = max_snapshots;
	return std::move(result);
}

bool CdcWindowData::Equals(const duckdb::FunctionData &other) const {
	return this == &other;
}

namespace {

bool IsH022TransientMessage(const std::string &message) {
	const auto lower = duckdb::StringUtil::Lower(message);
	return lower.find("resource deadlock avoided") != std::string::npos ||
	       lower.find("resource deadlock would occur") != std::string::npos ||
	       lower.find("thread::join failed") != std::string::npos;
}

//===--------------------------------------------------------------------===//
// Forward declarations for the schema-shape pin enforcement helpers.
// Definitions live further down in this translation unit; lifecycle paths
// (cdc_consumer_reset, cdc_commit) call them, so a forward declaration is
// needed to keep the file in topological order without a major reshuffle.
//===--------------------------------------------------------------------===//

void ValidateDmlConsumerShapeBoundary(duckdb::Connection &conn, const std::string &catalog_name, const ConsumerRow &row,
                                      int64_t requested_snapshot);
void ValidateDmlConsumerResetTarget(duckdb::Connection &conn, const std::string &catalog_name, const ConsumerRow &row,
                                    int64_t target_snapshot);
int64_t FirstDmlSubscribedSnapshot(duckdb::Connection &conn, const std::string &catalog_name, int64_t start_snapshot,
                                   int64_t current_snapshot,
                                   const std::vector<ConsumerSubscriptionRow> &dml_subscriptions);

//===--------------------------------------------------------------------===//
// Process-wide caches: owner-token (cdc_window/commit/heartbeat lease
// continuity per (connection, catalog, consumer)) and the per-connection
// shared-connection warning latch (listen calls).
//===--------------------------------------------------------------------===//

std::mutex TOKEN_CACHE_MUTEX;
std::unordered_map<std::string, std::string> TOKEN_CACHE;

struct DmlSafeCommitRange {
	int64_t cursor = -1;
	int64_t safe_until = -1;
};

std::mutex DML_SAFE_COMMIT_MUTEX;
std::unordered_map<std::string, DmlSafeCommitRange> DML_SAFE_COMMIT_RANGES;

std::mutex WAIT_WARNING_MUTEX;
std::unordered_set<int64_t> WAIT_WARNED_CONNECTIONS;

struct TableResolutionCacheEntry {
	bool found = false;
	int64_t schema_id = 0;
	std::string qualified_name;
};

std::mutex DML_PREFLIGHT_CACHE_MUTEX;
std::unordered_map<std::string, TableResolutionCacheEntry> TABLE_RESOLUTION_CACHE;
std::unordered_map<std::string, bool> DML_TABLE_CHANGE_MATCH_CACHE;
constexpr size_t DML_PREFLIGHT_CACHE_MAX_ENTRIES = 8192;

struct DecodedSnapshotChange {
	int64_t snapshot_id = -1;
	std::unordered_set<int64_t> dml_table_ids;
	std::unordered_set<int64_t> altered_table_ids;
	std::unordered_set<int64_t> dropped_table_ids;
	std::unordered_set<int64_t> dropped_schema_ids;
};

struct SnapshotChangeIndex {
	std::unordered_map<int64_t, DecodedSnapshotChange> changes;
	int64_t loaded_from = -1;
	int64_t loaded_to = -1;
	uint64_t last_used = 0;
};

std::mutex SNAPSHOT_CHANGE_INDEX_MUTEX;
std::unordered_map<std::string, SnapshotChangeIndex> SNAPSHOT_CHANGE_INDEXES;
uint64_t SNAPSHOT_CHANGE_INDEX_CLOCK = 0;
constexpr size_t SNAPSHOT_CHANGE_INDEX_MAX_CATALOGS = 8;
constexpr size_t SNAPSHOT_CHANGE_INDEX_MAX_SNAPSHOTS_PER_CATALOG = 8192;

struct AdaptiveListenState {
	bool has_last_success = false;
	std::chrono::steady_clock::time_point last_success_at;
	int64_t burst_streak = 0;
};

std::mutex ADAPTIVE_LISTEN_MUTEX;
std::unordered_map<std::string, AdaptiveListenState> ADAPTIVE_LISTEN_STATES;

const int64_t ADAPTIVE_LISTEN_BURST_THRESHOLD_MS = 250;
const int64_t ADAPTIVE_LISTEN_TARGET_ROWS = 64;
const int64_t ADAPTIVE_LISTEN_TARGET_SNAPSHOTS = 8;
const int64_t ADAPTIVE_LISTEN_MIN_COALESCE_MS = 5;
const int64_t ADAPTIVE_LISTEN_MAX_COALESCE_MS = 50;
const int64_t ADAPTIVE_LISTEN_POLL_MS = 5;

#ifndef _WIN32
struct LibpqSymbols {
	void *handle = nullptr;
	void *(*connectdb)(const char *) = nullptr;
	int (*status)(void *) = nullptr;
	void (*finish)(void *) = nullptr;
	void *(*exec)(void *, const char *) = nullptr;
	void (*clear)(void *) = nullptr;
	int (*result_status)(void *) = nullptr;
	int (*socket)(void *) = nullptr;
	int (*consume_input)(void *) = nullptr;
	void *(*notifies)(void *) = nullptr;
	void (*freemem)(void *) = nullptr;
	const char *(*error_message)(void *) = nullptr;

	bool Available() const {
		return handle && connectdb && status && finish && exec && clear && result_status && socket && consume_input &&
		       notifies && freemem && error_message;
	}
};

LibpqSymbols LoadLibpqSymbols() {
	LibpqSymbols out;
	const char *candidates[] = {"libpq.dylib",
	                            "libpq.5.dylib",
	                            "/opt/homebrew/opt/libpq/lib/libpq.5.dylib",
	                            "/opt/homebrew/lib/libpq.5.dylib",
	                            "/usr/local/opt/libpq/lib/libpq.5.dylib",
	                            "libpq.so.5",
	                            "libpq.so"};
	for (const auto *candidate : candidates) {
		out.handle = dlopen(candidate, RTLD_LAZY | RTLD_LOCAL);
		if (out.handle) {
			break;
		}
	}
	if (!out.handle) {
		return out;
	}
	out.connectdb = reinterpret_cast<void *(*)(const char *)>(dlsym(out.handle, "PQconnectdb"));
	out.status = reinterpret_cast<int (*)(void *)>(dlsym(out.handle, "PQstatus"));
	out.finish = reinterpret_cast<void (*)(void *)>(dlsym(out.handle, "PQfinish"));
	out.exec = reinterpret_cast<void *(*)(void *, const char *)>(dlsym(out.handle, "PQexec"));
	out.clear = reinterpret_cast<void (*)(void *)>(dlsym(out.handle, "PQclear"));
	out.result_status = reinterpret_cast<int (*)(void *)>(dlsym(out.handle, "PQresultStatus"));
	out.socket = reinterpret_cast<int (*)(void *)>(dlsym(out.handle, "PQsocket"));
	out.consume_input = reinterpret_cast<int (*)(void *)>(dlsym(out.handle, "PQconsumeInput"));
	out.notifies = reinterpret_cast<void *(*)(void *)>(dlsym(out.handle, "PQnotifies"));
	out.freemem = reinterpret_cast<void (*)(void *)>(dlsym(out.handle, "PQfreemem"));
	out.error_message = reinterpret_cast<const char *(*)(void *)>(dlsym(out.handle, "PQerrorMessage"));
	if (!out.Available()) {
		dlclose(out.handle);
		out = LibpqSymbols();
	}
	return out;
}

LibpqSymbols &Libpq() {
	static LibpqSymbols symbols = LoadLibpqSymbols();
	return symbols;
}

class PostgresSnapshotListener {
public:
	PostgresSnapshotListener(const std::string &dsn, const std::string &channel) {
		auto &pq = Libpq();
		if (!pq.Available() || dsn.empty()) {
			return;
		}
		conn = pq.connectdb(dsn.c_str());
		if (!conn || pq.status(conn) != 0) { // CONNECTION_OK == 0
			Close();
			return;
		}
		const auto sql = "LISTEN " + QuoteIdentifier(channel);
		auto result = pq.exec(conn, sql.c_str());
		if (!result || pq.result_status(result) != 1) { // PGRES_COMMAND_OK == 1
			if (result) {
				pq.clear(result);
			}
			Close();
			return;
		}
		pq.clear(result);
		DrainNotifications();
		ok = true;
	}

	~PostgresSnapshotListener() {
		Close();
	}

	bool Available() const {
		return ok;
	}

	bool Wait(int64_t timeout_ms) {
		if (!ok || timeout_ms <= 0) {
			return false;
		}
		auto &pq = Libpq();
		const auto fd = pq.socket(conn);
		if (fd < 0) {
			return false;
		}
		fd_set input;
		FD_ZERO(&input);
		FD_SET(fd, &input);
		timeval timeout;
		timeout.tv_sec = timeout_ms / 1000;
		timeout.tv_usec = static_cast<suseconds_t>((timeout_ms % 1000) * 1000);
		const auto ready = select(fd + 1, &input, nullptr, nullptr, &timeout);
		if (ready <= 0) {
			return false;
		}
		if (pq.consume_input(conn) == 0) {
			return false;
		}
		return DrainNotifications();
	}

private:
	void *conn = nullptr;
	bool ok = false;

	bool DrainNotifications() {
		auto &pq = Libpq();
		bool got = false;
		while (auto notification = pq.notifies(conn)) {
			got = true;
			pq.freemem(notification);
		}
		return got;
	}

	void Close() {
		if (conn) {
			Libpq().finish(conn);
			conn = nullptr;
		}
		ok = false;
	}
};
#endif

//===--------------------------------------------------------------------===//
// Lease / cache helpers
//===--------------------------------------------------------------------===//

std::string ConnectionCachePrefix(duckdb::ClientContext &context) {
	std::ostringstream out;
	out << context.db.get() << ":" << context.GetConnectionId();
	return out.str();
}

std::string TokenCacheKey(duckdb::ClientContext &context, duckdb::Connection &conn, const std::string &catalog_name,
                          const std::string &consumer_name) {
	const auto state = ResolveCdcCatalogState(conn, catalog_name);
	const auto identity = state.attachment_identity.empty() ? catalog_name : state.attachment_identity;
	return ConnectionCachePrefix(context) + ":" + identity + ":" + state.state_schema + ":" + consumer_name;
}

std::string DmlSafeCommitKey(duckdb::ClientContext &context, duckdb::Connection &conn, const std::string &catalog_name,
                             const std::string &consumer_name) {
	return TokenCacheKey(context, conn, catalog_name, consumer_name);
}

std::string AdaptiveListenKey(const std::string &catalog_name, const std::string &consumer_name,
                              const std::string &stream_key) {
	return catalog_name + ":" + consumer_name + ":" + stream_key;
}

std::string CachedToken(duckdb::ClientContext &context, duckdb::Connection &conn, const std::string &catalog_name,
                        const std::string &consumer_name) {
	std::lock_guard<std::mutex> guard(TOKEN_CACHE_MUTEX);
	auto entry = TOKEN_CACHE.find(TokenCacheKey(context, conn, catalog_name, consumer_name));
	if (entry == TOKEN_CACHE.end()) {
		return "";
	}
	return entry->second;
}

void CacheToken(duckdb::ClientContext &context, duckdb::Connection &conn, const std::string &catalog_name,
                const std::string &consumer_name, const std::string &token) {
	std::lock_guard<std::mutex> guard(TOKEN_CACHE_MUTEX);
	TOKEN_CACHE[TokenCacheKey(context, conn, catalog_name, consumer_name)] = token;
}

void EraseCachedToken(duckdb::ClientContext &context, duckdb::Connection &conn, const std::string &catalog_name,
                      const std::string &consumer_name) {
	std::lock_guard<std::mutex> guard(TOKEN_CACHE_MUTEX);
	TOKEN_CACHE.erase(TokenCacheKey(context, conn, catalog_name, consumer_name));
}

void CacheDmlSafeCommitRange(duckdb::ClientContext &context, duckdb::Connection &conn, const std::string &catalog_name,
                             const std::string &consumer_name, int64_t cursor, int64_t safe_until) {
	std::lock_guard<std::mutex> guard(DML_SAFE_COMMIT_MUTEX);
	DmlSafeCommitRange range;
	range.cursor = cursor;
	range.safe_until = safe_until;
	DML_SAFE_COMMIT_RANGES[DmlSafeCommitKey(context, conn, catalog_name, consumer_name)] = range;
}

bool HasCachedDmlSafeCommitRange(duckdb::ClientContext &context, duckdb::Connection &conn,
                                 const std::string &catalog_name, const std::string &consumer_name, int64_t cursor,
                                 int64_t requested_snapshot) {
	std::lock_guard<std::mutex> guard(DML_SAFE_COMMIT_MUTEX);
	const auto entry = DML_SAFE_COMMIT_RANGES.find(DmlSafeCommitKey(context, conn, catalog_name, consumer_name));
	if (entry == DML_SAFE_COMMIT_RANGES.end()) {
		return false;
	}
	const auto &range = entry->second;
	return range.cursor == cursor && requested_snapshot <= range.safe_until;
}

std::string TokenSqlOrNull(const std::string &token) {
	if (token.empty()) {
		return "NULL";
	}
	return QuoteLiteral(token) + "::UUID";
}

ConsumerRow ConsumerRowFromResult(duckdb::MaterializedQueryResult &result, duckdb::idx_t row_index) {
	ConsumerRow row;
	row.consumer_name = result.GetValue(0, row_index).ToString();
	row.consumer_kind = result.GetValue(1, row_index).ToString();
	row.consumer_id = result.GetValue(2, row_index).GetValue<int64_t>();
	row.table_id = result.GetValue(3, row_index);
	auto snapshot_value = result.GetValue(4, row_index);
	row.last_committed_snapshot = snapshot_value.IsNull() ? -1 : snapshot_value.GetValue<int64_t>();
	auto schema_value = result.GetValue(5, row_index);
	row.last_committed_schema_version = schema_value.IsNull() ? -1 : schema_value.GetValue<int64_t>();
	row.owner_token = result.GetValue(6, row_index);
	row.owner_acquired_at = result.GetValue(7, row_index);
	row.owner_heartbeat_at = result.GetValue(8, row_index);
	row.lease_interval_seconds = result.GetValue(9, row_index).GetValue<int64_t>();
	return row;
}

std::string ConsumerRowProjection() {
	return "consumer_name, consumer_kind, consumer_id, table_id, last_committed_snapshot, "
	       "last_committed_schema_version, owner_token, owner_acquired_at, owner_heartbeat_at, "
	       "lease_interval_seconds";
}

enum class StateBackendKind { DuckDB, SQLite, Postgres, Unknown };

std::mutex BACKEND_CACHE_MUTEX;
std::unordered_map<std::string, StateBackendKind> BACKEND_CACHE;

std::string BackendCacheKey(duckdb::ClientContext &context, duckdb::Connection &conn, const std::string &catalog_name) {
	const auto attachment_key = MetadataAttachmentCacheKey(conn, catalog_name);
	return ConnectionCachePrefix(context) + ":" + (attachment_key.empty() ? catalog_name : attachment_key);
}

StateBackendKind DetectStateBackend(duckdb::Connection &conn, const std::string &catalog_name) {
	const auto metadata_database = std::string("__ducklake_metadata_") + catalog_name;
	auto result = conn.Query(
	    "SELECT type FROM duckdb_databases() WHERE database_name = " + QuoteLiteral(metadata_database) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return StateBackendKind::Unknown;
	}
	const auto type = duckdb::StringUtil::Lower(result->GetValue(0, 0).ToString());
	if (type == "duckdb") {
		return StateBackendKind::DuckDB;
	}
	if (type == "sqlite") {
		return StateBackendKind::SQLite;
	}
	if (type == "postgres" || type == "postgres_scanner") {
		return StateBackendKind::Postgres;
	}
	return StateBackendKind::Unknown;
}

StateBackendKind CachedStateBackend(duckdb::ClientContext &context, duckdb::Connection &conn,
                                    const std::string &catalog_name) {
	const auto key = BackendCacheKey(context, conn, catalog_name);
	{
		std::lock_guard<std::mutex> guard(BACKEND_CACHE_MUTEX);
		auto entry = BACKEND_CACHE.find(key);
		if (entry != BACKEND_CACHE.end()) {
			return entry->second;
		}
	}

	const auto backend = DetectStateBackend(conn, catalog_name);
	if (backend != StateBackendKind::Unknown) {
		std::lock_guard<std::mutex> guard(BACKEND_CACHE_MUTEX);
		BACKEND_CACHE[key] = backend;
	}
	return backend;
}

bool SupportsUpdateReturning(StateBackendKind backend) {
	// DuckDB's sqlite_scanner and postgres_scanner currently reject
	// `UPDATE ... RETURNING` for attached tables, even though both underlying
	// engines support it natively. Keep this gate narrow until scanners learn
	// to round-trip RETURNING correctly for attached tables.
	return backend == StateBackendKind::DuckDB;
}

std::string NativePostgresTable(const std::string &schema_name, const std::string &table_name) {
	return QuoteIdentifier(schema_name) + "." + QuoteIdentifier(table_name);
}

std::string NativePostgresStateTable(const std::string &state_schema, const std::string &table_name) {
	return NativePostgresTable(state_schema, table_name);
}

std::string NativePostgresMetadataTable(const std::string &metadata_schema, const std::string &table_name) {
	return NativePostgresTable(metadata_schema, table_name);
}

void PostgresExecuteChecked(duckdb::Connection &conn, const std::string &catalog_name, const std::string &sql) {
	auto result = conn.Query("CALL postgres_execute(" + QuoteLiteral("__ducklake_metadata_" + catalog_name) + ", " +
	                         QuoteLiteral(sql) + ")");
	ThrowIfQueryFailed(result);
}

std::string BuildBusyMessage(const std::string &catalog_name, const std::string &consumer_name,
                             const duckdb::Value &owner_token, const duckdb::Value &owner_acquired_at,
                             const duckdb::Value &owner_heartbeat_at, int64_t lease_interval_seconds,
                             bool lease_likely_dead) {
	std::ostringstream out;
	out << "CDC_BUSY: consumer '" << consumer_name << "' is currently held by token ";
	out << (owner_token.IsNull() ? "<unknown>" : owner_token.ToString());
	out << " (acquired " << (owner_acquired_at.IsNull() ? "<unknown>" : owner_acquired_at.ToString())
	    << ", last heartbeat " << (owner_heartbeat_at.IsNull() ? "<unknown>" : owner_heartbeat_at.ToString())
	    << "; lease interval " << lease_interval_seconds << "s). ";
	if (lease_likely_dead) {
		out << "The holder's last heartbeat is older than 2x the lease interval; it is likely dead.";
	} else {
		out << "The holder appears alive; wait for it to release, or run ";
	}
	out << " CALL cdc_consumer_force_release('" << catalog_name << "', '" << consumer_name
	    << "') if you know it has died.";
	return out.str();
}

//===--------------------------------------------------------------------===//
// Notice emitters
//===--------------------------------------------------------------------===//

//! Format and emit a `CDC_SCHEMA_BOUNDARY` notice. Per ADR 0006 + the
//! `docs/errors.md` contract, this fires at the *current* `cdc_window`
//! call when a future snapshot inside the visible range carries a
//! different `schema_version` — telling the operator (a) what to expect
//! when they next call `cdc_window` and (b) that DDL events for the
//! schema change will arrive ahead of any DML on the new schema.
void EmitSchemaBoundaryNotice(const std::string &consumer_name, int64_t end_snapshot, int64_t end_schema_version,
                              int64_t next_snapshot, int64_t next_schema_version) {
	std::ostringstream out;
	out << "CDC_SCHEMA_BOUNDARY: consumer '" << consumer_name << "' window ends at snapshot " << end_snapshot
	    << " (schema_version " << end_schema_version << "); the next snapshot (" << next_snapshot
	    << ") is at schema_version " << next_schema_version << ". The next cdc_window call will start at snapshot "
	    << next_snapshot
	    << "; DDL events for the schema change will arrive first per the DDL-before-DML ordering "
	       "contract (ADR 0008).";
	duckdb::Printer::Print(duckdb::OutputStream::STREAM_STDERR, out.str());
}

//! DDL consumers include schema-change snapshots in the returned window, so
//! their notice must describe an in-window transition rather than a future
//! DML boundary.
void EmitDdlSchemaBoundaryNotice(const std::string &consumer_name, int64_t start_snapshot, int64_t end_snapshot,
                                 int64_t schema_change_snapshot, int64_t schema_change_version) {
	std::ostringstream out;
	out << "CDC_SCHEMA_BOUNDARY: DDL consumer '" << consumer_name << "' window [" << start_snapshot << ", "
	    << end_snapshot << "] includes schema-change snapshot " << schema_change_snapshot << " (schema_version "
	    << schema_change_version
	    << "). Apply DDL events from this window before applying DML from the same snapshot "
	       "range per the DDL-before-DML ordering contract (ADR 0008).";
	duckdb::Printer::Print(duckdb::OutputStream::STREAM_STDERR, out.str());
}

//! `CDC_WAIT_TIMEOUT_CLAMPED` notice. Per ADR 0011 and `docs/errors.md`,
//! Listen functions clamp `timeout_ms` to the session-wide hard cap; this
//! notice exists so the caller can tell the difference between "I asked
//! for 30 minutes and got NULL because nothing happened" vs "I asked
//! for 30 minutes and got NULL after 5 because the cap clamped me".
//! Includes the requested and effective timeout so clients can surface a clear
//! operator message.
void EmitWaitTimeoutClampedNotice(int64_t requested_ms, int64_t cap_ms) {
	std::ostringstream out;
	out << "CDC_WAIT_TIMEOUT_CLAMPED: a listen function was called with timeout_ms => " << requested_ms
	    << " but the session cap is " << cap_ms << " (5 minutes); the call will return after at most " << cap_ms
	    << "ms. Prefer shorter waits with re-polling for long-lived consumers.";
	duckdb::Printer::Print(duckdb::OutputStream::STREAM_STDERR, out.str());
}

//! Best-effort one-time-per-connection `CDC_WAIT_SHARED_CONNECTION`
//! warning. DuckDB does not expose per-connection introspection that
//! lets us reliably detect when a connection is "shared" with other
//! query-issuing call sites (prepared statements, cursors, pooled
//! handles), so per the roadmap item this fires unconditionally on
//! first listen function per connection. The warning is informational
//! ("if you're calling a listen function on a shared connection, here is why
//! that is a foot gun") and only emits once so SQL-CLI and notebook
//! users are not spammed.
void MaybeEmitWaitSharedConnectionWarning(duckdb::ClientContext &context, int64_t timeout_ms) {
	const auto connection_id = static_cast<int64_t>(context.GetConnectionId());
	{
		std::lock_guard<std::mutex> guard(WAIT_WARNING_MUTEX);
		if (WAIT_WARNED_CONNECTIONS.count(connection_id) > 0) {
			return;
		}
		WAIT_WARNED_CONNECTIONS.insert(connection_id);
	}
	std::ostringstream out;
	out << "CDC_WAIT_SHARED_CONNECTION: listen calls hold this DuckDB connection for up to " << timeout_ms
	    << "ms. Calling it from a connection that also serves other queries (a pool handle, a "
	       "shared notebook session, an HTTP request handler) can starve them. Hold a dedicated "
	       "connection for listen calls. SQL-CLI users must do this themselves. See docs/api.md.";
	duckdb::Printer::Print(duckdb::OutputStream::STREAM_STDERR, out.str());
}

//===--------------------------------------------------------------------===//
// Audit writer
//===--------------------------------------------------------------------===//

void InsertAuditRow(duckdb::Connection &conn, const std::string &catalog_name, const std::string &action,
                    const std::string &consumer_name, int64_t consumer_id, const std::string &details) {
	const auto audit = StateTable(conn, catalog_name, AUDIT_TABLE);
	auto next_audit_id_result = conn.Query("SELECT COALESCE(MAX(audit_id), 0) + 1 FROM " + audit);
	int64_t audit_id = SingleInt64(*next_audit_id_result, "next audit_id");
	ExecuteChecked(conn, "INSERT INTO " + audit +
	                         " (audit_id, ts, actor, action, consumer_name, consumer_id, details) VALUES (" +
	                         std::to_string(audit_id) + ", now(), current_user, " + QuoteLiteral(action) + ", " +
	                         QuoteLiteral(consumer_name) + ", " + std::to_string(consumer_id) + ", " +
	                         QuoteLiteral(details) + ")");
}

//===--------------------------------------------------------------------===//
// cdc_ddl_consumer_create/cdc_dml_consumer_create
//===--------------------------------------------------------------------===//

struct ConsumerCreateData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	std::string consumer_kind;
	std::string start_at;
	// DDL-only scope inputs (lists of identifiers — DDL consumers
	// natively scope across multiple schemas/tables).
	duckdb::Value schemas;
	duckdb::Value schema_ids;
	duckdb::Value table_names;
	duckdb::Value table_ids;
	// DML scope inputs. A table identity pins row-level change consumers;
	// omitting both creates a catalog-wide, tick-only consumer.
	duckdb::Value table_name;
	duckdb::Value table_id;
	duckdb::Value change_types;
	duckdb::Value metadata;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerCreateData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->consumer_kind = consumer_kind;
		result->start_at = start_at;
		result->schemas = schemas;
		result->schema_ids = schema_ids;
		result->table_names = table_names;
		result->table_ids = table_ids;
		result->table_name = table_name;
		result->table_id = table_id;
		result->change_types = change_types;
		result->metadata = metadata;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

struct ResolvedSubscriptionInput {
	std::string scope_kind;
	duckdb::Value schema_id;
	duckdb::Value table_id;
	std::string event_category;
	std::string change_type;
	duckdb::Value original_qualified_name;
};

std::string StartAtParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("start_at");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return "now";
	}
	return entry->second.GetValue<std::string>();
}

duckdb::Value NamedParameterValue(duckdb::TableFunctionBindInput &input, const std::string &name) {
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return duckdb::Value();
	}
	return entry->second;
}

//===--------------------------------------------------------------------===//
// cdc_configure
//===--------------------------------------------------------------------===//

struct CdcConfigureData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string state_schema;
	std::string metadata_schema;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcConfigureData>();
		result->catalog_name = catalog_name;
		result->state_schema = state_schema;
		result->metadata_schema = metadata_schema;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcConfigureBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_configure requires catalog");
	}
	auto result = duckdb::make_uniq<CdcConfigureData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	const auto state_schema = NamedParameterValue(input, "state_schema");
	const auto metadata_schema = NamedParameterValue(input, "metadata_schema");
	if (state_schema.IsNull() || metadata_schema.IsNull()) {
		throw duckdb::BinderException("cdc_configure requires state_schema and metadata_schema");
	}
	result->state_schema = state_schema.GetValue<std::string>();
	result->metadata_schema = metadata_schema.GetValue<std::string>();
	names = {"catalog_name", "state_schema", "metadata_schema"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcConfigureInit(duckdb::ClientContext &context,
                                                                      duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcConfigureData>();
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	const auto configured = ConfigureCdcCatalogState(conn, data.catalog_name, data.state_schema, data.metadata_schema);
	result->rows.push_back({duckdb::Value(configured.catalog_name), duckdb::Value(configured.state_schema),
	                        duckdb::Value(configured.metadata_schema)});
	return std::move(result);
}

void ConsumerCreateReturnTypes(duckdb::vector<duckdb::LogicalType> &return_types,
                               duckdb::vector<duckdb::string> &names) {
	names = {"consumer_name",
	         "consumer_kind",
	         "consumer_id",
	         "last_committed_snapshot",
	         "subscription_id",
	         "scope_kind",
	         "schema_id",
	         "table_id",
	         "event_category",
	         "change_type",
	         "original_qualified_name",
	         "current_qualified_name"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
}

duckdb::unique_ptr<duckdb::FunctionData> DdlConsumerCreateBind(duckdb::ClientContext &context,
                                                               duckdb::TableFunctionBindInput &input,
                                                               duckdb::vector<duckdb::LogicalType> &return_types,
                                                               duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_ddl_consumer_create requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerCreateData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->consumer_kind = "ddl";
	result->start_at = StartAtParameter(input);
	result->schemas = NamedParameterValue(input, "schemas");
	result->schema_ids = NamedParameterValue(input, "schema_ids");
	result->table_names = NamedParameterValue(input, "table_names");
	result->table_ids = NamedParameterValue(input, "table_ids");
	result->metadata = NamedParameterValue(input, "metadata");
	ConsumerCreateReturnTypes(return_types, names);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> DmlConsumerCreateBind(duckdb::ClientContext &context,
                                                               duckdb::TableFunctionBindInput &input,
                                                               duckdb::vector<duckdb::LogicalType> &return_types,
                                                               duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_dml_consumer_create requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerCreateData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->consumer_kind = "dml";
	result->start_at = StartAtParameter(input);
	// A table identity pins row-level change consumers; omitting both
	// creates a catalog-wide cursor for schema-independent DML ticks.
	result->table_name = NamedParameterValue(input, "table_name");
	result->table_id = NamedParameterValue(input, "table_id");
	result->change_types = NamedParameterValue(input, "change_types");
	result->metadata = NamedParameterValue(input, "metadata");
	if (!result->table_name.IsNull() && !result->table_id.IsNull()) {
		throw duckdb::InvalidInputException(
		    "cdc_dml_consumer_create: pass exactly one of `table_name` or `table_id`, not both");
	}
	if (result->table_name.IsNull() && result->table_id.IsNull() && !result->change_types.IsNull()) {
		throw duckdb::InvalidInputException(
		    "cdc_dml_consumer_create: `change_types` requires `table_name` or `table_id`; "
		    "catalog-wide consumers emit DML ticks only");
	}
	ConsumerCreateReturnTypes(return_types, names);
	return std::move(result);
}

std::string QualifyTableInput(const std::string &table_name, const duckdb::Value &schema_name) {
	if (table_name.find('.') != std::string::npos) {
		return table_name;
	}
	const auto schema = schema_name.IsNull() ? std::string("main") : schema_name.ToString();
	return schema + "." + table_name;
}

duckdb::Value OptionalQualifiedName(const std::string &scope_kind, const duckdb::Value &schema_name,
                                    const duckdb::Value &table_name, const duckdb::Value &schema_id,
                                    const duckdb::Value &table_id, const std::string &current_name) {
	if (!table_name.IsNull()) {
		return duckdb::Value(QualifyTableInput(table_name.ToString(), schema_name));
	}
	if (!schema_name.IsNull()) {
		return duckdb::Value(schema_name.ToString());
	}
	if (!current_name.empty()) {
		return duckdb::Value(current_name);
	}
	if (scope_kind == "schema" && !schema_id.IsNull()) {
		return duckdb::Value("schema_id:" + schema_id.ToString());
	}
	if (scope_kind == "table" && !table_id.IsNull()) {
		return duckdb::Value("table_id:" + table_id.ToString());
	}
	return duckdb::Value();
}

bool ResolveSchemaByNameAt(duckdb::Connection &conn, const std::string &catalog_name, const std::string &schema_name,
                           int64_t snapshot_id, int64_t &schema_id) {
	auto result = conn.Query(
	    "SELECT schema_id FROM " + MetadataTable(conn, catalog_name, "ducklake_schema") +
	    " WHERE schema_name = " + QuoteLiteral(schema_name) + " AND begin_snapshot <= " + std::to_string(snapshot_id) +
	    " AND (end_snapshot IS NULL OR end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	schema_id = result->GetValue(0, 0).GetValue<int64_t>();
	return true;
}

bool ResolveSchemaByIdAt(duckdb::Connection &conn, const std::string &catalog_name, int64_t schema_id,
                         int64_t snapshot_id, std::string &schema_name) {
	auto result = conn.Query(
	    "SELECT schema_name FROM " + MetadataTable(conn, catalog_name, "ducklake_schema") +
	    " WHERE schema_id = " + std::to_string(schema_id) + " AND begin_snapshot <= " + std::to_string(snapshot_id) +
	    " AND (end_snapshot IS NULL OR end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	schema_name = result->GetValue(0, 0).ToString();
	return true;
}

bool ResolveTableByIdAt(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                        int64_t snapshot_id, int64_t &schema_id, std::string &qualified_name) {
	auto result =
	    conn.Query("SELECT s.schema_id, s.schema_name || '.' || t.table_name FROM " +
	               MetadataTable(conn, catalog_name, "ducklake_table") + " t JOIN " +
	               MetadataTable(conn, catalog_name, "ducklake_schema") + " s USING (schema_id) WHERE t.table_id = " +
	               std::to_string(table_id) + " AND t.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(snapshot_id) +
	               ") AND s.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (s.end_snapshot IS NULL OR s.end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull() ||
	    result->GetValue(1, 0).IsNull()) {
		return false;
	}
	schema_id = result->GetValue(0, 0).GetValue<int64_t>();
	qualified_name = result->GetValue(1, 0).ToString();
	return true;
}

bool StartAtMeansBeginning(const std::string &start_at) {
	const auto lower = duckdb::StringUtil::Lower(start_at);
	return lower == "beginning" || lower == "oldest" || lower == "oldest_available";
}

bool ResolveFirstLiveTableById(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                               int64_t &snapshot_id, int64_t &schema_id, std::string &qualified_name) {
	auto result = conn.Query("SELECT t.begin_snapshot, s.schema_id, s.schema_name || '.' || t.table_name FROM " +
	                         MetadataTable(conn, catalog_name, "ducklake_table") + " t JOIN " +
	                         MetadataTable(conn, catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE t.table_id = " + std::to_string(table_id) +
	                         " AND s.begin_snapshot <= t.begin_snapshot "
	                         "AND (s.end_snapshot IS NULL OR s.end_snapshot > t.begin_snapshot) "
	                         "ORDER BY t.begin_snapshot ASC LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull() ||
	    result->GetValue(1, 0).IsNull() || result->GetValue(2, 0).IsNull()) {
		return false;
	}
	snapshot_id = result->GetValue(0, 0).GetValue<int64_t>();
	schema_id = result->GetValue(1, 0).GetValue<int64_t>();
	qualified_name = result->GetValue(2, 0).ToString();
	return true;
}

bool ResolveFirstLiveTableByName(duckdb::Connection &conn, const std::string &catalog_name,
                                 const std::string &qualified_name, int64_t &snapshot_id, int64_t &schema_id,
                                 int64_t &table_id) {
	const auto dot = qualified_name.find('.');
	if (dot == std::string::npos) {
		return ResolveFirstLiveTableByName(conn, catalog_name, std::string("main.") + qualified_name, snapshot_id,
		                                   schema_id, table_id);
	}
	const auto schema_name = qualified_name.substr(0, dot);
	const auto table_name = qualified_name.substr(dot + 1);
	auto result = conn.Query("SELECT t.begin_snapshot, s.schema_id, t.table_id FROM " +
	                         MetadataTable(conn, catalog_name, "ducklake_table") + " t JOIN " +
	                         MetadataTable(conn, catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE s.schema_name = " + QuoteLiteral(schema_name) +
	                         " AND t.table_name = " + QuoteLiteral(table_name) +
	                         " AND s.begin_snapshot <= t.begin_snapshot "
	                         "AND (s.end_snapshot IS NULL OR s.end_snapshot > t.begin_snapshot) "
	                         "ORDER BY t.begin_snapshot ASC LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull() ||
	    result->GetValue(1, 0).IsNull() || result->GetValue(2, 0).IsNull()) {
		return false;
	}
	snapshot_id = result->GetValue(0, 0).GetValue<int64_t>();
	schema_id = result->GetValue(1, 0).GetValue<int64_t>();
	table_id = result->GetValue(2, 0).GetValue<int64_t>();
	return true;
}

std::string SubscriptionJsonArray(const std::vector<ResolvedSubscriptionInput> &subscriptions) {
	std::ostringstream out;
	out << "[";
	for (size_t i = 0; i < subscriptions.size(); ++i) {
		if (i > 0) {
			out << ",";
		}
		const auto &sub = subscriptions[i];
		out << "{\"scope_kind\":\"" << JsonEscape(sub.scope_kind)
		    << "\",\"schema_id\":" << (sub.schema_id.IsNull() ? "null" : sub.schema_id.ToString())
		    << ",\"table_id\":" << (sub.table_id.IsNull() ? "null" : sub.table_id.ToString())
		    << ",\"event_category\":\"" << JsonEscape(sub.event_category) << "\",\"change_type\":\""
		    << JsonEscape(sub.change_type) << "\",\"original_qualified_name\":";
		if (sub.original_qualified_name.IsNull()) {
			out << "null";
		} else {
			out << "\"" << JsonEscape(sub.original_qualified_name.ToString()) << "\"";
		}
		out << "}";
	}
	out << "]";
	return out.str();
}

std::vector<std::string> StringListParameter(const duckdb::Value &value) {
	std::vector<std::string> out;
	if (value.IsNull()) {
		return out;
	}
	for (const auto &child : duckdb::ListValue::GetChildren(value)) {
		if (!child.IsNull()) {
			out.push_back(child.GetValue<std::string>());
		}
	}
	return out;
}

std::vector<int64_t> Int64ListParameter(const duckdb::Value &value) {
	std::vector<int64_t> out;
	if (value.IsNull()) {
		return out;
	}
	for (const auto &child : duckdb::ListValue::GetChildren(value)) {
		if (!child.IsNull()) {
			out.push_back(child.GetValue<int64_t>());
		}
	}
	return out;
}

std::vector<std::string> DmlChangeTypeList(const duckdb::Value &value) {
	auto requested = StringListParameter(value);
	if (requested.empty()) {
		requested.push_back("*");
	}
	std::vector<std::string> out;
	for (const auto &change_type : requested) {
		if (change_type == "*") {
			for (const auto &kind : {"insert", "update_preimage", "update_postimage", "delete"}) {
				out.push_back(kind);
			}
			continue;
		}
		if (change_type != "insert" && change_type != "update_preimage" && change_type != "update_postimage" &&
		    change_type != "delete") {
			throw duckdb::InvalidInputException("cdc_dml_consumer_create: invalid change_type '%s'", change_type);
		}
		out.push_back(change_type);
	}
	std::sort(out.begin(), out.end());
	out.erase(std::unique(out.begin(), out.end()), out.end());
	return out;
}

void AddResolvedSubscription(std::vector<ResolvedSubscriptionInput> &out, const std::string &scope_kind,
                             const duckdb::Value &schema_id, const duckdb::Value &table_id,
                             const std::string &event_category, const std::string &change_type,
                             const duckdb::Value &original_name) {
	ResolvedSubscriptionInput sub;
	sub.scope_kind = scope_kind;
	sub.schema_id = schema_id;
	sub.table_id = table_id;
	sub.event_category = event_category;
	sub.change_type = change_type;
	sub.original_qualified_name = original_name;
	out.push_back(std::move(sub));
}

//! Resolve a DML consumer scope. Omitting table identity creates one
//! catalog-wide subscription for schema-independent ticks. A table-scoped
//! consumer fans out one subscription per requested row change type.
std::vector<ResolvedSubscriptionInput> ResolveDmlCreateSubscriptions(duckdb::Connection &conn,
                                                                     const std::string &catalog_name,
                                                                     const ConsumerCreateData &data,
                                                                     int64_t snapshot_id) {
	if (data.table_name.IsNull() && data.table_id.IsNull()) {
		std::vector<ResolvedSubscriptionInput> out;
		AddResolvedSubscription(out, "catalog", duckdb::Value(), duckdb::Value(), "dml", "*", duckdb::Value());
		return out;
	}
	const auto change_types = DmlChangeTypeList(data.change_types);
	int64_t schema_id = 0;
	int64_t table_id = 0;
	std::string qualified;
	if (!data.table_id.IsNull()) {
		table_id = data.table_id.GetValue<int64_t>();
		if (!ResolveTableByIdAt(conn, catalog_name, table_id, snapshot_id, schema_id, qualified)) {
			int64_t first_live_snapshot = -1;
			if (!StartAtMeansBeginning(data.start_at) ||
			    !ResolveFirstLiveTableById(conn, catalog_name, table_id, first_live_snapshot, schema_id, qualified)) {
				throw duckdb::InvalidInputException("cdc_dml_consumer_create: table_id %lld is not live",
				                                    static_cast<long long>(table_id));
			}
		}
	} else {
		qualified = QualifyTableInput(data.table_name.GetValue<std::string>(), duckdb::Value());
		if (!ResolveCurrentTableName(conn, catalog_name, qualified, snapshot_id, schema_id, table_id)) {
			int64_t first_live_snapshot = -1;
			if (!StartAtMeansBeginning(data.start_at) ||
			    !ResolveFirstLiveTableByName(conn, catalog_name, qualified, first_live_snapshot, schema_id, table_id)) {
				throw duckdb::InvalidInputException("cdc_dml_consumer_create: table identity '%s' is not live",
				                                    qualified);
			}
		}
	}
	std::vector<ResolvedSubscriptionInput> out;
	out.reserve(change_types.size());
	for (const auto &change_type : change_types) {
		AddResolvedSubscription(out, "table", duckdb::Value::BIGINT(schema_id), duckdb::Value::BIGINT(table_id), "dml",
		                        change_type, duckdb::Value(qualified));
	}
	return out;
}

std::vector<ResolvedSubscriptionInput> ResolveDdlCreateSubscriptions(duckdb::Connection &conn,
                                                                     const std::string &catalog_name,
                                                                     const ConsumerCreateData &data,
                                                                     int64_t snapshot_id) {
	std::vector<ResolvedSubscriptionInput> out;
	for (const auto &schema_name : StringListParameter(data.schemas)) {
		int64_t schema_id = 0;
		if (!ResolveSchemaByNameAt(conn, catalog_name, schema_name, snapshot_id, schema_id)) {
			throw duckdb::InvalidInputException("cdc_ddl_consumer_create: schema identity '%s' is not live",
			                                    schema_name);
		}
		AddResolvedSubscription(out, "schema", duckdb::Value::BIGINT(schema_id), duckdb::Value(), "ddl", "*",
		                        duckdb::Value(schema_name));
	}
	for (const auto &schema_id : Int64ListParameter(data.schema_ids)) {
		std::string schema_name;
		if (!ResolveSchemaByIdAt(conn, catalog_name, schema_id, snapshot_id, schema_name)) {
			throw duckdb::InvalidInputException("cdc_ddl_consumer_create: schema_id %lld is not live",
			                                    static_cast<long long>(schema_id));
		}
		AddResolvedSubscription(out, "schema", duckdb::Value::BIGINT(schema_id), duckdb::Value(), "ddl", "*",
		                        duckdb::Value(schema_name));
	}
	for (const auto &table_name : StringListParameter(data.table_names)) {
		const auto qualified = QualifyTableInput(table_name, duckdb::Value());
		int64_t schema_id = 0;
		int64_t table_id = 0;
		if (!ResolveCurrentTableName(conn, catalog_name, qualified, snapshot_id, schema_id, table_id)) {
			throw duckdb::InvalidInputException("cdc_ddl_consumer_create: table identity '%s' is not live", qualified);
		}
		AddResolvedSubscription(out, "table", duckdb::Value::BIGINT(schema_id), duckdb::Value::BIGINT(table_id), "ddl",
		                        "*", duckdb::Value(qualified));
	}
	for (const auto &table_id : Int64ListParameter(data.table_ids)) {
		int64_t schema_id = 0;
		std::string current_name;
		if (!ResolveTableByIdAt(conn, catalog_name, table_id, snapshot_id, schema_id, current_name)) {
			throw duckdb::InvalidInputException("cdc_ddl_consumer_create: table_id %lld is not live",
			                                    static_cast<long long>(table_id));
		}
		AddResolvedSubscription(out, "table", duckdb::Value::BIGINT(schema_id), duckdb::Value::BIGINT(table_id), "ddl",
		                        "*", duckdb::Value(current_name));
	}
	if (out.empty()) {
		AddResolvedSubscription(out, "catalog", duckdb::Value(), duckdb::Value(), "ddl", "*", duckdb::Value());
	}
	return out;
}

std::vector<ResolvedSubscriptionInput> ResolveCreateSubscriptions(duckdb::Connection &conn,
                                                                  const std::string &catalog_name,
                                                                  const ConsumerCreateData &data, int64_t snapshot_id) {
	if (data.consumer_kind == "dml") {
		return ResolveDmlCreateSubscriptions(conn, catalog_name, data, snapshot_id);
	}
	return ResolveDdlCreateSubscriptions(conn, catalog_name, data, snapshot_id);
}

std::vector<duckdb::Value> CreateConsumerOnce(duckdb::ClientContext &context, const ConsumerCreateData &data) {
	// Open ONE connection up front so the version probe, the bootstrap
	// CREATE writes, the start_at snapshot lookup, and the INSERTs all
	// share a single SQLite file handle when the metadata catalog is
	// SQLite-backed. The cross-connection variant of this chain trips
	// Windows MinGW's `LockFileEx` / `ERROR_POSSIBLE_DEADLOCK` path
	// (H-022 in `docs/hazard-log.md`).
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto subscriptions_table = StateTable(conn, data.catalog_name, CONSUMER_SUBSCRIPTIONS_TABLE);
	const auto quoted_name = QuoteLiteral(data.consumer_name);
	const auto actor_sql = "current_user";
	int64_t resolved_snapshot = ResolveCreateSnapshot(conn, data.catalog_name, data.start_at);
	int64_t schema_version = ResolveSchemaVersion(conn, data.catalog_name, resolved_snapshot);
	const auto subscriptions = ResolveCreateSubscriptions(conn, data.catalog_name, data, resolved_snapshot);

	// Audit details JSON stays append-only — every key from the create
	// call lands here (resolved snapshot included so a later
	// cdc_consumer_reset retains forensic continuity even if the
	// consumer is dropped/recreated under the same name).
	std::ostringstream details;
	details << "{\"consumer_kind\":\"" << JsonEscape(data.consumer_kind) << "\",\"start_at\":\""
	        << JsonEscape(data.start_at) << "\",\"start_at_resolved_snapshot\":" << resolved_snapshot;
	details << ",\"subscriptions\":" << SubscriptionJsonArray(subscriptions);
	details << "}";

	try {
		ExecuteChecked(conn, "BEGIN TRANSACTION");
		auto duplicate_result =
		    conn.Query("SELECT 1 FROM " + consumers + " WHERE consumer_name = " + quoted_name + " LIMIT 1");
		if (duplicate_result && !duplicate_result->HasError() && duplicate_result->RowCount() > 0) {
			throw duckdb::ConstraintException(
			    "PRIMARY KEY or UNIQUE constraint violation: consumer '%s' already exists", data.consumer_name);
		}
		auto next_id_result = conn.Query("SELECT COALESCE(MAX(consumer_id), 0) + 1 FROM " + consumers);
		int64_t consumer_id = SingleInt64(*next_id_result, "next consumer_id");
		// Pin the single subscribed table_id on the consumer row for DML
		// consumers (subscriptions[0] is the canonical entry — multiple
		// rows may exist when filtering specific change_types, but they
		// all share the same table_id by construction). DDL consumers
		// store NULL.
		std::string table_id_sql = "NULL";
		if (data.consumer_kind == "dml" && !subscriptions.empty() && !subscriptions.front().table_id.IsNull()) {
			table_id_sql = subscriptions.front().table_id.ToString();
		}
		ExecuteChecked(conn, "INSERT INTO " + consumers +
		                         " (consumer_name, consumer_kind, consumer_id, table_id, last_committed_snapshot, "
		                         "last_committed_schema_version, created_at, created_by, updated_at, "
		                         "lease_interval_seconds, metadata) VALUES (" +
		                         quoted_name + ", " + QuoteLiteral(data.consumer_kind) + ", " +
		                         std::to_string(consumer_id) + ", " + table_id_sql + ", " +
		                         std::to_string(resolved_snapshot) + ", " + std::to_string(schema_version) +
		                         ", now(), " + actor_sql + ", now(), 60, " + JsonValue(data.metadata) + ")");
		for (size_t i = 0; i < subscriptions.size(); ++i) {
			const auto &sub = subscriptions[i];
			ExecuteChecked(conn, "INSERT INTO " + subscriptions_table +
			                         " (consumer_id, subscription_id, scope_kind, schema_id, table_id, event_category, "
			                         "change_type, original_qualified_name, created_at, metadata) VALUES (" +
			                         std::to_string(consumer_id) + ", " + std::to_string(i + 1) + ", " +
			                         QuoteLiteral(sub.scope_kind) + ", " +
			                         (sub.schema_id.IsNull() ? "NULL" : sub.schema_id.ToString()) + ", " +
			                         (sub.table_id.IsNull() ? "NULL" : sub.table_id.ToString()) + ", " +
			                         QuoteLiteral(sub.event_category) + ", " + QuoteLiteral(sub.change_type) + ", " +
			                         (sub.original_qualified_name.IsNull()
			                              ? "NULL"
			                              : QuoteLiteral(sub.original_qualified_name.ToString())) +
			                         ", now(), NULL)");
		}
		InsertAuditRow(conn, data.catalog_name, "consumer_create", data.consumer_name, consumer_id, details.str());
		ExecuteChecked(conn, "COMMIT");
		std::vector<duckdb::Value> first_row;
		const auto decorated = LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
		if (decorated.empty()) {
			return {duckdb::Value(data.consumer_name),
			        duckdb::Value(data.consumer_kind),
			        duckdb::Value::BIGINT(consumer_id),
			        duckdb::Value::BIGINT(resolved_snapshot),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value()};
		}
		const auto &sub = decorated[0];
		return {duckdb::Value(data.consumer_name),
		        duckdb::Value(data.consumer_kind),
		        duckdb::Value::BIGINT(consumer_id),
		        duckdb::Value::BIGINT(resolved_snapshot),
		        duckdb::Value::BIGINT(sub.subscription_id),
		        duckdb::Value(sub.scope_kind),
		        sub.schema_id,
		        sub.table_id,
		        duckdb::Value(sub.event_category),
		        duckdb::Value(sub.change_type),
		        sub.original_qualified_name,
		        sub.current_qualified_name};
	} catch (...) {
		try {
			ExecuteChecked(conn, "ROLLBACK");
		} catch (...) {
		}
		throw;
	}
}

std::vector<duckdb::Value> CreateConsumer(duckdb::ClientContext &context, const ConsumerCreateData &data) {
	for (int attempt = 0; attempt < 5; ++attempt) {
		try {
			return CreateConsumerOnce(context, data);
		} catch (const std::exception &ex) {
			if (!IsH022TransientMessage(ex.what()) || attempt == 4) {
				throw;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
		}
	}
	throw duckdb::InternalException("cdc consumer create retry loop exited unexpectedly");
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerCreateInit(duckdb::ClientContext &context,
                                                                        duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerCreateData>();
	result->rows.push_back(CreateConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_reset
//===--------------------------------------------------------------------===//

struct ConsumerResetData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	std::string to_snapshot;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerResetData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->to_snapshot = to_snapshot;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

std::string ToSnapshotParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("to_snapshot");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return "";
	}
	return entry->second.GetValue<std::string>();
}

std::string ResetKind(const std::string &to_snapshot) {
	if (to_snapshot.empty()) {
		return "oldest_available";
	}
	const auto lower = duckdb::StringUtil::Lower(to_snapshot);
	if (lower == "beginning" || lower == "oldest" || lower == "oldest_available") {
		return "oldest_available";
	}
	if (lower == "now") {
		return "now";
	}
	int64_t ignored = 0;
	if (TryParseInt64(to_snapshot, ignored)) {
		return "snapshot_id";
	}
	return "timestamp";
}

duckdb::unique_ptr<duckdb::FunctionData> ConsumerResetBind(duckdb::ClientContext &context,
                                                           duckdb::TableFunctionBindInput &input,
                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                           duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_reset requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerResetData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->to_snapshot = ToSnapshotParameter(input);

	names = {"consumer_name", "consumer_id", "previous_snapshot", "new_snapshot"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT};
	return std::move(result);
}

std::vector<duckdb::Value> ResetConsumer(duckdb::ClientContext &context, const ConsumerResetData &data) {
	// Single-connection chain — see CreateConsumer for the H-022 rationale.
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	int64_t resolved_snapshot = ResolveResetSnapshot(conn, data.catalog_name, data.to_snapshot);
	ValidateDmlConsumerResetTarget(conn, data.catalog_name, row, resolved_snapshot);
	int64_t schema_version = ResolveSchemaVersion(conn, data.catalog_name, resolved_snapshot);
	const auto details = "{\"from_snapshot\":" + std::to_string(row.last_committed_snapshot) +
	                     ",\"to_snapshot\":" + std::to_string(resolved_snapshot) + ",\"reset_kind\":\"" +
	                     JsonEscape(ResetKind(data.to_snapshot)) + "\"}";
	ExecuteChecked(conn, "UPDATE " + consumers + " SET last_committed_snapshot = " + std::to_string(resolved_snapshot) +
	                         ", last_committed_schema_version = " + std::to_string(schema_version) +
	                         ", owner_token = NULL, owner_acquired_at = NULL, owner_heartbeat_at = NULL, "
	                         "updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(data.consumer_name));
	InsertAuditRow(conn, data.catalog_name, "consumer_reset", data.consumer_name, row.consumer_id, details);
	return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id),
	        duckdb::Value::BIGINT(row.last_committed_snapshot), duckdb::Value::BIGINT(resolved_snapshot)};
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerResetInit(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerResetData>();
	result->rows.push_back(ResetConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_drop
//===--------------------------------------------------------------------===//

struct ConsumerDropData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerDropData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerDropBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_drop requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerDropData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");

	names = {"consumer_name", "consumer_id", "last_committed_snapshot"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	return std::move(result);
}

std::vector<duckdb::Value> DropConsumer(duckdb::ClientContext &context, const ConsumerDropData &data) {
	// Single-connection chain — see CreateConsumer for the H-022 rationale.
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto subscriptions = StateTable(conn, data.catalog_name, CONSUMER_SUBSCRIPTIONS_TABLE);
	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	const auto details = "{\"last_committed_snapshot\":" + std::to_string(row.last_committed_snapshot) +
	                     ",\"last_committed_schema_version\":" + std::to_string(row.last_committed_schema_version) +
	                     "}";
	InsertAuditRow(conn, data.catalog_name, "consumer_drop", data.consumer_name, row.consumer_id, details);
	ExecuteChecked(conn, "DELETE FROM " + subscriptions + " WHERE consumer_id = " + std::to_string(row.consumer_id));
	ExecuteChecked(conn, "DELETE FROM " + consumers + " WHERE consumer_name = " + QuoteLiteral(data.consumer_name));
	return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id),
	        duckdb::Value::BIGINT(row.last_committed_snapshot)};
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerDropInit(duckdb::ClientContext &context,
                                                                      duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerDropData>();
	result->rows.push_back(DropConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_force_release
//===--------------------------------------------------------------------===//

struct ConsumerForceReleaseData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerForceReleaseData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerForceReleaseBind(duckdb::ClientContext &context,
                                                                  duckdb::TableFunctionBindInput &input,
                                                                  duckdb::vector<duckdb::LogicalType> &return_types,
                                                                  duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_force_release requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerForceReleaseData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");

	names = {"consumer_name", "consumer_id", "previous_token"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

std::vector<duckdb::Value> ForceReleaseConsumer(duckdb::ClientContext &context, const ConsumerForceReleaseData &data) {
	// Single-connection chain — see CreateConsumer for the H-022 rationale.
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	const auto details = "{\"previous_token\":" + JsonValue(row.owner_token) +
	                     ",\"previous_acquired_at\":" + JsonValue(row.owner_acquired_at) +
	                     ",\"previous_heartbeat_at\":" + JsonValue(row.owner_heartbeat_at) + "}";
	ExecuteChecked(conn, "UPDATE " + consumers +
	                         " SET owner_token = NULL, owner_acquired_at = NULL, owner_heartbeat_at = NULL, "
	                         "updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(data.consumer_name));
	InsertAuditRow(conn, data.catalog_name, "consumer_force_release", data.consumer_name, row.consumer_id, details);
	duckdb::Value previous_token =
	    row.owner_token.IsNull() ? duckdb::Value() : duckdb::Value(row.owner_token.ToString());
	return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id), previous_token};
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerForceReleaseInit(duckdb::ClientContext &context,
                                                                              duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerForceReleaseData>();
	result->rows.push_back(ForceReleaseConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_release
//===--------------------------------------------------------------------===//

void ThrowBusy(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name);

struct ConsumerReleaseData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerReleaseData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerReleaseBind(duckdb::ClientContext &context,
                                                             duckdb::TableFunctionBindInput &input,
                                                             duckdb::vector<duckdb::LogicalType> &return_types,
                                                             duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_release requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerReleaseData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");

	names = {"consumer_name", "consumer_id", "released_token"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::UUID};
	return std::move(result);
}

std::vector<duckdb::Value> ReleaseConsumer(duckdb::ClientContext &context, const ConsumerReleaseData &data) {
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	const auto cached_token = CachedToken(context, conn, data.catalog_name, data.consumer_name);
	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (cached_token.empty() || row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}

	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	ExecuteChecked(conn, "UPDATE " + consumers +
	                         " SET owner_token = NULL, owner_acquired_at = NULL, owner_heartbeat_at = NULL, "
	                         "updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(data.consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token));
	row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (!row.owner_token.IsNull()) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}
	EraseCachedToken(context, conn, data.catalog_name, data.consumer_name);
	const auto details = "{\"released_token\":\"" + JsonEscape(cached_token) + "\"}";
	InsertAuditRow(conn, data.catalog_name, "consumer_release", data.consumer_name, row.consumer_id, details);
	return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id), duckdb::Value(cached_token)};
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerReleaseInit(duckdb::ClientContext &context,
                                                                         duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerReleaseData>();
	result->rows.push_back(ReleaseConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_window
//===--------------------------------------------------------------------===//

duckdb::unique_ptr<duckdb::FunctionData> CdcWindowBind(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionBindInput &input,
                                                       duckdb::vector<duckdb::LogicalType> &return_types,
                                                       duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_window requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcWindowData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->max_snapshots = MaxSnapshotsParameter(input);

	names = {"start_snapshot",         "end_snapshot", "has_changes",         "schema_version",
	         "schema_changes_pending", "terminal",     "terminal_at_snapshot"};
	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BIGINT};
	return std::move(result);
}

void ThrowMaxSnapshotsExceeded(int64_t requested) {
	throw duckdb::InvalidInputException(
	    "CDC_MAX_SNAPSHOTS_EXCEEDED: cdc_window was called with max_snapshots => %lld, but the session hard cap is "
	    "%lld. To raise the cap for this session: SET ducklake_cdc_max_snapshots_hard_cap = %lld; Caps above ~10000 "
	    "are likely to read megabytes of catalog state in a single transaction; consider committing in smaller "
	    "batches instead.",
	    static_cast<long long>(requested), static_cast<long long>(HARD_MAX_SNAPSHOTS),
	    static_cast<long long>(requested));
}

void ThrowBusy(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name) {
	auto row = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
	bool likely_dead = false;
	if (!row.owner_heartbeat_at.IsNull()) {
		auto result = conn.Query("SELECT epoch(now()) - epoch(" + QuoteLiteral(row.owner_heartbeat_at.ToString()) +
		                         "::TIMESTAMP WITH TIME ZONE) > " + std::to_string(2 * row.lease_interval_seconds));
		likely_dead = result && !result->HasError() && result->RowCount() > 0 && !result->GetValue(0, 0).IsNull() &&
		              result->GetValue(0, 0).GetValue<bool>();
	}
	throw duckdb::InvalidInputException(BuildBusyMessage(catalog_name, consumer_name, row.owner_token,
	                                                     row.owner_acquired_at, row.owner_heartbeat_at,
	                                                     row.lease_interval_seconds, likely_dead));
}

bool LeaseExpired(duckdb::Connection &conn, const ConsumerRow &row, int64_t multiplier = 1) {
	if (row.owner_token.IsNull() || row.owner_heartbeat_at.IsNull()) {
		return false;
	}
	auto result =
	    conn.Query("SELECT epoch(now()) - epoch(" + QuoteLiteral(row.owner_heartbeat_at.ToString()) +
	               "::TIMESTAMP WITH TIME ZONE) > " + std::to_string(multiplier * row.lease_interval_seconds));
	return result && !result->HasError() && result->RowCount() > 0 && !result->GetValue(0, 0).IsNull() &&
	       result->GetValue(0, 0).GetValue<bool>();
}

ConsumerRow AcquireLeaseLegacy(duckdb::Connection &conn, const std::string &catalog_name,
                               const std::string &consumer_name, const std::string &cached_token) {
	const auto consumers = StateTable(conn, catalog_name, CONSUMERS_TABLE);
	const auto new_token = GenerateUuid(conn);
	const auto cached_token_sql = TokenSqlOrNull(cached_token);
	auto row = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
	const bool expired = LeaseExpired(conn, row);
	const bool owns_lease =
	    !row.owner_token.IsNull() && !cached_token.empty() && row.owner_token.ToString() == cached_token;
	if (!row.owner_token.IsNull() && !expired && !owns_lease) {
		ThrowBusy(conn, catalog_name, consumer_name);
	}
	const auto owner_token =
	    row.owner_token.IsNull() || (expired && !owns_lease) ? new_token : row.owner_token.ToString();
	if (expired && !owns_lease) {
		const auto details = "{\"previous_token\":" + JsonValue(row.owner_token) +
		                     ",\"previous_acquired_at\":" + JsonValue(row.owner_acquired_at) +
		                     ",\"previous_heartbeat_at\":" + JsonValue(row.owner_heartbeat_at) +
		                     ",\"lease_interval_seconds\":" + std::to_string(row.lease_interval_seconds) + "}";
		InsertAuditRow(conn, catalog_name, "lease_force_acquire", consumer_name, row.consumer_id, details);
	}
	ExecuteChecked(conn, "UPDATE " + consumers + " SET owner_token = " + TokenSqlOrNull(owner_token) +
	                         ", owner_acquired_at = CASE WHEN owner_token IS NULL OR epoch(now()) - "
	                         "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > lease_interval_seconds THEN "
	                         "now() ELSE owner_acquired_at::TIMESTAMP WITH TIME ZONE "
	                         "END, owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(consumer_name) +
	                         " AND (owner_token IS NULL OR epoch(now()) - "
	                         "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > lease_interval_seconds OR "
	                         "owner_token = " +
	                         cached_token_sql + ")");
	row = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
	if (row.owner_token.IsNull() || row.owner_token.ToString() != owner_token) {
		ThrowBusy(conn, catalog_name, consumer_name);
	}
	return row;
}

ConsumerRow AcquireLeaseReturning(duckdb::Connection &conn, const std::string &catalog_name,
                                  const std::string &consumer_name, const std::string &cached_token) {
	const auto consumers = StateTable(conn, catalog_name, CONSUMERS_TABLE);
	const auto cached_token_sql = TokenSqlOrNull(cached_token);
	auto result =
	    conn.Query("UPDATE " + consumers +
	               " SET owner_token = CASE WHEN owner_token IS NULL THEN CAST(uuid() AS VARCHAR) "
	               "ELSE CAST(owner_token AS VARCHAR) END, "
	               "owner_acquired_at = CASE WHEN owner_token IS NULL OR epoch(now()) - "
	               "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > lease_interval_seconds THEN now() "
	               "ELSE owner_acquired_at::TIMESTAMP WITH TIME ZONE END, owner_heartbeat_at = now(), updated_at = "
	               "now() WHERE consumer_name = " +
	               QuoteLiteral(consumer_name) + " AND (owner_token IS NULL OR owner_token = " + cached_token_sql +
	               ") RETURNING " + ConsumerRowProjection());
	ThrowIfQueryFailed(result);
	if (result && result->RowCount() > 0) {
		return ConsumerRowFromResult(*result, 0);
	}

	auto previous = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
	if (!LeaseExpired(conn, previous)) {
		ThrowBusy(conn, catalog_name, consumer_name);
	}

	const auto previous_token_sql =
	    TokenSqlOrNull(previous.owner_token.IsNull() ? std::string() : previous.owner_token.ToString());
	result =
	    conn.Query("UPDATE " + consumers +
	               " SET owner_token = CAST(uuid() AS VARCHAR), owner_acquired_at = now(), owner_heartbeat_at = now(), "
	               "updated_at = now() WHERE consumer_name = " +
	               QuoteLiteral(consumer_name) + " AND owner_token = " + previous_token_sql +
	               " AND epoch(now()) - epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > "
	               "lease_interval_seconds RETURNING " +
	               ConsumerRowProjection());
	ThrowIfQueryFailed(result);
	if (!result || result->RowCount() == 0) {
		ThrowBusy(conn, catalog_name, consumer_name);
	}

	auto acquired = ConsumerRowFromResult(*result, 0);
	const auto details = "{\"previous_token\":" + JsonValue(previous.owner_token) +
	                     ",\"previous_acquired_at\":" + JsonValue(previous.owner_acquired_at) +
	                     ",\"previous_heartbeat_at\":" + JsonValue(previous.owner_heartbeat_at) +
	                     ",\"lease_interval_seconds\":" + std::to_string(previous.lease_interval_seconds) + "}";
	InsertAuditRow(conn, catalog_name, "lease_force_acquire", consumer_name, previous.consumer_id, details);
	return acquired;
}

ConsumerRow AcquireLease(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                         const std::string &cached_token, StateBackendKind backend) {
	if (SupportsUpdateReturning(backend)) {
		return AcquireLeaseReturning(conn, catalog_name, consumer_name, cached_token);
	}
	return AcquireLeaseLegacy(conn, catalog_name, consumer_name, cached_token);
}

bool TryUseFreshCachedLease(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                            const std::string &cached_token, ConsumerRow &row) {
	if (cached_token.empty()) {
		return false;
	}
	const auto consumers = StateTable(conn, catalog_name, CONSUMERS_TABLE);
	auto result = conn.Query("SELECT " + ConsumerRowProjection() + " FROM " + consumers + " WHERE consumer_name = " +
	                         QuoteLiteral(consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token) +
	                         " AND owner_heartbeat_at IS NOT NULL AND epoch(now()) - "
	                         "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) < "
	                         "lease_interval_seconds / 4.0 LIMIT 1");
	ThrowIfQueryFailed(result);
	if (!result || result->RowCount() == 0) {
		return false;
	}
	row = ConsumerRowFromResult(*result, 0);
	return true;
}

struct WindowResolution {
	int64_t current_snapshot;
	duckdb::Value oldest_snapshot;
	bool last_snapshot_exists;
	int64_t first_snapshot;
	int64_t start_snapshot;
	int64_t end_snapshot;
	duckdb::Value last_schema_version;
	duckdb::Value start_schema_version;
	duckdb::Value end_schema_version;
	bool start_is_schema_change = false;
	int64_t next_schema_change = -1;
	duckdb::Value next_schema_change_schema_version;
};

struct DmlWindowResolution {
	int64_t current_snapshot = -1;
	duckdb::Value oldest_snapshot;
	bool last_snapshot_exists = false;
	int64_t start_snapshot = -1;
	int64_t end_snapshot = -1;
	bool has_changes = false;
	int64_t schema_version = -1;
	int64_t boundary_snapshot = -1;
	duckdb::Value boundary_schema_version;
};

int64_t RequiredInt64(const duckdb::Value &value, const std::string &description) {
	if (value.IsNull()) {
		throw duckdb::InvalidInputException("Unable to resolve %s", description);
	}
	return value.GetValue<int64_t>();
}

DmlWindowResolution ResolveDmlWindowIndexed(duckdb::Connection &conn, const std::string &catalog_name,
                                            int64_t last_snapshot, int64_t max_snapshots,
                                            const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	const auto snapshot_table = MetadataTable(conn, catalog_name, "ducklake_snapshot");
	const auto last_snapshot_sql = std::to_string(last_snapshot);
	auto result =
	    conn.Query("WITH snapshot_bounds AS ("
	               "SELECT max(snapshot_id) AS current_snapshot, min(snapshot_id) AS oldest_snapshot, "
	               "count(*) FILTER (WHERE snapshot_id = " +
	               last_snapshot_sql + ") AS last_exists FROM " + snapshot_table +
	               ") SELECT b.current_snapshot, b.oldest_snapshot, b.last_exists > 0 AS last_snapshot_exists, "
	               "last_s.schema_version AS last_schema_version FROM snapshot_bounds b LEFT JOIN " +
	               snapshot_table + " last_s ON last_s.snapshot_id = " + last_snapshot_sql);
	ThrowIfQueryFailed(result);
	if (!result || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		throw duckdb::InvalidInputException("Unable to resolve current snapshot");
	}

	DmlWindowResolution resolution;
	resolution.current_snapshot = result->GetValue(0, 0).GetValue<int64_t>();
	resolution.oldest_snapshot = result->GetValue(1, 0);
	resolution.last_snapshot_exists = !result->GetValue(2, 0).IsNull() && result->GetValue(2, 0).GetValue<bool>();
	if (!resolution.last_snapshot_exists) {
		return resolution;
	}
	resolution.schema_version =
	    last_snapshot <= resolution.current_snapshot ? RequiredInt64(result->GetValue(3, 0), "schema_version") : -1;

	const auto first_dml_snapshot =
	    FirstDmlSubscribedSnapshot(conn, catalog_name, last_snapshot, resolution.current_snapshot, subscriptions);
	const auto boundary_snapshot = NextDmlSubscribedSchemaChangeSnapshot(conn, catalog_name, last_snapshot,
	                                                                     resolution.current_snapshot, subscriptions);
	if (first_dml_snapshot == -1 && boundary_snapshot == -1) {
		resolution.start_snapshot = last_snapshot + 1;
		resolution.end_snapshot = last_snapshot;
		return resolution;
	}
	if (boundary_snapshot != -1 && (first_dml_snapshot == -1 || boundary_snapshot <= first_dml_snapshot)) {
		resolution.start_snapshot = boundary_snapshot;
		resolution.end_snapshot = boundary_snapshot - 1;
		resolution.boundary_snapshot = boundary_snapshot;
		resolution.boundary_schema_version =
		    duckdb::Value::BIGINT(ResolveSchemaVersion(conn, catalog_name, boundary_snapshot));
		return resolution;
	}

	resolution.start_snapshot = first_dml_snapshot;
	resolution.end_snapshot = std::min(resolution.current_snapshot, first_dml_snapshot + max_snapshots - 1);
	if (boundary_snapshot != -1 && boundary_snapshot <= resolution.end_snapshot) {
		resolution.end_snapshot = boundary_snapshot - 1;
		resolution.boundary_snapshot = boundary_snapshot;
		resolution.boundary_schema_version =
		    duckdb::Value::BIGINT(ResolveSchemaVersion(conn, catalog_name, boundary_snapshot));
	}
	resolution.has_changes = resolution.end_snapshot >= resolution.start_snapshot;
	if (resolution.has_changes) {
		resolution.schema_version = ResolveSchemaVersion(conn, catalog_name, resolution.start_snapshot);
	}
	return resolution;
}

WindowResolution ResolveWindowFast(duckdb::Connection &conn, const std::string &catalog_name, int64_t last_snapshot,
                                   int64_t max_snapshots) {
	const auto snapshot_table = MetadataTable(conn, catalog_name, "ducklake_snapshot");
	const auto changes_table = MetadataTable(conn, catalog_name, "ducklake_snapshot_changes");
	const auto last_snapshot_sql = std::to_string(last_snapshot);
	const auto max_snapshots_sql = std::to_string(max_snapshots);
	const auto schema_change_filter = "(sc.changes_made LIKE 'created_%' OR sc.changes_made LIKE '%,created_%' OR "
	                                  "sc.changes_made LIKE 'altered_%' OR sc.changes_made LIKE '%,altered_%' OR "
	                                  "sc.changes_made LIKE 'dropped_%' OR sc.changes_made LIKE '%,dropped_%' OR "
	                                  "sc.changes_made LIKE 'renamed_%' OR sc.changes_made LIKE '%,renamed_%')";
	auto result =
	    conn.Query("WITH snapshot_bounds AS ("
	               "SELECT max(snapshot_id) AS current_snapshot, min(snapshot_id) AS oldest_snapshot, "
	               "count(*) FILTER (WHERE snapshot_id = " +
	               last_snapshot_sql + ") AS last_exists FROM " + snapshot_table +
	               "), first_change AS ("
	               "SELECT min(sc.snapshot_id) AS first_snapshot FROM " +
	               changes_table + " sc, snapshot_bounds b WHERE sc.snapshot_id > " + last_snapshot_sql +
	               " AND sc.snapshot_id <= b.current_snapshot), computed AS ("
	               "SELECT b.current_snapshot, b.oldest_snapshot, b.last_exists > 0 AS last_snapshot_exists, "
	               "COALESCE(f.first_snapshot, -1) AS first_snapshot, "
	               "CASE WHEN f.first_snapshot IS NULL THEN " +
	               last_snapshot_sql +
	               " + 1 ELSE f.first_snapshot END AS start_snapshot, "
	               "CASE WHEN f.first_snapshot IS NULL THEN " +
	               last_snapshot_sql + " ELSE LEAST(b.current_snapshot, f.first_snapshot + " + max_snapshots_sql +
	               " - 1) END AS end_snapshot FROM snapshot_bounds b CROSS JOIN first_change f)"
	               " SELECT c.current_snapshot, c.oldest_snapshot, c.last_snapshot_exists, c.first_snapshot, "
	               "c.start_snapshot, c.end_snapshot, last_s.schema_version AS last_schema_version, "
	               "start_s.schema_version AS start_schema_version, end_s.schema_version AS end_schema_version, "
	               "sc.snapshot_id AS change_snapshot, sc.changes_made, change_s.schema_version AS "
	               "change_schema_version FROM computed c LEFT JOIN " +
	               snapshot_table + " last_s ON last_s.snapshot_id = " + last_snapshot_sql + " LEFT JOIN " +
	               snapshot_table + " start_s ON start_s.snapshot_id = c.start_snapshot LEFT JOIN " + snapshot_table +
	               " end_s ON end_s.snapshot_id = c.end_snapshot LEFT JOIN " + changes_table +
	               " sc ON sc.snapshot_id >= c.start_snapshot AND sc.snapshot_id <= c.current_snapshot AND " +
	               schema_change_filter + " LEFT JOIN " + snapshot_table +
	               " change_s ON change_s.snapshot_id = sc.snapshot_id ORDER BY sc.snapshot_id ASC");
	ThrowIfQueryFailed(result);
	if (!result || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		throw duckdb::InvalidInputException("Unable to resolve current snapshot");
	}

	WindowResolution resolution;
	resolution.current_snapshot = result->GetValue(0, 0).GetValue<int64_t>();
	resolution.oldest_snapshot = result->GetValue(1, 0);
	resolution.last_snapshot_exists = !result->GetValue(2, 0).IsNull() && result->GetValue(2, 0).GetValue<bool>();
	resolution.first_snapshot = result->GetValue(3, 0).GetValue<int64_t>();
	resolution.start_snapshot = result->GetValue(4, 0).GetValue<int64_t>();
	resolution.end_snapshot = result->GetValue(5, 0).GetValue<int64_t>();
	resolution.last_schema_version = result->GetValue(6, 0);
	resolution.start_schema_version = result->GetValue(7, 0);
	resolution.end_schema_version = result->GetValue(8, 0);
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		const auto snapshot_value = result->GetValue(9, i);
		const auto changes_value = result->GetValue(10, i);
		if (snapshot_value.IsNull() || changes_value.IsNull()) {
			continue;
		}
		const auto snapshot_id = snapshot_value.GetValue<int64_t>();
		if (!SnapshotHasSchemaChange(changes_value.ToString())) {
			continue;
		}
		if (snapshot_id == resolution.start_snapshot) {
			resolution.start_is_schema_change = true;
			continue;
		}
		if (resolution.next_schema_change == -1) {
			resolution.next_schema_change = snapshot_id;
			resolution.next_schema_change_schema_version = result->GetValue(11, i);
		}
	}
	return resolution;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcWindowInit(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcWindowData>();
	result->rows.push_back(ReadWindow(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_commit
//===--------------------------------------------------------------------===//

struct CdcCommitData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t snapshot_id;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcCommitData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->snapshot_id = snapshot_id;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcCommitBind(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionBindInput &input,
                                                       duckdb::vector<duckdb::LogicalType> &return_types,
                                                       duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 3) {
		throw duckdb::BinderException("cdc_commit requires catalog, consumer name, and snapshot_id");
	}
	auto result = duckdb::make_uniq<CdcCommitData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->snapshot_id = input.inputs[2].GetValue<int64_t>();

	names = {"consumer_name", "committed_snapshot", "schema_version"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	return std::move(result);
}

void ThrowSchemaTerminated(const std::string &consumer_name, int64_t cursor, int64_t boundary_snapshot,
                           int64_t requested_snapshot) {
	throw duckdb::InvalidInputException(
	    "CDC_SCHEMA_TERMINATED: consumer '%s' is pinned to the schema shape at snapshot %lld; snapshot %lld carries a "
	    "shape change for a subscribed table. Cursor cannot advance past %lld. Refused cdc_commit to %lld. Create a "
	    "new DML consumer with start_at >= %lld to consume the post-change shape; orchestrate this from a DDL "
	    "consumer (see docs/api.md).",
	    consumer_name, static_cast<long long>(cursor), static_cast<long long>(boundary_snapshot),
	    static_cast<long long>(boundary_snapshot - 1), static_cast<long long>(requested_snapshot),
	    static_cast<long long>(boundary_snapshot));
}

void ValidateCommitSnapshot(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                            int64_t current_cursor, int64_t snapshot_id) {
	if (snapshot_id < current_cursor) {
		throw duckdb::InvalidInputException("cdc_commit snapshot_id %lld is behind consumer '%s' cursor %lld",
		                                    static_cast<long long>(snapshot_id), consumer_name,
		                                    static_cast<long long>(current_cursor));
	}
	const auto snapshot_table = MetadataTable(conn, catalog_name, "ducklake_snapshot");
	auto result =
	    conn.Query("SELECT count(*) FROM " + snapshot_table + " WHERE snapshot_id = " + std::to_string(snapshot_id));
	if (SingleInt64(*result, "commit snapshot existence") == 0) {
		throw duckdb::InvalidInputException("cdc_commit snapshot_id %lld does not exist in catalog '%s'",
		                                    static_cast<long long>(snapshot_id), catalog_name);
	}
	const auto current_snapshot = CurrentSnapshot(conn, catalog_name);
	if (snapshot_id > current_snapshot) {
		throw duckdb::InvalidInputException(
		    "cdc_commit snapshot_id %lld is newer than current snapshot %lld for catalog '%s'",
		    static_cast<long long>(snapshot_id), static_cast<long long>(current_snapshot), catalog_name);
	}
}

//! Refuse cdc_commit advances that would land at or past the consumer's
//! pinned schema-shape boundary. Idempotent commits to the current cursor are
//! always allowed. The check only needs to inspect the snapshots being
//! committed: boundaries after `requested_snapshot` must not block this commit
//! and scanning to catalog head makes every commit pay for unrelated future
//! work.
void ValidateDmlConsumerShapeBoundary(duckdb::Connection &conn, const std::string &catalog_name, const ConsumerRow &row,
                                      int64_t requested_snapshot) {
	if (row.consumer_kind != "dml") {
		return;
	}
	if (requested_snapshot <= row.last_committed_snapshot) {
		return;
	}
	const auto subscriptions = LoadDmlConsumerSubscriptions(conn, catalog_name, row.consumer_name);
	const auto boundary = NextDmlSubscribedSchemaChangeSnapshot(conn, catalog_name, row.last_committed_snapshot,
	                                                            requested_snapshot, subscriptions);
	if (boundary == -1) {
		return;
	}
	ThrowSchemaTerminated(row.consumer_name, row.last_committed_snapshot, boundary, requested_snapshot);
}

//! Refuse cdc_consumer_reset targets that would land in a different
//! schema-shape epoch from the consumer's current cursor. The consumer is
//! pinned; cross-shape repositioning requires a new consumer.
void ValidateDmlConsumerResetTarget(duckdb::Connection &conn, const std::string &catalog_name, const ConsumerRow &row,
                                    int64_t target_snapshot) {
	if (row.consumer_kind != "dml") {
		return;
	}
	const auto cursor = row.last_committed_snapshot;
	if (target_snapshot == cursor) {
		return;
	}
	const auto subscriptions = LoadDmlConsumerSubscriptions(conn, catalog_name, row.consumer_name);
	if (subscriptions.empty()) {
		return;
	}
	const auto lo = std::min(cursor, target_snapshot);
	const auto hi = std::max(cursor, target_snapshot);
	const auto boundary = NextDmlSubscribedSchemaChangeSnapshot(conn, catalog_name, lo, hi, subscriptions);
	if (boundary == -1) {
		return;
	}
	throw duckdb::InvalidInputException(
	    "CDC_SCHEMA_TERMINATED: cdc_consumer_reset for DML consumer '%s' would cross a schema-shape boundary at "
	    "snapshot %lld (between cursor %lld and target %lld). DML consumers are pinned to a single schema shape; "
	    "create a new consumer with start_at >= %lld (or <= %lld) to consume that shape epoch.",
	    row.consumer_name, static_cast<long long>(boundary), static_cast<long long>(cursor),
	    static_cast<long long>(target_snapshot), static_cast<long long>(boundary),
	    static_cast<long long>(boundary - 1));
}

duckdb::unique_ptr<duckdb::MaterializedQueryResult>
VerifyCommittedConsumer(duckdb::Connection &conn, const std::string &consumers, const std::string &consumer_name,
                        const std::string &cached_token, int64_t snapshot_id) {
	auto result = conn.Query("SELECT consumer_name, last_committed_snapshot, last_committed_schema_version FROM " +
	                         consumers + " WHERE consumer_name = " + QuoteLiteral(consumer_name) +
	                         " AND owner_token = " + TokenSqlOrNull(cached_token) +
	                         " AND last_committed_snapshot = " + std::to_string(snapshot_id) + " LIMIT 1");
	ThrowIfQueryFailed(result);
	return result;
}

bool TryCommitPostgresNative(duckdb::Connection &conn, const std::string &catalog_name,
                             const std::string &consumer_name, const std::string &cached_token, int64_t snapshot_id,
                             std::vector<duckdb::Value> &out) {
	if (cached_token.empty()) {
		return false;
	}
	if (!StateSchemaExists(conn, catalog_name)) {
		return false;
	}
	const auto state = ResolveCdcCatalogState(conn, catalog_name);
	const auto consumers = NativePostgresStateTable(state.state_schema, CONSUMERS_TABLE);
	const auto attached_consumers = StateTable(catalog_name, CONSUMERS_TABLE, state.state_schema);
	const auto snapshot_table = NativePostgresMetadataTable(state.metadata_schema, "ducklake_snapshot");
	const auto snapshot_sql = std::to_string(snapshot_id);
	const auto update_sql =
	    "UPDATE " + consumers + " SET last_committed_snapshot = " + snapshot_sql +
	    ", last_committed_schema_version = (SELECT schema_version FROM " + snapshot_table +
	    " WHERE snapshot_id = " + snapshot_sql + " LIMIT 1), owner_heartbeat_at = now(), updated_at = now() " +
	    "WHERE consumer_name = " + QuoteLiteral(consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token) +
	    " AND last_committed_snapshot <= " + snapshot_sql + " AND EXISTS (SELECT 1 FROM " + snapshot_table +
	    " WHERE snapshot_id = " + snapshot_sql + ")";
	PostgresExecuteChecked(conn, catalog_name, update_sql);

	auto verified = VerifyCommittedConsumer(conn, attached_consumers, consumer_name, cached_token, snapshot_id);
	if (!verified || verified->RowCount() == 0) {
		return false;
	}
	out = {verified->GetValue(0, 0), verified->GetValue(1, 0), verified->GetValue(2, 0)};
	return true;
}

std::vector<duckdb::Value> CommitWindowWithConnection(duckdb::ClientContext &context, duckdb::Connection &conn,
                                                      const CdcCommitData &data) {
	CheckCatalogOrThrow(conn, data.catalog_name);

	const auto backend = CachedStateBackend(context, conn, data.catalog_name);
	const auto cached_token = CachedToken(context, conn, data.catalog_name, data.consumer_name);
	// Look up the consumer once before any UPDATE: the per-subscribed-table
	// schema-shape boundary check must happen on every code path (fast,
	// postgres-native, legacy) and must precede any state mutation.
	auto initial_row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (!HasCachedDmlSafeCommitRange(context, conn, data.catalog_name, data.consumer_name,
	                                 initial_row.last_committed_snapshot, data.snapshot_id)) {
		ValidateDmlConsumerShapeBoundary(conn, data.catalog_name, initial_row, data.snapshot_id);
	}
	if (SupportsUpdateReturning(backend)) {
		const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
		const auto snapshot_table = MetadataTable(conn, data.catalog_name, "ducklake_snapshot");
		auto fast_commit = conn.Query(
		    "UPDATE " + consumers + " SET last_committed_snapshot = " + std::to_string(data.snapshot_id) +
		    ", last_committed_schema_version = (SELECT schema_version FROM " + snapshot_table +
		    " WHERE snapshot_id = " + std::to_string(data.snapshot_id) +
		    " LIMIT 1), owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
		    QuoteLiteral(data.consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token) +
		    " AND last_committed_snapshot <= " + std::to_string(data.snapshot_id) + " AND EXISTS (SELECT 1 FROM " +
		    snapshot_table + " WHERE snapshot_id = " + std::to_string(data.snapshot_id) +
		    ") RETURNING consumer_name, last_committed_snapshot, last_committed_schema_version");
		ThrowIfQueryFailed(fast_commit);
		if (fast_commit && fast_commit->RowCount() > 0) {
			return {fast_commit->GetValue(0, 0), fast_commit->GetValue(1, 0), fast_commit->GetValue(2, 0)};
		}
	}
	if (backend == StateBackendKind::Postgres) {
		std::vector<duckdb::Value> committed;
		if (TryCommitPostgresNative(conn, data.catalog_name, data.consumer_name, cached_token, data.snapshot_id,
		                            committed)) {
			return committed;
		}
	}

	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (cached_token.empty() || row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}
	ValidateCommitSnapshot(conn, data.catalog_name, data.consumer_name, row.last_committed_snapshot, data.snapshot_id);
	const auto schema_version = ResolveSchemaVersion(conn, data.catalog_name, data.snapshot_id);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	ExecuteChecked(conn, "UPDATE " + consumers + " SET last_committed_snapshot = " + std::to_string(data.snapshot_id) +
	                         ", last_committed_schema_version = " + std::to_string(schema_version) +
	                         ", owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(data.consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token));
	row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (row.last_committed_snapshot != data.snapshot_id || row.owner_token.IsNull() ||
	    row.owner_token.ToString() != cached_token) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}
	return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(data.snapshot_id),
	        duckdb::Value::BIGINT(schema_version)};
}

std::vector<duckdb::Value> CommitWindow(duckdb::ClientContext &context, const CdcCommitData &data) {
	duckdb::Connection conn(*context.db);
	return CommitWindowWithConnection(context, conn, data);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcCommitInit(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcCommitData>();
	result->rows.push_back(CommitWindow(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_heartbeat
//===--------------------------------------------------------------------===//

struct ConsumerHeartbeatData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerHeartbeatData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerHeartbeatBind(duckdb::ClientContext &context,
                                                               duckdb::TableFunctionBindInput &input,
                                                               duckdb::vector<duckdb::LogicalType> &return_types,
                                                               duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_heartbeat requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerHeartbeatData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");

	names = {"heartbeat_extended"};
	return_types = {duckdb::LogicalType::BOOLEAN};
	return std::move(result);
}

std::vector<duckdb::Value> HeartbeatConsumer(duckdb::ClientContext &context, const ConsumerHeartbeatData &data) {
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, conn, data.catalog_name, data.consumer_name);
	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (cached_token.empty() || row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}
	ExecuteChecked(conn, "UPDATE " + consumers +
	                         " SET owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(data.consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token));
	row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}
	return {duckdb::Value::BOOLEAN(true)};
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerHeartbeatInit(duckdb::ClientContext &context,
                                                                           duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerHeartbeatData>();
	result->rows.push_back(HeartbeatConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// listen helper
//===--------------------------------------------------------------------===//

struct ListenWaitData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t timeout_ms;
	int64_t poll_min_ms = WAIT_INITIAL_INTERVAL_MS;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ListenWaitData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->timeout_ms = timeout_ms;
		result->poll_min_ms = poll_min_ms;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

int64_t TimeoutMsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("timeout_ms");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return DEFAULT_WAIT_TIMEOUT_MS;
	}
	return entry->second.GetValue<int64_t>();
}

std::vector<std::string> SplitListenChangeTokens(const std::string &changes_made) {
	std::vector<std::string> tokens;
	size_t start = 0;
	while (start < changes_made.size()) {
		const auto comma = changes_made.find(',', start);
		tokens.push_back(changes_made.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
		if (comma == std::string::npos) {
			break;
		}
		start = comma + 1;
	}
	return tokens;
}

bool ParseListenTableIdToken(const std::string &token, const std::string &prefix, int64_t &table_id) {
	if (token.rfind(prefix, 0) != 0) {
		return false;
	}
	return TryParseInt64(token.substr(prefix.size()), table_id);
}

std::string UnquoteListenName(const std::string &quoted) {
	if (quoted.size() >= 2 && quoted.front() == '"' && quoted.back() == '"') {
		return quoted.substr(1, quoted.size() - 2);
	}
	return quoted;
}

bool ParseListenQualifiedNameToken(const std::string &token, const std::string &prefix, std::string &schema_name,
                                   std::string &object_name) {
	if (token.rfind(prefix, 0) != 0) {
		return false;
	}
	const auto value = token.substr(prefix.size());
	const auto dot = value.find("\".\"");
	if (dot == std::string::npos) {
		return false;
	}
	schema_name = UnquoteListenName(value.substr(0, dot + 1));
	object_name = UnquoteListenName(value.substr(dot + 2));
	return true;
}

bool ParseListenNameToken(const std::string &token, const std::string &prefix, std::string &name) {
	if (token.rfind(prefix, 0) != 0) {
		return false;
	}
	name = UnquoteListenName(token.substr(prefix.size()));
	return !name.empty();
}

DecodedSnapshotChange DecodeSnapshotChange(int64_t snapshot_id, const std::string &changes_made) {
	DecodedSnapshotChange decoded;
	decoded.snapshot_id = snapshot_id;
	for (const auto &token : SplitListenChangeTokens(changes_made)) {
		int64_t table_id = 0;
		for (const auto &prefix : {"inserted_into_table:", "deleted_from_table:", "tables_inserted_into:",
		                           "tables_deleted_from:", "inlined_insert:", "inlined_delete:"}) {
			if (ParseListenTableIdToken(token, prefix, table_id)) {
				decoded.dml_table_ids.insert(table_id);
				break;
			}
		}
		if (ParseListenTableIdToken(token, "altered_table:", table_id)) {
			decoded.altered_table_ids.insert(table_id);
			continue;
		}
		if (ParseListenTableIdToken(token, "dropped_table:", table_id)) {
			decoded.dropped_table_ids.insert(table_id);
			continue;
		}
		int64_t schema_id = 0;
		if (ParseListenTableIdToken(token, "dropped_schema:", schema_id)) {
			decoded.dropped_schema_ids.insert(schema_id);
		}
	}
	return decoded;
}

void PruneSnapshotChangeIndexLocked(SnapshotChangeIndex &index) {
	while (index.changes.size() > SNAPSHOT_CHANGE_INDEX_MAX_SNAPSHOTS_PER_CATALOG) {
		auto victim = index.changes.end();
		for (auto entry = index.changes.begin(); entry != index.changes.end(); ++entry) {
			if (victim == index.changes.end() || entry->first < victim->first) {
				victim = entry;
			}
		}
		if (victim == index.changes.end()) {
			break;
		}
		index.changes.erase(victim);
	}
	if (index.changes.empty()) {
		index.loaded_from = -1;
		index.loaded_to = -1;
		return;
	}
	int64_t min_snapshot = std::numeric_limits<int64_t>::max();
	int64_t max_snapshot = -1;
	for (const auto &entry : index.changes) {
		min_snapshot = std::min(min_snapshot, entry.first);
		max_snapshot = std::max(max_snapshot, entry.first);
	}
	index.loaded_from = min_snapshot;
	index.loaded_to = max_snapshot;
}

void PruneSnapshotChangeCatalogsLocked() {
	while (SNAPSHOT_CHANGE_INDEXES.size() > SNAPSHOT_CHANGE_INDEX_MAX_CATALOGS) {
		auto victim = SNAPSHOT_CHANGE_INDEXES.end();
		for (auto entry = SNAPSHOT_CHANGE_INDEXES.begin(); entry != SNAPSHOT_CHANGE_INDEXES.end(); ++entry) {
			if (victim == SNAPSHOT_CHANGE_INDEXES.end() || entry->second.last_used < victim->second.last_used) {
				victim = entry;
			}
		}
		if (victim == SNAPSHOT_CHANGE_INDEXES.end()) {
			break;
		}
		SNAPSHOT_CHANGE_INDEXES.erase(victim);
	}
}

std::string SnapshotChangeIndexCacheKey(duckdb::Connection &conn, const std::string &catalog_name) {
	const auto attachment_key = MetadataAttachmentCacheKey(conn, catalog_name);
	return attachment_key.empty() ? catalog_name : attachment_key;
}

void MergeDecodedSnapshotChanges(const std::string &cache_key, int64_t from_snapshot, int64_t to_snapshot,
                                 std::vector<DecodedSnapshotChange> decoded) {
	std::lock_guard<std::mutex> guard(SNAPSHOT_CHANGE_INDEX_MUTEX);
	auto &index = SNAPSHOT_CHANGE_INDEXES[cache_key];
	if (index.loaded_to != -1 && to_snapshot < index.loaded_to) {
		index.changes.clear();
		index.loaded_from = -1;
		index.loaded_to = -1;
	}
	for (auto &change : decoded) {
		index.changes[change.snapshot_id] = std::move(change);
	}
	if (index.loaded_from == -1 || from_snapshot + 1 < index.loaded_from) {
		index.loaded_from = from_snapshot + 1;
	}
	index.loaded_to = std::max(index.loaded_to, to_snapshot);
	index.last_used = ++SNAPSHOT_CHANGE_INDEX_CLOCK;
	PruneSnapshotChangeIndexLocked(index);
	PruneSnapshotChangeCatalogsLocked();
}

std::vector<DecodedSnapshotChange> LoadDecodedSnapshotChanges(duckdb::Connection &conn, const std::string &catalog_name,
                                                              int64_t from_snapshot, int64_t to_snapshot) {
	std::vector<DecodedSnapshotChange> cached;
	if (to_snapshot <= from_snapshot) {
		return cached;
	}
	const auto cache_key = SnapshotChangeIndexCacheKey(conn, catalog_name);
	bool cache_hit = false;
	{
		std::lock_guard<std::mutex> guard(SNAPSHOT_CHANGE_INDEX_MUTEX);
		auto entry = SNAPSHOT_CHANGE_INDEXES.find(cache_key);
		if (entry != SNAPSHOT_CHANGE_INDEXES.end() && entry->second.loaded_from <= from_snapshot + 1 &&
		    entry->second.loaded_to >= to_snapshot) {
			cache_hit = true;
			entry->second.last_used = ++SNAPSHOT_CHANGE_INDEX_CLOCK;
			for (const auto &change : entry->second.changes) {
				if (change.first > from_snapshot && change.first <= to_snapshot) {
					cached.push_back(change.second);
				}
			}
		}
	}
	if (cache_hit) {
		std::sort(cached.begin(), cached.end(),
		          [](const DecodedSnapshotChange &left, const DecodedSnapshotChange &right) {
			          return left.snapshot_id < right.snapshot_id;
		          });
		return cached;
	}

	auto rows = conn.Query("SELECT snapshot_id, changes_made FROM " +
	                       MetadataTable(conn, catalog_name, "ducklake_snapshot_changes") + " WHERE snapshot_id > " +
	                       std::to_string(from_snapshot) + " AND snapshot_id <= " + std::to_string(to_snapshot) +
	                       " ORDER BY snapshot_id ASC");
	ThrowIfQueryFailed(rows);
	if (!rows) {
		return {};
	}
	std::vector<DecodedSnapshotChange> decoded;
	decoded.reserve(rows->RowCount());
	for (duckdb::idx_t row_idx = 0; row_idx < rows->RowCount(); ++row_idx) {
		if (rows->GetValue(0, row_idx).IsNull()) {
			continue;
		}
		const auto changes_value = rows->GetValue(1, row_idx);
		decoded.push_back(DecodeSnapshotChange(rows->GetValue(0, row_idx).GetValue<int64_t>(),
		                                       changes_value.IsNull() ? std::string() : changes_value.ToString()));
	}
	auto result = decoded;
	MergeDecodedSnapshotChanges(cache_key, from_snapshot, to_snapshot, std::move(decoded));
	return result;
}

std::string TableResolutionCacheKey(const std::string &cache_namespace, const std::string &catalog_name,
                                    int64_t table_id, int64_t snapshot_id) {
	return cache_namespace + ":" + catalog_name + ":" + std::to_string(table_id) + ":" + std::to_string(snapshot_id);
}

std::string DmlTableChangeMatchCacheKey(const std::string &cache_namespace, const std::string &catalog_name,
                                        int64_t snapshot_id, int64_t schema_id, int64_t table_id,
                                        const std::string &qualified_name,
                                        const std::vector<std::string> &change_types) {
	std::ostringstream key;
	key << cache_namespace << ":" << catalog_name << ":" << snapshot_id << ":" << schema_id << ":" << table_id << ":"
	    << qualified_name;
	for (const auto &change_type : change_types) {
		key << ":" << change_type;
	}
	return key.str();
}

void PruneDmlPreflightCachesIfNeeded() {
	if (TABLE_RESOLUTION_CACHE.size() + DML_TABLE_CHANGE_MATCH_CACHE.size() <= DML_PREFLIGHT_CACHE_MAX_ENTRIES) {
		return;
	}
	TABLE_RESOLUTION_CACHE.clear();
	DML_TABLE_CHANGE_MATCH_CACHE.clear();
}

bool ResolveTableIdNearSnapshot(duckdb::Connection &conn, const std::string &cache_namespace,
                                const std::string &catalog_name, int64_t table_id, int64_t snapshot_id,
                                int64_t &schema_id, std::string &qualified_name) {
	const auto cache_key = TableResolutionCacheKey(cache_namespace, catalog_name, table_id, snapshot_id);
	{
		std::lock_guard<std::mutex> guard(DML_PREFLIGHT_CACHE_MUTEX);
		const auto entry = TABLE_RESOLUTION_CACHE.find(cache_key);
		if (entry != TABLE_RESOLUTION_CACHE.end()) {
			if (!entry->second.found) {
				return false;
			}
			schema_id = entry->second.schema_id;
			qualified_name = entry->second.qualified_name;
			return true;
		}
	}

	if (ResolveTableByIdAt(conn, catalog_name, table_id, snapshot_id, schema_id, qualified_name)) {
		TableResolutionCacheEntry cache_entry;
		cache_entry.found = true;
		cache_entry.schema_id = schema_id;
		cache_entry.qualified_name = qualified_name;
		std::lock_guard<std::mutex> guard(DML_PREFLIGHT_CACHE_MUTEX);
		TABLE_RESOLUTION_CACHE[cache_key] = std::move(cache_entry);
		PruneDmlPreflightCachesIfNeeded();
		return true;
	}
	if (snapshot_id > 0) {
		const auto found = ResolveTableByIdAt(conn, catalog_name, table_id, snapshot_id - 1, schema_id, qualified_name);
		if (found) {
			TableResolutionCacheEntry cache_entry;
			cache_entry.found = true;
			cache_entry.schema_id = schema_id;
			cache_entry.qualified_name = qualified_name;
			std::lock_guard<std::mutex> guard(DML_PREFLIGHT_CACHE_MUTEX);
			TABLE_RESOLUTION_CACHE[cache_key] = std::move(cache_entry);
			PruneDmlPreflightCachesIfNeeded();
		}
		return found;
	}
	return false;
}

bool ResolveSchemaIdNearSnapshot(duckdb::Connection &conn, const std::string &catalog_name,
                                 const std::string &schema_name, int64_t snapshot_id, int64_t &schema_id) {
	if (ResolveSchemaByNameAt(conn, catalog_name, schema_name, snapshot_id, schema_id)) {
		return true;
	}
	if (snapshot_id > 0) {
		return ResolveSchemaByNameAt(conn, catalog_name, schema_name, snapshot_id - 1, schema_id);
	}
	return false;
}

bool AnyDdlSubscriptionMatches(const std::vector<ConsumerSubscriptionRow> &subscriptions, int64_t schema_id,
                               duckdb::Value table_id) {
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category != "ddl") {
			continue;
		}
		if (subscription.scope_kind == "catalog") {
			return true;
		}
		if (subscription.scope_kind == "schema" && !subscription.schema_id.IsNull() &&
		    subscription.schema_id.GetValue<int64_t>() == schema_id) {
			return true;
		}
		if (subscription.scope_kind == "table" && !table_id.IsNull() && !subscription.table_id.IsNull() &&
		    subscription.table_id.GetValue<int64_t>() == table_id.GetValue<int64_t>()) {
			return true;
		}
	}
	return false;
}

bool OnlyDmlTableSubscriptions(const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	if (subscriptions.empty()) {
		return false;
	}
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category != "dml" || subscription.scope_kind != "table" ||
		    subscription.table_id.IsNull()) {
			return false;
		}
	}
	return true;
}

bool DmlSubscriptionsIncludeTable(const std::vector<ConsumerSubscriptionRow> &subscriptions, int64_t table_id) {
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category == "dml" && subscription.scope_kind == "table" &&
		    !subscription.table_id.IsNull() && subscription.table_id.GetValue<int64_t>() == table_id) {
			return true;
		}
	}
	return false;
}

bool HasCatalogDmlSubscription(const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category == "dml" && subscription.scope_kind == "catalog") {
			return true;
		}
	}
	return false;
}

void DmlSubscriptionIds(const std::vector<ConsumerSubscriptionRow> &subscriptions,
                        std::unordered_set<int64_t> &table_ids, std::unordered_set<int64_t> &schema_ids) {
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category != "dml" || subscription.scope_kind != "table") {
			continue;
		}
		if (subscription.table_id.IsNull() || subscription.status == "dropped") {
			continue;
		}
		table_ids.insert(subscription.table_id.GetValue<int64_t>());
		if (!subscription.schema_id.IsNull()) {
			schema_ids.insert(subscription.schema_id.GetValue<int64_t>());
		}
	}
}

bool SnapshotChangeTouchesDmlTables(const DecodedSnapshotChange &change, const std::unordered_set<int64_t> &table_ids) {
	for (const auto table_id : change.dml_table_ids) {
		if (table_ids.count(table_id) > 0) {
			return true;
		}
	}
	return false;
}

int64_t FirstDmlSubscribedSnapshot(duckdb::Connection &conn, const std::string &catalog_name, int64_t start_snapshot,
                                   int64_t current_snapshot,
                                   const std::vector<ConsumerSubscriptionRow> &dml_subscriptions) {
	const auto catalog_wide = HasCatalogDmlSubscription(dml_subscriptions);
	std::unordered_set<int64_t> table_ids;
	std::unordered_set<int64_t> schema_ids;
	DmlSubscriptionIds(dml_subscriptions, table_ids, schema_ids);
	if (!catalog_wide && table_ids.empty()) {
		return -1;
	}
	for (const auto &change : LoadDecodedSnapshotChanges(conn, catalog_name, start_snapshot, current_snapshot)) {
		if ((catalog_wide && !change.dml_table_ids.empty()) || SnapshotChangeTouchesDmlTables(change, table_ids)) {
			return change.snapshot_id;
		}
	}
	return -1;
}

std::string ExactTokenPredicate(const std::string &column_name, const std::string &token) {
	const auto quoted_token = QuoteLiteral(token);
	return "(" + column_name + " = " + quoted_token + " OR " + column_name + " LIKE " + QuoteLiteral(token + ",%") +
	       " OR " + column_name + " LIKE " + QuoteLiteral("%," + token) + " OR " + column_name + " LIKE " +
	       QuoteLiteral("%," + token + ",%") + ")";
}

std::string DmlTableSnapshotChangesFilter(const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	if (!OnlyDmlTableSubscriptions(subscriptions)) {
		return std::string();
	}
	std::unordered_set<int64_t> seen_table_ids;
	std::vector<int64_t> table_ids;
	for (const auto &subscription : subscriptions) {
		const auto table_id = subscription.table_id.GetValue<int64_t>();
		if (seen_table_ids.insert(table_id).second) {
			table_ids.push_back(table_id);
		}
	}
	if (table_ids.empty()) {
		return std::string();
	}
	std::ostringstream filter;
	filter << " AND (";
	bool needs_or = false;
	for (const auto table_id : table_ids) {
		for (const auto &prefix : {"inserted_into_table:", "deleted_from_table:", "tables_inserted_into:",
		                           "tables_deleted_from:", "inlined_insert:", "inlined_delete:"}) {
			if (needs_or) {
				filter << " OR ";
			}
			filter << ExactTokenPredicate("changes_made", std::string(prefix) + std::to_string(table_id));
			needs_or = true;
		}
	}
	filter << ")";
	return filter.str();
}

bool DmlTableChangesMatchSubscriptions(duckdb::Connection &conn, const std::string &cache_namespace,
                                       const std::string &catalog_name, int64_t snapshot_id, int64_t schema_id,
                                       int64_t table_id, const std::string &qualified_name,
                                       const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	const auto change_types = MatchingDmlChangeTypes(subscriptions, schema_id, table_id);
	if (change_types.empty()) {
		return false;
	}
	if (std::find(change_types.begin(), change_types.end(), "*") != change_types.end()) {
		return true;
	}
	const auto dot = qualified_name.find('.');
	if (dot == std::string::npos) {
		return false;
	}
	const auto schema_name = qualified_name.substr(0, dot);
	const auto table_name = qualified_name.substr(dot + 1);
	std::ostringstream filter;
	filter << " WHERE change_type IN (";
	for (size_t i = 0; i < change_types.size(); ++i) {
		if (i > 0) {
			filter << ", ";
		}
		filter << QuoteLiteral(change_types[i]);
	}
	filter << ")";
	const auto cache_key = DmlTableChangeMatchCacheKey(cache_namespace, catalog_name, snapshot_id, schema_id, table_id,
	                                                   qualified_name, change_types);
	{
		std::lock_guard<std::mutex> guard(DML_PREFLIGHT_CACHE_MUTEX);
		const auto entry = DML_TABLE_CHANGE_MATCH_CACHE.find(cache_key);
		if (entry != DML_TABLE_CHANGE_MATCH_CACHE.end()) {
			return entry->second;
		}
	}
	auto rows = conn.Query("SELECT count(*) FROM ducklake_table_changes(" + QuoteLiteral(catalog_name) + ", " +
	                       QuoteLiteral(schema_name) + ", " + QuoteLiteral(table_name) + ", " +
	                       std::to_string(snapshot_id) + ", " + std::to_string(snapshot_id) + ")" + filter.str());
	if (!rows || rows->HasError() || rows->RowCount() == 0 || rows->GetValue(0, 0).IsNull()) {
		return false;
	}
	const auto matches = rows->GetValue(0, 0).GetValue<int64_t>() > 0;
	std::lock_guard<std::mutex> guard(DML_PREFLIGHT_CACHE_MUTEX);
	DML_TABLE_CHANGE_MATCH_CACHE[cache_key] = matches;
	PruneDmlPreflightCachesIfNeeded();
	return matches;
}

bool SnapshotTouchesSubscriptions(duckdb::Connection &conn, const std::string &cache_namespace,
                                  const std::string &catalog_name, int64_t snapshot_id, const std::string &changes_made,
                                  const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	if (subscriptions.empty()) {
		return true;
	}
	const auto only_dml_table_subscriptions = OnlyDmlTableSubscriptions(subscriptions);
	for (const auto &token : SplitListenChangeTokens(changes_made)) {
		int64_t table_id = 0;
		bool dml_table_token = false;
		for (const auto &prefix : {"inserted_into_table:", "deleted_from_table:", "tables_inserted_into:",
		                           "tables_deleted_from:", "inlined_insert:", "inlined_delete:"}) {
			if (ParseListenTableIdToken(token, prefix, table_id)) {
				dml_table_token = true;
				break;
			}
		}
		if (dml_table_token) {
			if (only_dml_table_subscriptions && !DmlSubscriptionsIncludeTable(subscriptions, table_id)) {
				continue;
			}
			int64_t schema_id = 0;
			std::string qualified_name;
			if (ResolveTableIdNearSnapshot(conn, cache_namespace, catalog_name, table_id, snapshot_id, schema_id,
			                               qualified_name) &&
			    DmlTableChangesMatchSubscriptions(conn, cache_namespace, catalog_name, snapshot_id, schema_id, table_id,
			                                      qualified_name, subscriptions)) {
				return true;
			}
			continue;
		}

		std::string schema_name;
		std::string object_name;
		if (ParseListenQualifiedNameToken(token, "created_table:", schema_name, object_name)) {
			int64_t schema_id = 0;
			int64_t created_table_id = 0;
			const auto qualified_name = schema_name + "." + object_name;
			const auto table_value =
			    ResolveCurrentTableName(conn, catalog_name, qualified_name, snapshot_id, schema_id, created_table_id)
			        ? duckdb::Value::BIGINT(created_table_id)
			        : duckdb::Value();
			if ((table_value.IsNull() &&
			     ResolveSchemaIdNearSnapshot(conn, catalog_name, schema_name, snapshot_id, schema_id) &&
			     AnyDdlSubscriptionMatches(subscriptions, schema_id, table_value)) ||
			    (!table_value.IsNull() && AnyDdlSubscriptionMatches(subscriptions, schema_id, table_value))) {
				return true;
			}
			continue;
		}
		for (const auto &prefix : {"altered_table:", "dropped_table:"}) {
			if (ParseListenTableIdToken(token, prefix, table_id)) {
				int64_t schema_id = 0;
				std::string qualified_name;
				if (ResolveTableIdNearSnapshot(conn, cache_namespace, catalog_name, table_id, snapshot_id, schema_id,
				                               qualified_name) &&
				    AnyDdlSubscriptionMatches(subscriptions, schema_id, duckdb::Value::BIGINT(table_id))) {
					return true;
				}
			}
		}
		for (const auto &prefix : {"created_schema:", "dropped_schema:"}) {
			if (ParseListenNameToken(token, prefix, schema_name)) {
				int64_t schema_id = 0;
				if (ResolveSchemaIdNearSnapshot(conn, catalog_name, schema_name, snapshot_id, schema_id) &&
				    AnyDdlSubscriptionMatches(subscriptions, schema_id, duckdb::Value())) {
					return true;
				}
			}
		}
	}
	return false;
}

struct SnapshotMatchProbe {
	duckdb::Value matching_snapshot;
	int64_t scanned_through;

	SnapshotMatchProbe(duckdb::Value matching_snapshot_p, int64_t scanned_through_p)
	    : matching_snapshot(std::move(matching_snapshot_p)), scanned_through(scanned_through_p) {
	}
};

SnapshotMatchProbe NextMatchingSnapshot(duckdb::Connection &conn, const std::string &cache_namespace,
                                        const std::string &catalog_name, const ConsumerRow &consumer,
                                        const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	if (OnlyDmlTableSubscriptions(subscriptions)) {
		const auto current_snapshot = CurrentSnapshot(conn, catalog_name);
		const auto snapshot = FirstDmlSubscribedSnapshot(conn, catalog_name, consumer.last_committed_snapshot,
		                                                 current_snapshot, subscriptions);
		if (snapshot != -1) {
			return {duckdb::Value::BIGINT(snapshot), current_snapshot};
		}
		return {duckdb::Value(), current_snapshot};
	}
	const auto current_snapshot = CurrentSnapshot(conn, catalog_name);
	const auto metadata_schema = ResolveCdcCatalogState(conn, catalog_name).metadata_schema;
	const auto range_sql = "SELECT snapshot_id, changes_made FROM " +
	                       NativePostgresMetadataTable(metadata_schema, "ducklake_snapshot_changes") +
	                       " WHERE snapshot_id > " + std::to_string(consumer.last_committed_snapshot) +
	                       " AND snapshot_id <= " + std::to_string(current_snapshot) +
	                       DmlTableSnapshotChangesFilter(subscriptions) + " ORDER BY snapshot_id ASC";
	auto rows = MetadataBackendIsPostgres(conn, catalog_name)
	                ? QueryPostgresMetadata(conn, catalog_name, range_sql)
	                : conn.Query("SELECT snapshot_id, changes_made FROM " +
	                             MetadataTable(conn, catalog_name, "ducklake_snapshot_changes") +
	                             " WHERE snapshot_id > " + std::to_string(consumer.last_committed_snapshot) +
	                             " AND snapshot_id <= " + std::to_string(current_snapshot) +
	                             DmlTableSnapshotChangesFilter(subscriptions) + " ORDER BY snapshot_id ASC");
	ThrowIfQueryFailed(rows);
	if (!rows) {
		return {duckdb::Value(), current_snapshot};
	}
	for (duckdb::idx_t row_idx = 0; row_idx < rows->RowCount(); ++row_idx) {
		if (rows->GetValue(0, row_idx).IsNull()) {
			continue;
		}
		const auto snapshot_id = rows->GetValue(0, row_idx).GetValue<int64_t>();
		const auto changes_value = rows->GetValue(1, row_idx);
		const auto changes_made = changes_value.IsNull() ? std::string() : changes_value.ToString();
		if (SnapshotTouchesSubscriptions(conn, cache_namespace, catalog_name, snapshot_id, changes_made,
		                                 subscriptions)) {
			return {duckdb::Value::BIGINT(snapshot_id), current_snapshot};
		}
	}
	return {duckdb::Value(), current_snapshot};
}

bool AdvanceDdlCursorPastInspectedSnapshots(duckdb::ClientContext &context, duckdb::Connection &conn,
                                            const std::string &catalog_name, const ConsumerRow &probed_consumer,
                                            int64_t inspected_through) {
	if (probed_consumer.consumer_kind != "ddl" || inspected_through <= probed_consumer.last_committed_snapshot) {
		return false;
	}

	const auto cached_token = CachedToken(context, conn, catalog_name, probed_consumer.consumer_name);
	ConsumerRow owned_consumer;
	if (!TryUseFreshCachedLease(conn, catalog_name, probed_consumer.consumer_name, cached_token, owned_consumer)) {
		const auto backend = CachedStateBackend(context, conn, catalog_name);
		owned_consumer = AcquireLease(conn, catalog_name, probed_consumer.consumer_name, cached_token, backend);
	}
	CacheToken(context, conn, catalog_name, probed_consumer.consumer_name, owned_consumer.owner_token.ToString());

	// The subscription scan was performed from probed_consumer's cursor.
	// A concurrent reset/commit changes that premise, so retry from the new
	// cursor instead of advancing a range we did not inspect.
	if (owned_consumer.last_committed_snapshot != probed_consumer.last_committed_snapshot) {
		return false;
	}
	CommitConsumerSnapshotWithConnection(context, conn, catalog_name, probed_consumer.consumer_name, inspected_through);
	return true;
}

duckdb::unique_ptr<duckdb::FunctionData> ListenWaitBind(duckdb::ClientContext &context,
                                                        duckdb::TableFunctionBindInput &input,
                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                        duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("listen helper requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ListenWaitData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->timeout_ms = TimeoutMsParameter(input);
	result->poll_min_ms = PollMinMsParameter(input);

	names = {"snapshot_id", "snapshot_span"};
	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	return std::move(result);
}

std::vector<duckdb::Value>
WaitForNextSnapshotWithSubscriptions(duckdb::ClientContext &context, duckdb::Connection &conn,
                                     const std::string &catalog_name, const std::string &consumer_name,
                                     int64_t timeout_ms, const std::vector<ConsumerSubscriptionRow> &subscriptions,
                                     int64_t poll_min_ms) {
	CheckCatalogOrThrow(conn, catalog_name);
	if (timeout_ms < 0) {
		throw duckdb::InvalidInputException("listen timeout_ms must be >= 0");
	}
	if (poll_min_ms < 0) {
		throw duckdb::InvalidInputException("listen poll_min_ms must be >= 0");
	}
	const auto clamped_timeout_ms = std::min<int64_t>(timeout_ms, HARD_WAIT_TIMEOUT_MS);
	if (timeout_ms > HARD_WAIT_TIMEOUT_MS) {
		EmitWaitTimeoutClampedNotice(timeout_ms, HARD_WAIT_TIMEOUT_MS);
	}
	if (clamped_timeout_ms > 0) {
		MaybeEmitWaitSharedConnectionWarning(context, clamped_timeout_ms);
	}
	auto interval_ms = std::min<int64_t>(std::max<int64_t>(poll_min_ms, 1), WAIT_MAX_INTERVAL_MS);
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(clamped_timeout_ms);
#ifndef _WIN32
	const auto pg_dsn = MetadataBackendIsPostgres(conn, catalog_name) ? PostgresMetadataDsn(conn, catalog_name) : "";
	PostgresSnapshotListener pg_listener(pg_dsn, SnapshotNotifyChannel(conn, catalog_name));
#endif
	while (true) {
		if (context.interrupted) {
			throw duckdb::InterruptException();
		}
		const auto row = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
		const auto probe = NextMatchingSnapshot(conn, ConnectionCachePrefix(context), catalog_name, row, subscriptions);
		if (!probe.matching_snapshot.IsNull()) {
			const auto snapshot = probe.matching_snapshot.GetValue<int64_t>();
			return {probe.matching_snapshot, duckdb::Value::BIGINT(snapshot - row.last_committed_snapshot)};
		}
		if (row.consumer_kind == "ddl" && probe.scanned_through > row.last_committed_snapshot) {
			if (AdvanceDdlCursorPastInspectedSnapshots(context, conn, catalog_name, row, probe.scanned_through)) {
				// Completing this empty window is observable progress. Return
				// to the caller instead of holding the lease through another
				// long wait without a heartbeat.
				return {duckdb::Value()};
			}
		}
		const auto now = std::chrono::steady_clock::now();
		if (now >= deadline || clamped_timeout_ms == 0) {
			return {duckdb::Value()};
		}
		const auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
#ifndef _WIN32
		if (pg_listener.Available()) {
			pg_listener.Wait(std::min<int64_t>(std::max<int64_t>(remaining_ms, 0), clamped_timeout_ms));
		} else
#endif
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(std::min(interval_ms, remaining_ms)));
		}
		interval_ms = std::min<int64_t>(interval_ms * 2, WAIT_MAX_INTERVAL_MS);
	}
}

std::vector<duckdb::Value> WaitForNextSnapshot(duckdb::ClientContext &context, const ListenWaitData &data) {
	duckdb::Connection conn(*context.db);
	const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
	return WaitForNextSnapshotWithSubscriptions(context, conn, data.catalog_name, data.consumer_name, data.timeout_ms,
	                                            subscriptions, data.poll_min_ms);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ListenWaitInit(duckdb::ClientContext &context,
                                                                    duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ListenWaitData>();
	result->rows.push_back(WaitForNextSnapshot(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_list_consumers
//===--------------------------------------------------------------------===//

void ConsumerListReturnTypes(duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names) {
	// `table_id` and `table_name` are populated for DML consumers (one
	// table per DML consumer, by contract) and NULL for DDL consumers.
	// `table_name` is the *current* qualified name in the DuckLake
	// catalog so an orchestrator never has to chase renames separately.
	names = {"consumer_name",
	         "consumer_kind",
	         "consumer_id",
	         "table_id",
	         "table_name",
	         "subscription_count",
	         "subscriptions_active",
	         "subscriptions_renamed",
	         "subscriptions_dropped",
	         "last_committed_snapshot",
	         "last_committed_schema_version",
	         "terminal_at_snapshot",
	         "owner_token",
	         "owner_acquired_at",
	         "owner_heartbeat_at",
	         "lease_interval_seconds",
	         "created_at",
	         "created_by",
	         "updated_at",
	         "metadata"};
	return_types = {
	    duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::BIGINT,
	    duckdb::LogicalType::BIGINT,       duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::BIGINT,
	    duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,
	    duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,
	    duckdb::LogicalType::UUID,         duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::TIMESTAMP_TZ,
	    duckdb::LogicalType::INTEGER,      duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR,
	    duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR};
}

struct ConsumerListData : public duckdb::TableFunctionData {
	std::string catalog_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerListData>();
		result->catalog_name = catalog_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerListBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_list_consumers requires catalog");
	}
	auto result = duckdb::make_uniq<ConsumerListData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	ConsumerListReturnTypes(return_types, names);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerListInit(duckdb::ClientContext &context,
                                                                      duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerListData>();
	// Single-connection chain — see CreateConsumer for the H-022 rationale.
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	// SELECT order:
	//   0 consumer_name, 1 consumer_kind, 2 consumer_id, 3 table_id,
	//   4 last_committed_snapshot, 5 last_committed_schema_version,
	//   6 owner_token, 7 owner_acquired_at, 8 owner_heartbeat_at,
	//   9 lease_interval_seconds, 10 created_at, 11 created_by,
	//   12 updated_at, 13 metadata.
	auto query = "SELECT consumer_name, consumer_kind, consumer_id, table_id, "
	             "last_committed_snapshot, last_committed_schema_version, owner_token, owner_acquired_at, "
	             "owner_heartbeat_at, lease_interval_seconds, created_at, created_by, updated_at, metadata FROM " +
	             StateTable(conn, data.catalog_name, CONSUMERS_TABLE) + " ORDER BY consumer_id ASC";
	auto query_result = conn.Query(query);
	if (!query_result || query_result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, query_result ? query_result->GetError() : query);
	}
	int64_t current_snapshot = -1;
	for (duckdb::idx_t row_idx = 0; row_idx < query_result->RowCount(); ++row_idx) {
		const auto consumer_name = query_result->GetValue(0, row_idx).ToString();
		const auto consumer_kind = query_result->GetValue(1, row_idx).ToString();
		const auto table_id_value = query_result->GetValue(3, row_idx);
		const auto last_committed_value = query_result->GetValue(4, row_idx);
		const int64_t last_committed_snapshot =
		    last_committed_value.IsNull() ? -1 : last_committed_value.GetValue<int64_t>();
		const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, consumer_name);
		int64_t active = 0;
		int64_t renamed = 0;
		int64_t dropped = 0;
		for (const auto &sub : subscriptions) {
			if (sub.status == "active") {
				active++;
			} else if (sub.status == "renamed") {
				renamed++;
			} else if (sub.status == "dropped") {
				dropped++;
			}
		}
		// Resolve the *current* qualified name for DML consumers'
		// pinned table so the orchestrator never has to chase renames.
		// Costs one catalog lookup per DML consumer; not on a hot path.
		duckdb::Value table_name_value;
		if (consumer_kind == "dml" && !table_id_value.IsNull()) {
			if (current_snapshot < 0) {
				current_snapshot = CurrentSnapshot(conn, data.catalog_name);
			}
			const auto qualified = CurrentQualifiedTableName(conn, data.catalog_name,
			                                                 table_id_value.GetValue<int64_t>(), current_snapshot);
			if (!qualified.empty()) {
				table_name_value = duckdb::Value(qualified);
			}
		}
		// terminal_at_snapshot is only meaningful for DML consumers and
		// only when there is a pending shape boundary for the
		// consumer's subscribed table. Compute lazily so DDL consumers
		// (and DML consumers with no pending boundary) cost nothing.
		duckdb::Value terminal_at_snapshot;
		if (consumer_kind == "dml" && last_committed_snapshot >= 0) {
			if (current_snapshot < 0) {
				current_snapshot = CurrentSnapshot(conn, data.catalog_name);
			}
			const auto dml_subscriptions = LoadDmlConsumerSubscriptions(conn, data.catalog_name, consumer_name);
			const auto boundary = NextDmlSubscribedSchemaChangeSnapshot(
			    conn, data.catalog_name, last_committed_snapshot, current_snapshot, dml_subscriptions);
			if (boundary != -1) {
				terminal_at_snapshot = duckdb::Value::BIGINT(boundary);
			}
		}
		std::vector<duckdb::Value> row;
		row.push_back(query_result->GetValue(0, row_idx)); // consumer_name
		row.push_back(query_result->GetValue(1, row_idx)); // consumer_kind
		row.push_back(query_result->GetValue(2, row_idx)); // consumer_id
		row.push_back(table_id_value);                     // table_id
		row.push_back(table_name_value);                   // table_name
		row.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(subscriptions.size())));
		row.push_back(duckdb::Value::BIGINT(active));
		row.push_back(duckdb::Value::BIGINT(renamed));
		row.push_back(duckdb::Value::BIGINT(dropped));
		row.push_back(query_result->GetValue(4, row_idx)); // last_committed_snapshot
		row.push_back(query_result->GetValue(5, row_idx)); // last_committed_schema_version
		row.push_back(terminal_at_snapshot);
		// Owner / lease / audit / metadata: SELECT cols 6..13.
		for (duckdb::idx_t col_idx = 6; col_idx < query_result->ColumnCount(); ++col_idx) {
			row.push_back(query_result->GetValue(col_idx, row_idx));
		}
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_list_subscriptions
//===--------------------------------------------------------------------===//

struct ConsumerSubscriptionsData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerSubscriptionsData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerSubscriptionsBind(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionBindInput &input,
                                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                                   duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_list_subscriptions requires catalog");
	}
	auto result = duckdb::make_uniq<ConsumerSubscriptionsData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	auto entry = input.named_parameters.find("name");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->consumer_name = entry->second.GetValue<std::string>();
	}
	names = {"consumer_name",
	         "consumer_kind",
	         "consumer_id",
	         "subscription_id",
	         "scope_kind",
	         "schema_id",
	         "table_id",
	         "event_category",
	         "change_type",
	         "original_qualified_name",
	         "current_qualified_name",
	         "status"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerSubscriptionsInit(duckdb::ClientContext &context,
                                                                               duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerSubscriptionsData>();
	// Single-connection chain — see CreateConsumer for the H-022 rationale.
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, data.catalog_name);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	for (const auto &sub : LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name)) {
		result->rows.push_back({duckdb::Value(sub.consumer_name), duckdb::Value(sub.consumer_kind),
		                        duckdb::Value::BIGINT(sub.consumer_id), duckdb::Value::BIGINT(sub.subscription_id),
		                        duckdb::Value(sub.scope_kind), sub.schema_id, sub.table_id,
		                        duckdb::Value(sub.event_category), duckdb::Value(sub.change_type),
		                        sub.original_qualified_name, sub.current_qualified_name, duckdb::Value(sub.status)});
	}
	return std::move(result);
}

} // namespace

//===--------------------------------------------------------------------===//
// Public surface (declared in consumer.hpp)
//===--------------------------------------------------------------------===//

ConsumerRow LoadConsumerOrThrow(duckdb::Connection &conn, const std::string &catalog_name,
                                const std::string &consumer_name) {
	const auto consumers = StateTable(conn, catalog_name, CONSUMERS_TABLE);
	auto result = conn.Query("SELECT " + ConsumerRowProjection() + " FROM " + consumers +
	                         " WHERE consumer_name = " + QuoteLiteral(consumer_name) + " LIMIT 1");
	if (!result || result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, result ? result->GetError() : "consumer lookup failed");
	}
	if (result->RowCount() == 0) {
		throw duckdb::InvalidInputException("consumer '%s' does not exist", consumer_name);
	}
	return ConsumerRowFromResult(*result, 0);
}

std::string CurrentQualifiedTableName(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                                      int64_t snapshot_id) {
	int64_t schema_id = 0;
	std::string qualified;
	if (!ResolveTableByIdAt(conn, catalog_name, table_id, snapshot_id, schema_id, qualified)) {
		return std::string();
	}
	return qualified;
}

bool ResolveCurrentTableName(duckdb::Connection &conn, const std::string &catalog_name,
                             const std::string &qualified_name, int64_t snapshot_id, int64_t &schema_id,
                             int64_t &table_id) {
	const auto dot = qualified_name.find('.');
	if (dot == std::string::npos) {
		return ResolveCurrentTableName(conn, catalog_name, std::string("main.") + qualified_name, snapshot_id,
		                               schema_id, table_id);
	}
	const auto schema_name = qualified_name.substr(0, dot);
	const auto table_name = qualified_name.substr(dot + 1);
	auto result =
	    conn.Query("SELECT s.schema_id, t.table_id FROM " + MetadataTable(conn, catalog_name, "ducklake_table") +
	               " t JOIN " + MetadataTable(conn, catalog_name, "ducklake_schema") +
	               " s USING (schema_id) WHERE s.schema_name = " + QuoteLiteral(schema_name) + " AND t.table_name = " +
	               QuoteLiteral(table_name) + " AND t.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(snapshot_id) +
	               ") AND s.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (s.end_snapshot IS NULL OR s.end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull() ||
	    result->GetValue(1, 0).IsNull()) {
		return false;
	}
	schema_id = result->GetValue(0, 0).GetValue<int64_t>();
	table_id = result->GetValue(1, 0).GetValue<int64_t>();
	return true;
}

std::string CurrentSchemaName(duckdb::Connection &conn, const std::string &catalog_name, int64_t schema_id,
                              int64_t snapshot_id) {
	std::string schema_name;
	if (!ResolveSchemaByIdAt(conn, catalog_name, schema_id, snapshot_id, schema_name)) {
		return std::string();
	}
	return schema_name;
}

std::vector<ConsumerSubscriptionRow>
LoadConsumerSubscriptions(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name) {
	std::vector<ConsumerSubscriptionRow> out;
	const auto current_snapshot = CurrentSnapshot(conn, catalog_name);
	std::ostringstream query;
	query << "SELECT c.consumer_name, c.consumer_id, s.subscription_id, s.scope_kind, s.schema_id, s.table_id, "
	      << "s.event_category, s.change_type, s.original_qualified_name, c.consumer_kind FROM "
	      << StateTable(conn, catalog_name, CONSUMERS_TABLE) << " c JOIN "
	      << StateTable(conn, catalog_name, CONSUMER_SUBSCRIPTIONS_TABLE) << " s ON s.consumer_id = c.consumer_id";
	if (!consumer_name.empty()) {
		query << " WHERE c.consumer_name = " << QuoteLiteral(consumer_name);
	}
	query << " ORDER BY c.consumer_id ASC, s.subscription_id ASC";
	auto result = conn.Query(query.str());
	if (!result || result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        result ? result->GetError() : "consumer subscription lookup failed");
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		ConsumerSubscriptionRow row;
		row.consumer_name = result->GetValue(0, i).ToString();
		row.consumer_kind = result->GetValue(9, i).ToString();
		row.consumer_id = result->GetValue(1, i).GetValue<int64_t>();
		row.subscription_id = result->GetValue(2, i).GetValue<int64_t>();
		row.scope_kind = result->GetValue(3, i).ToString();
		row.schema_id = result->GetValue(4, i);
		row.table_id = result->GetValue(5, i);
		row.event_category = result->GetValue(6, i).ToString();
		row.change_type = result->GetValue(7, i).ToString();
		row.original_qualified_name = result->GetValue(8, i);
		row.status = "active";
		if (row.scope_kind == "catalog") {
			row.current_qualified_name = duckdb::Value();
		} else if (row.scope_kind == "schema" && !row.schema_id.IsNull()) {
			const auto current =
			    CurrentSchemaName(conn, catalog_name, row.schema_id.GetValue<int64_t>(), current_snapshot);
			if (current.empty()) {
				row.status = "dropped";
				row.current_qualified_name = duckdb::Value();
			} else {
				row.current_qualified_name = duckdb::Value(current);
				if (!row.original_qualified_name.IsNull() && row.original_qualified_name.ToString() != current) {
					row.status = "renamed";
				}
			}
		} else if (row.scope_kind == "table" && !row.table_id.IsNull()) {
			const auto current =
			    CurrentQualifiedTableName(conn, catalog_name, row.table_id.GetValue<int64_t>(), current_snapshot);
			if (current.empty()) {
				row.status = "dropped";
				row.current_qualified_name = duckdb::Value();
			} else {
				row.current_qualified_name = duckdb::Value(current);
				if (!row.original_qualified_name.IsNull() && row.original_qualified_name.ToString() != current) {
					row.status = "renamed";
				}
			}
		}
		out.push_back(std::move(row));
	}
	return out;
}

std::vector<ConsumerSubscriptionRow> LoadDmlConsumerSubscriptions(duckdb::Connection &conn,
                                                                  const std::string &catalog_name,
                                                                  const std::string &consumer_name) {
	std::vector<ConsumerSubscriptionRow> out;
	std::ostringstream query;
	query << "SELECT c.consumer_name, c.consumer_id, s.subscription_id, s.scope_kind, s.schema_id, s.table_id, "
	      << "s.event_category, s.change_type, s.original_qualified_name, c.consumer_kind FROM "
	      << StateTable(conn, catalog_name, CONSUMERS_TABLE) << " c JOIN "
	      << StateTable(conn, catalog_name, CONSUMER_SUBSCRIPTIONS_TABLE) << " s ON s.consumer_id = c.consumer_id "
	      << "WHERE c.consumer_name = " << QuoteLiteral(consumer_name) << " AND s.event_category = 'dml' "
	      << "ORDER BY c.consumer_id ASC, s.subscription_id ASC";
	auto result = conn.Query(query.str());
	if (!result || result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        result ? result->GetError() : "DML consumer subscription lookup failed");
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		ConsumerSubscriptionRow row;
		row.consumer_name = result->GetValue(0, i).ToString();
		row.consumer_kind = result->GetValue(9, i).ToString();
		row.consumer_id = result->GetValue(1, i).GetValue<int64_t>();
		row.subscription_id = result->GetValue(2, i).GetValue<int64_t>();
		row.scope_kind = result->GetValue(3, i).ToString();
		row.schema_id = result->GetValue(4, i);
		row.table_id = result->GetValue(5, i);
		row.event_category = result->GetValue(6, i).ToString();
		row.change_type = result->GetValue(7, i).ToString();
		row.original_qualified_name = result->GetValue(8, i);
		row.current_qualified_name = duckdb::Value();
		row.status = "active";
		out.push_back(std::move(row));
	}
	return out;
}

bool SubscriptionCoversTable(const ConsumerSubscriptionRow &subscription, int64_t schema_id, int64_t table_id,
                             const std::string &event_category) {
	// A "dropped" status means the table/schema no longer exists at HEAD,
	// but the original subscription still applies for any pre-drop snapshot
	// the consumer hasn't drained yet. We must NOT gate by `status` here:
	// the schema-shape boundary detector caps the readable window strictly
	// BEFORE the drop snapshot, so callers either:
	//   - read a pre-drop snapshot (the subscription was active then), or
	//   - try to read past the boundary (already rejected upstream by the
	//     terminal cdc_window contract).
	// Filtering "dropped" out here would orphan the still-drainable backlog
	// of a terminated DML consumer (regression: see Rule 6 in
	// `dml_schema_shape_pinning.test`).
	if (subscription.event_category != event_category) {
		return false;
	}
	if (subscription.scope_kind == "catalog") {
		return true;
	}
	if (subscription.scope_kind == "schema" && !subscription.schema_id.IsNull()) {
		return subscription.schema_id.GetValue<int64_t>() == schema_id;
	}
	if (subscription.scope_kind == "table" && !subscription.table_id.IsNull()) {
		return subscription.table_id.GetValue<int64_t>() == table_id;
	}
	return false;
}

std::vector<std::string> MatchingDmlChangeTypes(const std::vector<ConsumerSubscriptionRow> &subscriptions,
                                                int64_t schema_id, int64_t table_id) {
	std::vector<std::string> change_types;
	for (const auto &subscription : subscriptions) {
		if (SubscriptionCoversTable(subscription, schema_id, table_id, "dml")) {
			change_types.push_back(subscription.change_type);
		}
	}
	std::sort(change_types.begin(), change_types.end());
	change_types.erase(std::unique(change_types.begin(), change_types.end()), change_types.end());
	return change_types;
}

int64_t NextDmlSubscribedSchemaChangeSnapshot(duckdb::Connection &conn, const std::string &catalog_name,
                                              int64_t start_snapshot, int64_t current_snapshot,
                                              const std::vector<ConsumerSubscriptionRow> &dml_subscriptions) {
	if (dml_subscriptions.empty() || start_snapshot > current_snapshot) {
		return -1;
	}
	// Collect both the subscribed `table_id`s and the subscriptions'
	// `schema_id`s so we can also catch `dropped_schema:<id>` tokens that
	// implicitly take a subscribed table down with them.
	std::unordered_set<int64_t> subscribed_table_ids;
	std::unordered_set<int64_t> subscribed_schema_ids;
	DmlSubscriptionIds(dml_subscriptions, subscribed_table_ids, subscribed_schema_ids);
	if (subscribed_table_ids.empty()) {
		return -1;
	}
	// DuckLake 1.0 token vocabulary (per the `ducklake_snapshot_changes`
	// spec) reduces to three shape-affecting tokens for a DML consumer:
	//   - `altered_table:<table_id>` — covers ALTER … ADD/DROP COLUMN and
	//     ALTER … RENAME (renames are encoded as ALTER, not a separate
	//     `renamed_table:` token).
	//   - `dropped_table:<table_id>` — direct drop of a subscribed table.
	//   - `dropped_schema:<schema_id>` — drop of a subscribed table's
	//     containing schema. DROP SCHEMA … CASCADE in DuckLake also emits
	//     per-table `dropped_table:<id>` tokens; matching the schema id
	//     here keeps detection stable in case that decomposition ever
	//     changes.
	// `created_table` / `inserted_into_table` / `deleted_from_table` /
	// `compacted_table` / view tokens / `created_schema` are NOT shape
	// changes for the consumer's existing subscriptions and are skipped.
	for (const auto &change : LoadDecodedSnapshotChanges(conn, catalog_name, start_snapshot, current_snapshot)) {
		for (const auto table_id : change.altered_table_ids) {
			if (subscribed_table_ids.count(table_id) > 0) {
				return change.snapshot_id;
			}
		}
		for (const auto table_id : change.dropped_table_ids) {
			if (subscribed_table_ids.count(table_id) > 0) {
				return change.snapshot_id;
			}
		}
		for (const auto schema_id : change.dropped_schema_ids) {
			if (subscribed_schema_ids.count(schema_id) > 0) {
				return change.snapshot_id;
			}
		}
	}
	return -1;
}

int64_t MaxSnapshotsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("max_snapshots");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return DEFAULT_MAX_SNAPSHOTS;
	}
	return entry->second.GetValue<int64_t>();
}

int64_t PollMinMsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("poll_min_ms");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return WAIT_INITIAL_INTERVAL_MS;
	}
	const auto value = entry->second.GetValue<int64_t>();
	if (value < 0) {
		throw duckdb::InvalidInputException("listen poll_min_ms must be >= 0");
	}
	return value;
}

std::vector<duckdb::Value> ReadWindowWithConnection(duckdb::ClientContext &context, duckdb::Connection &conn,
                                                    const CdcWindowData &data) {
	CheckCatalogOrThrow(conn, data.catalog_name);
	if (data.max_snapshots > HARD_MAX_SNAPSHOTS) {
		ThrowMaxSnapshotsExceeded(data.max_snapshots);
	}
	if (data.max_snapshots < 1) {
		throw duckdb::InvalidInputException("cdc_window max_snapshots must be >= 1");
	}

	const auto cached_token = CachedToken(context, conn, data.catalog_name, data.consumer_name);
	ConsumerRow row;
	if (!TryUseFreshCachedLease(conn, data.catalog_name, data.consumer_name, cached_token, row)) {
		const auto backend = CachedStateBackend(context, conn, data.catalog_name);
		row = AcquireLease(conn, data.catalog_name, data.consumer_name, cached_token, backend);
	}
	const auto last_snapshot = row.last_committed_snapshot;
	const auto consumer_kind = row.consumer_kind;
	const auto owner_token = row.owner_token.ToString();
	CacheToken(context, conn, data.catalog_name, data.consumer_name, owner_token);
	const bool is_dml = consumer_kind == "dml";

	if (is_dml) {
		// DuckDB's Postgres scanner cannot push the aggregate in
		// ResolveDmlWindowIndexed into the remote query. Avoid copying the
		// complete snapshot metadata history when this consumer is already at
		// the catalogue head—the steady-state path for non-blocking relays.
		if (MetadataBackendIsPostgres(conn, data.catalog_name)) {
			const auto current_snapshot = CurrentSnapshot(conn, data.catalog_name);
			if (last_snapshot == current_snapshot) {
				CacheDmlSafeCommitRange(context, conn, data.catalog_name, data.consumer_name, last_snapshot,
				                        last_snapshot);
				return {duckdb::Value::BIGINT(last_snapshot + 1),
				        duckdb::Value::BIGINT(last_snapshot),
				        duckdb::Value::BOOLEAN(false),
				        duckdb::Value::BIGINT(row.last_committed_schema_version),
				        duckdb::Value::BOOLEAN(false),
				        duckdb::Value::BOOLEAN(false),
				        duckdb::Value()};
			}
		}
		const auto subscriptions = LoadDmlConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
		const auto resolved =
		    ResolveDmlWindowIndexed(conn, data.catalog_name, last_snapshot, data.max_snapshots, subscriptions);
		if (!resolved.last_snapshot_exists) {
			const auto oldest_snapshot = RequiredInt64(resolved.oldest_snapshot, "oldest snapshot");
			throw duckdb::InvalidInputException(
			    "CDC_GAP: consumer '%s' is at snapshot %lld, but the oldest available snapshot is %lld. To recover "
			    "and skip the gap: CALL cdc_consumer_reset('%s', '%s', to_snapshot => 'oldest_available'); To "
			    "preserve all events, run consumers more frequently than your expire_older_than setting.",
			    data.consumer_name, static_cast<long long>(last_snapshot), static_cast<long long>(oldest_snapshot),
			    data.catalog_name, data.consumer_name);
		}
		const auto schema_version =
		    resolved.schema_version == -1 ? row.last_committed_schema_version : resolved.schema_version;
		const bool schema_changes_pending = resolved.boundary_snapshot != -1;
		const bool terminal = !resolved.has_changes && schema_changes_pending;
		const duckdb::Value terminal_at_snapshot =
		    schema_changes_pending ? duckdb::Value::BIGINT(resolved.boundary_snapshot) : duckdb::Value();
		if (schema_changes_pending) {
			const auto end_schema_version = resolved.has_changes
			                                    ? ResolveSchemaVersion(conn, data.catalog_name, resolved.end_snapshot)
			                                    : schema_version;
			const auto boundary_schema_version = RequiredInt64(resolved.boundary_schema_version, "schema_version");
			if (end_schema_version != boundary_schema_version) {
				EmitSchemaBoundaryNotice(data.consumer_name,
				                         resolved.has_changes ? resolved.end_snapshot : last_snapshot,
				                         end_schema_version, resolved.boundary_snapshot, boundary_schema_version);
			}
		}
		CacheDmlSafeCommitRange(context, conn, data.catalog_name, data.consumer_name, last_snapshot,
		                        resolved.has_changes ? resolved.end_snapshot : last_snapshot);
		return {duckdb::Value::BIGINT(resolved.start_snapshot),
		        duckdb::Value::BIGINT(resolved.end_snapshot),
		        duckdb::Value::BOOLEAN(resolved.has_changes),
		        duckdb::Value::BIGINT(schema_version),
		        duckdb::Value::BOOLEAN(schema_changes_pending),
		        duckdb::Value::BOOLEAN(terminal),
		        terminal_at_snapshot};
	}

	const auto resolved = ResolveWindowFast(conn, data.catalog_name, last_snapshot, data.max_snapshots);
	if (!resolved.last_snapshot_exists) {
		const auto oldest_snapshot = RequiredInt64(resolved.oldest_snapshot, "oldest snapshot");
		throw duckdb::InvalidInputException(
		    "CDC_GAP: consumer '%s' is at snapshot %lld, but the oldest available snapshot is %lld. To recover "
		    "and skip the gap: CALL cdc_consumer_reset('%s', '%s', to_snapshot => 'oldest_available'); To "
		    "preserve all events, run consumers more frequently than your expire_older_than setting.",
		    data.consumer_name, static_cast<long long>(last_snapshot), static_cast<long long>(oldest_snapshot),
		    data.catalog_name, data.consumer_name);
	}
	const auto current_snapshot = resolved.current_snapshot;
	const auto first_snapshot = resolved.first_snapshot;
	const auto start_snapshot = resolved.start_snapshot;
	int64_t end_snapshot = resolved.end_snapshot;
	bool schema_changes_pending = false;
	int64_t boundary_next_snapshot = -1;
	duckdb::Value boundary_next_schema_version;
	int64_t ddl_schema_change_snapshot = -1;
	duckdb::Value ddl_schema_change_version;
	auto schema_version = last_snapshot <= current_snapshot
	                          ? RequiredInt64(resolved.last_schema_version, "schema_version")
	                          : row.last_committed_schema_version;
	// DML consumers are pinned to the schema shape of their subscribed
	// tables. The boundary detector flags an `altered_table:<id>`,
	// `dropped_table:<id>`, or `dropped_schema:<id>` token in the visible
	// range whose target intersects the consumer's subscriptions. When it
	// fires, the window collapses unconditionally and the consumer is
	// terminal: no more DML for the old shape, no advance possible.
	// Catalog-wide DDL on unsubscribed objects does NOT terminate a DML
	// consumer; the DML listen path auto-advances past those snapshots.
	int64_t dml_boundary_snapshot = -1;
	if (is_dml && first_snapshot != -1 && start_snapshot <= current_snapshot) {
		const auto subscriptions = LoadDmlConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
		dml_boundary_snapshot = NextDmlSubscribedSchemaChangeSnapshot(conn, data.catalog_name, last_snapshot,
		                                                              current_snapshot, subscriptions);
	}
	if (first_snapshot != -1 && start_snapshot <= current_snapshot) {
		schema_version = RequiredInt64(resolved.start_schema_version, "schema_version");
		if (is_dml) {
			// DML consumers: schema_changes_pending strictly tracks
			// shape changes for the consumer's subscribed tables.
			// Catalog-wide DDL on unsubscribed objects is invisible
			// here and the listen path auto-advances past it.
			if (dml_boundary_snapshot != -1) {
				schema_changes_pending = true;
				if (dml_boundary_snapshot - 1 < end_snapshot) {
					end_snapshot = dml_boundary_snapshot - 1;
				}
				boundary_next_snapshot = dml_boundary_snapshot;
				boundary_next_schema_version =
				    duckdb::Value::BIGINT(ResolveSchemaVersion(conn, data.catalog_name, dml_boundary_snapshot));
			}
		} else {
			// DDL consumers: surface any catalog-wide schema-version
			// transition in the visible range. The schema-change
			// snapshot stays inside the window so the DDL events for
			// the transition can be drained on the same call.
			if (resolved.start_is_schema_change) {
				schema_changes_pending = true;
				ddl_schema_change_snapshot = start_snapshot;
				ddl_schema_change_version = resolved.start_schema_version;
			}
			const auto next_schema_change = resolved.next_schema_change;
			if (next_schema_change != -1 && next_schema_change <= end_snapshot) {
				schema_changes_pending = true;
				if (ddl_schema_change_snapshot == -1) {
					ddl_schema_change_snapshot = next_schema_change;
					ddl_schema_change_version = resolved.next_schema_change_schema_version;
				}
			}
		}
	}
	const bool has_changes = end_snapshot >= start_snapshot;
	// `terminal` is the canonical "this DML consumer cannot advance any
	// further on the current shape" signal. By construction it is only
	// ever true for DML consumers: DDL consumers are designed to ride
	// schema changes.
	const bool terminal = is_dml && !has_changes && schema_changes_pending && dml_boundary_snapshot != -1;
	const duckdb::Value terminal_at_snapshot =
	    is_dml && dml_boundary_snapshot != -1 ? duckdb::Value::BIGINT(dml_boundary_snapshot) : duckdb::Value();
	// docs/errors.md spells the notice as "window ends at snapshot N
	// (schema_version X); the next snapshot is at schema_version Y".
	// `schema_version` is the schema at `start_snapshot` (kept that
	// way for the row payload contract); compute the schema at the
	// actual `end_snapshot` for the notice so X<Y reads correctly
	// even when the window collapsed to a single snapshot at the
	// boundary.
	int64_t end_schema_version_for_notice = -1;
	if (schema_changes_pending && boundary_next_snapshot != -1) {
		end_schema_version_for_notice =
		    RequiredInt64(has_changes ? resolved.end_schema_version : resolved.last_schema_version, "schema_version");
	}
	// Emit the CDC_SCHEMA_BOUNDARY notice after window resolution so callers
	// see the boundary before deciding which DDL/DML paths to drain.
	if (is_dml && schema_changes_pending && boundary_next_snapshot != -1 &&
	    end_schema_version_for_notice != RequiredInt64(boundary_next_schema_version, "schema_version")) {
		EmitSchemaBoundaryNotice(data.consumer_name, has_changes ? end_snapshot : last_snapshot,
		                         end_schema_version_for_notice, boundary_next_snapshot,
		                         RequiredInt64(boundary_next_schema_version, "schema_version"));
	}
	if (!is_dml && schema_changes_pending && ddl_schema_change_snapshot != -1) {
		EmitDdlSchemaBoundaryNotice(data.consumer_name, start_snapshot, end_snapshot, ddl_schema_change_snapshot,
		                            RequiredInt64(ddl_schema_change_version, "schema_version"));
	}
	if (is_dml) {
		CacheDmlSafeCommitRange(context, conn, data.catalog_name, data.consumer_name, last_snapshot,
		                        has_changes ? end_snapshot : last_snapshot);
	}
	return {duckdb::Value::BIGINT(start_snapshot),
	        duckdb::Value::BIGINT(end_snapshot),
	        duckdb::Value::BOOLEAN(has_changes),
	        duckdb::Value::BIGINT(schema_version),
	        duckdb::Value::BOOLEAN(schema_changes_pending),
	        duckdb::Value::BOOLEAN(terminal),
	        terminal_at_snapshot};
}

std::vector<duckdb::Value> ReadWindow(duckdb::ClientContext &context, const CdcWindowData &data) {
	duckdb::Connection conn(*context.db);
	return ReadWindowWithConnection(context, conn, data);
}

std::vector<duckdb::Value> CommitConsumerSnapshot(duckdb::ClientContext &context, const std::string &catalog_name,
                                                  const std::string &consumer_name, int64_t snapshot_id) {
	CdcCommitData data;
	data.catalog_name = catalog_name;
	data.consumer_name = consumer_name;
	data.snapshot_id = snapshot_id;
	return CommitWindow(context, data);
}

std::vector<duckdb::Value> CommitConsumerSnapshotWithConnection(duckdb::ClientContext &context,
                                                                duckdb::Connection &conn,
                                                                const std::string &catalog_name,
                                                                const std::string &consumer_name, int64_t snapshot_id) {
	CdcCommitData data;
	data.catalog_name = catalog_name;
	data.consumer_name = consumer_name;
	data.snapshot_id = snapshot_id;
	return CommitWindowWithConnection(context, conn, data);
}

std::vector<duckdb::Value> WaitForConsumerSnapshot(duckdb::ClientContext &context, const std::string &catalog_name,
                                                   const std::string &consumer_name, int64_t timeout_ms,
                                                   int64_t poll_min_ms) {
	ListenWaitData data;
	data.catalog_name = catalog_name;
	data.consumer_name = consumer_name;
	data.timeout_ms = timeout_ms;
	data.poll_min_ms = poll_min_ms;
	return WaitForNextSnapshot(context, data);
}

std::vector<duckdb::Value> WaitForDmlConsumerSnapshot(duckdb::ClientContext &context, duckdb::Connection &conn,
                                                      const std::string &catalog_name, const std::string &consumer_name,
                                                      int64_t timeout_ms,
                                                      const std::vector<ConsumerSubscriptionRow> &subscriptions,
                                                      int64_t poll_min_ms) {
	return WaitForNextSnapshotWithSubscriptions(context, conn, catalog_name, consumer_name, timeout_ms, subscriptions,
	                                            poll_min_ms);
}

int64_t AdaptiveListenDelayMs(const std::string &catalog_name, const std::string &consumer_name,
                              const std::string &stream_key, int64_t timeout_ms) {
	if (timeout_ms <= 0) {
		return 0;
	}
	std::lock_guard<std::mutex> guard(ADAPTIVE_LISTEN_MUTEX);
	const auto entry = ADAPTIVE_LISTEN_STATES.find(AdaptiveListenKey(catalog_name, consumer_name, stream_key));
	if (entry == ADAPTIVE_LISTEN_STATES.end() || entry->second.burst_streak < 2) {
		return 0;
	}
	const auto exponent = std::min<int64_t>(entry->second.burst_streak - 2, 4);
	const auto delay_ms = ADAPTIVE_LISTEN_MIN_COALESCE_MS << exponent;
	return std::min<int64_t>(std::min<int64_t>(delay_ms, ADAPTIVE_LISTEN_MAX_COALESCE_MS), timeout_ms);
}

void MaybeCoalesceConsumerListenWithConnection(duckdb::ClientContext &context, duckdb::Connection &conn,
                                               const std::string &catalog_name, const std::string &consumer_name,
                                               const std::string &stream_key, int64_t timeout_ms, int64_t max_snapshots,
                                               int64_t first_matching_snapshot) {
	const auto coalesce_ms = AdaptiveListenDelayMs(catalog_name, consumer_name, stream_key, timeout_ms);
	if (coalesce_ms <= 0 || max_snapshots <= 1 || first_matching_snapshot < 0) {
		return;
	}

	auto current_snapshot = CurrentSnapshot(conn, catalog_name);
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(coalesce_ms);
	while (std::chrono::steady_clock::now() < deadline) {
		if (context.interrupted) {
			throw duckdb::InterruptException();
		}
		if (current_snapshot - first_matching_snapshot + 1 >= max_snapshots) {
			return;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(ADAPTIVE_LISTEN_POLL_MS));
		const auto next_snapshot = CurrentSnapshot(conn, catalog_name);
		if (next_snapshot > current_snapshot) {
			current_snapshot = next_snapshot;
		}
	}
}

void MaybeCoalesceConsumerListen(duckdb::ClientContext &context, const std::string &catalog_name,
                                 const std::string &consumer_name, const std::string &stream_key, int64_t timeout_ms,
                                 int64_t max_snapshots, int64_t first_matching_snapshot) {
	duckdb::Connection conn(*context.db);
	MaybeCoalesceConsumerListenWithConnection(context, conn, catalog_name, consumer_name, stream_key, timeout_ms,
	                                          max_snapshots, first_matching_snapshot);
}

void RecordConsumerListenResult(const std::string &catalog_name, const std::string &consumer_name,
                                const std::string &stream_key, bool has_rows, int64_t start_snapshot,
                                int64_t end_snapshot, int64_t row_count, int64_t max_snapshots) {
	const auto key = AdaptiveListenKey(catalog_name, consumer_name, stream_key);
	const auto now = std::chrono::steady_clock::now();
	std::lock_guard<std::mutex> guard(ADAPTIVE_LISTEN_MUTEX);
	auto &state = ADAPTIVE_LISTEN_STATES[key];
	if (!has_rows) {
		state.burst_streak = 0;
		state.has_last_success = false;
		return;
	}

	const auto window_span = end_snapshot >= start_snapshot ? end_snapshot - start_snapshot + 1 : 0;
	const auto is_small_result = row_count < ADAPTIVE_LISTEN_TARGET_ROWS &&
	                             window_span < ADAPTIVE_LISTEN_TARGET_SNAPSHOTS && window_span < max_snapshots;
	const auto is_quick_success =
	    state.has_last_success &&
	    std::chrono::duration_cast<std::chrono::milliseconds>(now - state.last_success_at).count() <
	        ADAPTIVE_LISTEN_BURST_THRESHOLD_MS;
	if (is_quick_success && is_small_result) {
		state.burst_streak += 1;
	} else if (!is_small_result) {
		state.burst_streak = 0;
	} else {
		state.burst_streak = std::max<int64_t>(0, state.burst_streak - 1);
	}
	state.has_last_success = true;
	state.last_success_at = now;
}

void RegisterConsumerFunctions(duckdb::ExtensionLoader &loader) {
	const auto varchar_list = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
	const auto bigint_list = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
	duckdb::TableFunction configure_function("cdc_configure",
	                                         duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
	                                         RowScanExecute, CdcConfigureBind, CdcConfigureInit);
	configure_function.named_parameters["state_schema"] = duckdb::LogicalType::VARCHAR;
	configure_function.named_parameters["metadata_schema"] = duckdb::LogicalType::VARCHAR;
	loader.RegisterFunction(configure_function);

	duckdb::TableFunction ddl_create_function(
	    "cdc_ddl_consumer_create",
	    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
	    RowScanExecute, DdlConsumerCreateBind, ConsumerCreateInit);
	ddl_create_function.named_parameters["start_at"] = duckdb::LogicalType::VARCHAR;
	ddl_create_function.named_parameters["schemas"] = varchar_list;
	ddl_create_function.named_parameters["schema_ids"] = bigint_list;
	ddl_create_function.named_parameters["table_names"] = varchar_list;
	ddl_create_function.named_parameters["table_ids"] = bigint_list;
	ddl_create_function.named_parameters["metadata"] = duckdb::LogicalType::VARCHAR;
	loader.RegisterFunction(ddl_create_function);

	duckdb::TableFunction dml_create_function(
	    "cdc_dml_consumer_create",
	    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
	    RowScanExecute, DmlConsumerCreateBind, ConsumerCreateInit);
	dml_create_function.named_parameters["start_at"] = duckdb::LogicalType::VARCHAR;
	// Scalar table identity is optional. Pass exactly one of `table_name`
	// or `table_id` for row changes, or omit both for catalog-wide ticks.
	dml_create_function.named_parameters["table_name"] = duckdb::LogicalType::VARCHAR;
	dml_create_function.named_parameters["table_id"] = duckdb::LogicalType::BIGINT;
	dml_create_function.named_parameters["change_types"] = varchar_list;
	dml_create_function.named_parameters["metadata"] = duckdb::LogicalType::VARCHAR;
	loader.RegisterFunction(dml_create_function);

	for (const auto &name : {"cdc_consumer_reset"}) {
		duckdb::TableFunction reset_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerResetBind, ConsumerResetInit);
		reset_function.named_parameters["to_snapshot"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(reset_function);
	}

	for (const auto &name : {"cdc_consumer_drop"}) {
		duckdb::TableFunction drop_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerDropBind, ConsumerDropInit);
		loader.RegisterFunction(drop_function);
	}

	for (const auto &name : {"cdc_consumer_force_release"}) {
		duckdb::TableFunction force_release_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerForceReleaseBind, ConsumerForceReleaseInit);
		loader.RegisterFunction(force_release_function);
	}

	for (const auto &name : {"cdc_consumer_release"}) {
		duckdb::TableFunction release_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerReleaseBind, ConsumerReleaseInit);
		loader.RegisterFunction(release_function);
	}

	for (const auto &name : {"cdc_consumer_heartbeat"}) {
		duckdb::TableFunction heartbeat_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerHeartbeatBind, ConsumerHeartbeatInit);
		loader.RegisterFunction(heartbeat_function);
	}

	for (const auto &name : {"cdc_window"}) {
		duckdb::TableFunction window_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, CdcWindowBind, CdcWindowInit);
		window_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(window_function);
	}

	for (const auto &name : {"cdc_commit"}) {
		duckdb::TableFunction commit_function(name,
		                                      duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR,
		                                                                           duckdb::LogicalType::VARCHAR,
		                                                                           duckdb::LogicalType::BIGINT},
		                                      RowScanExecute, CdcCommitBind, CdcCommitInit);
		loader.RegisterFunction(commit_function);
	}

	for (const auto &name : {"cdc_list_consumers"}) {
		duckdb::TableFunction list_function(name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                    RowScanExecute, ConsumerListBind, ConsumerListInit);
		loader.RegisterFunction(list_function);
	}

	for (const auto &name : {"cdc_list_subscriptions"}) {
		duckdb::TableFunction subscriptions_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR}, RowScanExecute,
		    ConsumerSubscriptionsBind, ConsumerSubscriptionsInit);
		subscriptions_function.named_parameters["name"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(subscriptions_function);
	}
}

} // namespace duckdb_cdc
