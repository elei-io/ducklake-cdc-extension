//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// dml.cpp
//
// DML stream implementation. One DML consumer = one table, by contract;
// `cdc_dml_changes_read` / `cdc_dml_changes_listen` therefore project the
// subscribed table's native DuckDB column types directly (no JSON payload).
// `cdc_dml_changes_query` is the stateless single-table sibling for
// ad-hoc lookback. `cdc_dml_ticks_*` summarise per-snapshot DML activity
// across the consumer's subscriptions for monitoring use cases.
//===----------------------------------------------------------------------===//

#include "dml.hpp"

#include "compat_check.hpp"
#include "consumer.hpp"
#include "ddl.hpp"
#include "ducklake_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

namespace duckdb_cdc {

namespace {

//===--------------------------------------------------------------------===//
// DML ticks
//===--------------------------------------------------------------------===//

struct DmlTicksData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t max_snapshots;
	bool auto_commit = false;
	bool listen = false;
	int64_t timeout_ms = DEFAULT_WAIT_TIMEOUT_MS;
	int64_t poll_min_ms = WAIT_INITIAL_INTERVAL_MS;
	bool coalesce = true;
	bool explicit_window = false;
	int64_t start_snapshot = -1;
	int64_t end_snapshot = -1;
	bool stateless = false;
	int64_t from_snapshot = -1;
	int64_t to_snapshot = -1;
	std::vector<int64_t> table_ids;
	std::vector<std::string> table_names;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<DmlTicksData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->max_snapshots = max_snapshots;
		result->auto_commit = auto_commit;
		result->listen = listen;
		result->timeout_ms = timeout_ms;
		result->poll_min_ms = poll_min_ms;
		result->coalesce = coalesce;
		result->explicit_window = explicit_window;
		result->start_snapshot = start_snapshot;
		result->end_snapshot = end_snapshot;
		result->stateless = stateless;
		result->from_snapshot = from_snapshot;
		result->to_snapshot = to_snapshot;
		result->table_ids = table_ids;
		result->table_names = table_names;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

std::vector<std::string> SplitEventTokens(const std::string &changes_made) {
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

bool ParseTableIdToken(const std::string &token, const std::string &prefix, int64_t &table_id) {
	if (token.rfind(prefix, 0) != 0) {
		return false;
	}
	return TryParseInt64(token.substr(prefix.size()), table_id);
}

bool DmlTokenInfo(const std::string &token, int64_t &table_id, std::string &change_type) {
	for (const auto &prefix : {"inserted_into_table:", "tables_inserted_into:", "inlined_insert:"}) {
		if (ParseTableIdToken(token, prefix, table_id)) {
			change_type = "insert";
			return true;
		}
	}
	for (const auto &prefix : {"deleted_from_table:", "tables_deleted_from:", "inlined_delete:"}) {
		if (ParseTableIdToken(token, prefix, table_id)) {
			change_type = "delete";
			return true;
		}
	}
	return false;
}

bool DdlTokenInfo(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                  const std::string &token, int64_t &schema_id, int64_t &table_id, bool &has_table) {
	has_table = false;
	if (token.rfind("created_schema:", 0) == 0 || token.rfind("dropped_schema:", 0) == 0) {
		return false;
	}
	for (const auto &prefix : {"altered_table:", "dropped_table:"}) {
		if (ParseTableIdToken(token, prefix, table_id)) {
			const auto qualified = CurrentQualifiedTableName(conn, catalog_name, table_id, snapshot_id);
			if (!qualified.empty()) {
				int64_t ignored = 0;
				ResolveCurrentTableName(conn, catalog_name, qualified, snapshot_id, schema_id, ignored);
			}
			has_table = true;
			return true;
		}
	}
	return false;
}

bool ChangesTouchSubscriptions(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                               const std::string &changes_made,
                               const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	for (const auto &token : SplitEventTokens(changes_made)) {
		int64_t table_id = 0;
		std::string change_type;
		if (DmlTokenInfo(token, table_id, change_type)) {
			const auto qualified = CurrentQualifiedTableName(conn, catalog_name, table_id, snapshot_id);
			if (qualified.empty()) {
				continue;
			}
			int64_t schema_id = 0;
			int64_t ignored_table = 0;
			ResolveCurrentTableName(conn, catalog_name, qualified, snapshot_id, schema_id, ignored_table);
			for (const auto &subscription : subscriptions) {
				if (subscription.change_type == change_type &&
				    SubscriptionCoversTable(subscription, schema_id, table_id, "dml")) {
					return true;
				}
			}
			continue;
		}
		int64_t schema_id = 0;
		bool has_table = false;
		if (DdlTokenInfo(conn, catalog_name, snapshot_id, token, schema_id, table_id, has_table)) {
			for (const auto &subscription : subscriptions) {
				if (has_table && SubscriptionCoversTable(subscription, schema_id, table_id, "ddl")) {
					return true;
				}
				if (!has_table && subscription.event_category == "ddl" && subscription.scope_kind == "catalog") {
					return true;
				}
			}
		}
	}
	return false;
}

int64_t TimeoutMsParameter(duckdb::TableFunctionBindInput &input);

duckdb::Value BigIntListValue(const std::vector<int64_t> &values) {
	duckdb::vector<duckdb::Value> children;
	children.reserve(values.size());
	for (const auto value : values) {
		children.push_back(duckdb::Value::BIGINT(value));
	}
	return duckdb::Value::LIST(duckdb::LogicalType::BIGINT, children);
}

std::pair<std::string, std::string> SplitQualifiedTableName(const std::string &table_name);
std::string DuckLakeTableChangesCall(const std::string &catalog_name, const std::string &table_name,
                                     int64_t start_snapshot, int64_t end_snapshot);
std::vector<std::string> StringListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name);
std::vector<int64_t> Int64ListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name);

void DmlTicksReturnTypes(duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names,
                         bool stateful) {
	if (stateful) {
		names = {"consumer_name", "start_snapshot", "end_snapshot"};
		return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	} else {
		names.clear();
		return_types.clear();
	}
	for (const auto &name : {"snapshot_id", "snapshot_time", "schema_version", "table_ids"}) {
		names.push_back(name);
	}
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));
}

duckdb::unique_ptr<duckdb::FunctionData> DmlTicksBindBase(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names, bool listen) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_dml_ticks_read/listen requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<DmlTicksData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->max_snapshots = MaxSnapshotsParameter(input);
	result->listen = listen;
	result->timeout_ms = TimeoutMsParameter(input);
	result->poll_min_ms = PollMinMsParameter(input);
	auto coalesce_entry = input.named_parameters.find("coalesce");
	if (coalesce_entry != input.named_parameters.end() && !coalesce_entry->second.IsNull()) {
		result->coalesce = coalesce_entry->second.GetValue<bool>();
	}
	auto auto_commit_entry = input.named_parameters.find("auto_commit");
	if (auto_commit_entry != input.named_parameters.end() && !auto_commit_entry->second.IsNull()) {
		result->auto_commit = auto_commit_entry->second.GetValue<bool>();
	}
	auto start_snapshot_entry = input.named_parameters.find("start_snapshot");
	auto end_snapshot_entry = input.named_parameters.find("end_snapshot");
	const bool has_start_snapshot =
	    start_snapshot_entry != input.named_parameters.end() && !start_snapshot_entry->second.IsNull();
	const bool has_end_snapshot =
	    end_snapshot_entry != input.named_parameters.end() && !end_snapshot_entry->second.IsNull();
	if (has_start_snapshot != has_end_snapshot) {
		throw duckdb::BinderException(
		    "cdc_dml_ticks_read requires both start_snapshot and end_snapshot when either is set");
	}
	if (has_start_snapshot) {
		result->explicit_window = true;
		result->start_snapshot = start_snapshot_entry->second.GetValue<int64_t>();
		result->end_snapshot = end_snapshot_entry->second.GetValue<int64_t>();
	}

	DmlTicksReturnTypes(return_types, names, true);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> DmlTicksReadBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	return DmlTicksBindBase(context, input, return_types, names, false);
}

duckdb::unique_ptr<duckdb::FunctionData> DmlTicksListenBind(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names) {
	return DmlTicksBindBase(context, input, return_types, names, true);
}

void AppendDmlTickRows(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                       int64_t start_snapshot, int64_t end_snapshot,
                       const std::vector<ConsumerSubscriptionRow> *subscriptions,
                       const std::unordered_set<int64_t> *filter_table_ids,
                       std::vector<std::vector<duckdb::Value>> &out, bool stateful) {
	auto rows =
	    conn.Query(std::string("SELECT s.snapshot_id, s.snapshot_time, s.schema_version, c.changes_made FROM ") +
	               MetadataTable(conn, catalog_name, "ducklake_snapshot") + " s JOIN " +
	               MetadataTable(conn, catalog_name, "ducklake_snapshot_changes") +
	               " c USING (snapshot_id) WHERE s.snapshot_id BETWEEN " + std::to_string(start_snapshot) + " AND " +
	               std::to_string(end_snapshot) + " ORDER BY s.snapshot_id ASC");
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, rows ? rows->GetError() : "DML ticks scan failed");
	}
	for (duckdb::idx_t i = 0; i < rows->RowCount(); ++i) {
		const auto changes_value = rows->GetValue(3, i);
		const auto changes = changes_value.IsNull() ? std::string() : changes_value.ToString();
		std::unordered_set<int64_t> touched_table_ids;
		for (const auto &token : SplitEventTokens(changes)) {
			int64_t table_id = 0;
			std::string change_type;
			if (!DmlTokenInfo(token, table_id, change_type)) {
				continue;
			}
			if (filter_table_ids != nullptr && filter_table_ids->count(table_id) == 0) {
				continue;
			}
			if (subscriptions != nullptr) {
				bool covered = false;
				for (const auto &subscription : *subscriptions) {
					if (subscription.event_category == "dml" &&
					    (subscription.scope_kind == "catalog" ||
					     (subscription.scope_kind == "table" && !subscription.table_id.IsNull() &&
					      subscription.table_id.GetValue<int64_t>() == table_id))) {
						covered = true;
						break;
					}
				}
				if (!covered) {
					continue;
				}
			}
			touched_table_ids.insert(table_id);
		}
		if (touched_table_ids.empty()) {
			continue;
		}
		std::vector<int64_t> table_ids(touched_table_ids.begin(), touched_table_ids.end());
		std::sort(table_ids.begin(), table_ids.end());
		std::vector<duckdb::Value> row;
		if (stateful) {
			row.push_back(duckdb::Value(consumer_name));
			row.push_back(duckdb::Value::BIGINT(start_snapshot));
			row.push_back(duckdb::Value::BIGINT(end_snapshot));
		}
		row.push_back(rows->GetValue(0, i));
		row.push_back(rows->GetValue(1, i));
		row.push_back(rows->GetValue(2, i));
		row.push_back(BigIntListValue(table_ids));
		out.push_back(std::move(row));
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DmlTicksInit(duckdb::ClientContext &context,
                                                                  duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<DmlTicksData>();
	auto max_snapshots = data.max_snapshots;
	duckdb::Connection conn(*context.db);
	const auto subscriptions = LoadDmlConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
	if (data.listen && !data.explicit_window) {
		auto ready = WaitForDmlConsumerSnapshot(context, conn, data.catalog_name, data.consumer_name, data.timeout_ms,
		                                        subscriptions, data.poll_min_ms);
		if (ready.empty() || ready[0].IsNull()) {
			return std::move(result);
		}
		if (data.coalesce) {
			MaybeCoalesceConsumerListenWithConnection(context, conn, data.catalog_name, data.consumer_name, "dml_ticks",
			                                          data.timeout_ms, max_snapshots, ready[0].GetValue<int64_t>());
		}
		if (ready.size() > 1 && !ready[1].IsNull()) {
			max_snapshots = std::max<int64_t>(max_snapshots, ready[1].GetValue<int64_t>());
		}
	}

	int64_t start_snapshot;
	int64_t end_snapshot;
	bool has_changes;
	if (data.explicit_window) {
		start_snapshot = data.start_snapshot;
		end_snapshot = data.end_snapshot;
		has_changes = end_snapshot >= start_snapshot;
	} else {
		CdcWindowData window_data;
		window_data.catalog_name = data.catalog_name;
		window_data.consumer_name = data.consumer_name;
		window_data.max_snapshots = max_snapshots;
		auto window = ReadWindowWithConnection(context, conn, window_data);
		start_snapshot = window[0].GetValue<int64_t>();
		end_snapshot = window[1].GetValue<int64_t>();
		has_changes = window[2].GetValue<bool>();
	}
	if (!has_changes) {
		return std::move(result);
	}
	AppendDmlTickRows(conn, data.catalog_name, data.consumer_name, start_snapshot, end_snapshot, &subscriptions,
	                  nullptr, result->rows, true);
	if (data.listen && !data.explicit_window) {
		RecordConsumerListenResult(data.catalog_name, data.consumer_name, "dml_ticks", !result->rows.empty(),
		                           start_snapshot, end_snapshot, static_cast<int64_t>(result->rows.size()),
		                           max_snapshots);
	}
	if (!data.explicit_window) {
		// Same auto-advance contract as cdc_dml_changes_listen: drain
		// non-terminal empty windows so the cursor can't get pinned on a
		// snapshot the consumer has nothing to do for. Terminal windows
		// (has_changes = false from ReadWindow) are not auto-advanced.
		if (data.auto_commit || (data.listen && has_changes && result->rows.empty())) {
			CommitConsumerSnapshot(context, data.catalog_name, data.consumer_name, end_snapshot);
		}
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Helpers shared by the stateless `cdc_dml_changes_query` and
// `cdc_dml_ticks_query` paths (which still take LIST of tables for ad-hoc
// lookback). Stateful read/listen call sites resolve the table from the
// consumer subscription, so they don't need these helpers.
//===--------------------------------------------------------------------===//

std::vector<std::string> StringListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name) {
	std::vector<std::string> out;
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return out;
	}
	for (const auto &child : duckdb::ListValue::GetChildren(entry->second)) {
		if (!child.IsNull()) {
			out.push_back(child.GetValue<std::string>());
		}
	}
	return out;
}

std::vector<int64_t> Int64ListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name) {
	std::vector<int64_t> out;
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return out;
	}
	for (const auto &child : duckdb::ListValue::GetChildren(entry->second)) {
		if (!child.IsNull()) {
			out.push_back(child.GetValue<int64_t>());
		}
	}
	return out;
}

//===--------------------------------------------------------------------===//
// DML changes (typed, single-table — the only DML row-level read API)
//===--------------------------------------------------------------------===//

struct CdcChangesData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	std::string table_name;
	int64_t table_id = -1;
	int64_t schema_id = -1;
	int64_t max_snapshots;
	bool explicit_window = false;
	int64_t start_snapshot = -1;
	int64_t end_snapshot = -1;
	int64_t probe_schema_version = -1;
	std::vector<std::string> change_types;
	bool auto_commit = false;
	bool listen = false;
	int64_t timeout_ms = DEFAULT_WAIT_TIMEOUT_MS;
	int64_t poll_min_ms = WAIT_INITIAL_INTERVAL_MS;
	bool coalesce = true;
	// Column names + types belonging to the underlying DuckLake table (the
	// `<table cols>` segment of `table_changes(...)` output, after the
	// fixed `(snapshot_id, rowid, change_type)` prefix). Discovered during
	// Bind via a `LIMIT 0` probe so that the bind result can carry an
	// accurate schema for the planner.
	std::vector<std::string> table_column_names;
	std::vector<duckdb::LogicalType> table_column_types;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcChangesData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->table_name = table_name;
		result->table_id = table_id;
		result->schema_id = schema_id;
		result->max_snapshots = max_snapshots;
		result->explicit_window = explicit_window;
		result->start_snapshot = start_snapshot;
		result->end_snapshot = end_snapshot;
		result->probe_schema_version = probe_schema_version;
		result->change_types = change_types;
		result->auto_commit = auto_commit;
		result->listen = listen;
		result->timeout_ms = timeout_ms;
		result->poll_min_ms = poll_min_ms;
		result->coalesce = coalesce;
		result->table_column_names = table_column_names;
		result->table_column_types = table_column_types;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

struct TableChangesSchemaCacheEntry {
	std::vector<std::string> column_names;
	std::vector<duckdb::LogicalType> column_types;
};

std::mutex TABLE_CHANGES_SCHEMA_CACHE_MUTEX;
std::unordered_map<std::string, TableChangesSchemaCacheEntry> TABLE_CHANGES_SCHEMA_CACHE;

struct DmlSegmentCacheEntry {
	std::shared_ptr<duckdb::MaterializedQueryResult> segment;
	bool ready = false;
	bool failed = false;
	uint64_t last_used = 0;
};

struct DmlSegmentScanState : public duckdb::GlobalTableFunctionState {
	std::shared_ptr<duckdb::MaterializedQueryResult> segment;
	std::string consumer_name;
	int64_t start_snapshot = -1;
	int64_t end_snapshot = -1;
	duckdb::idx_t offset = 0;
};

std::mutex DML_SEGMENT_CACHE_MUTEX;
std::condition_variable DML_SEGMENT_CACHE_CV;
std::unordered_map<std::string, std::shared_ptr<DmlSegmentCacheEntry>> DML_SEGMENT_CACHE;
uint64_t DML_SEGMENT_CACHE_CLOCK = 0;

std::string TableChangesSchemaCacheKey(const std::string &attachment_key, const std::string &catalog_name,
                                       int64_t table_id, int64_t schema_version) {
	return attachment_key + ":" + catalog_name + ":" + std::to_string(table_id) + ":" + std::to_string(schema_version);
}

std::string DmlSegmentCacheKey(const std::string &attachment_key, const CdcChangesData &data,
                               const std::string &scan_table_name, int64_t start_snapshot, int64_t end_snapshot) {
	std::ostringstream key;
	key << attachment_key << ":" << data.catalog_name << ":" << data.table_id << ":" << scan_table_name << ":"
	    << start_snapshot << ":" << end_snapshot << ":" << data.probe_schema_version;
	for (const auto &change_type : data.change_types) {
		key << ":" << change_type;
	}
	return key.str();
}

std::shared_ptr<DmlSegmentCacheEntry> ReserveDmlSegment(const std::string &cache_key, bool &build_segment) {
	std::unique_lock<std::mutex> guard(DML_SEGMENT_CACHE_MUTEX);
	for (;;) {
		auto existing = DML_SEGMENT_CACHE.find(cache_key);
		if (existing == DML_SEGMENT_CACHE.end()) {
			auto entry = std::make_shared<DmlSegmentCacheEntry>();
			entry->last_used = ++DML_SEGMENT_CACHE_CLOCK;
			DML_SEGMENT_CACHE[cache_key] = entry;
			build_segment = true;
			return entry;
		}
		auto entry = existing->second;
		if (entry->ready) {
			entry->last_used = ++DML_SEGMENT_CACHE_CLOCK;
			build_segment = false;
			return entry;
		}
		DML_SEGMENT_CACHE_CV.wait(guard);
		if (entry->ready) {
			entry->last_used = ++DML_SEGMENT_CACHE_CLOCK;
			build_segment = false;
			return entry;
		}
		if (entry->failed) {
			DML_SEGMENT_CACHE.erase(cache_key);
		}
	}
}

void PublishDmlSegment(const std::string &cache_key, const std::shared_ptr<DmlSegmentCacheEntry> &entry,
                       std::shared_ptr<duckdb::MaterializedQueryResult> segment) {
	std::lock_guard<std::mutex> guard(DML_SEGMENT_CACHE_MUTEX);
	entry->segment = std::move(segment);
	entry->ready = true;
	entry->last_used = ++DML_SEGMENT_CACHE_CLOCK;
	// Keep the cache as an in-flight coalescing map only. Retaining DuckDB
	// MaterializedQueryResult objects in process-global state can outlive the
	// database instance in the unittest host and crash during shutdown.
	DML_SEGMENT_CACHE.erase(cache_key);
	DML_SEGMENT_CACHE_CV.notify_all();
}

void FailDmlSegment(const std::string &cache_key, const std::shared_ptr<DmlSegmentCacheEntry> &entry) {
	std::lock_guard<std::mutex> guard(DML_SEGMENT_CACHE_MUTEX);
	entry->failed = true;
	DML_SEGMENT_CACHE.erase(cache_key);
	DML_SEGMENT_CACHE_CV.notify_all();
}

void DmlSegmentScanExecute(duckdb::ClientContext &context, duckdb::TableFunctionInput &input,
                           duckdb::DataChunk &output) {
	auto &state = input.global_state->Cast<DmlSegmentScanState>();
	if (!state.segment || state.offset >= state.segment->RowCount()) {
		return;
	}
	duckdb::idx_t count = 0;
	while (state.offset < state.segment->RowCount() && count < STANDARD_VECTOR_SIZE) {
		if (state.segment->ColumnCount() + 3 != output.ColumnCount()) {
			throw duckdb::InternalException("Unaligned cached DML segment row in table function result");
		}
		output.SetValue(0, count, duckdb::Value(state.consumer_name));
		output.SetValue(1, count, duckdb::Value::BIGINT(state.start_snapshot));
		output.SetValue(2, count, duckdb::Value::BIGINT(state.end_snapshot));
		for (duckdb::idx_t col = 0; col < state.segment->ColumnCount(); ++col) {
			output.SetValue(col + 3, count, state.segment->GetValue(col, state.offset));
		}
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DmlChangesReturnTypes(const CdcChangesData &data, duckdb::vector<duckdb::LogicalType> &return_types,
                           duckdb::vector<duckdb::string> &names) {
	names = {"consumer_name", "start_snapshot", "end_snapshot", "snapshot_id",
	         "rowid",         "change_type",    "table_id",     "table_name"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR};
	for (duckdb::idx_t i = 0; i < data.table_column_names.size(); ++i) {
		names.push_back(data.table_column_names[i]);
		return_types.push_back(data.table_column_types[i]);
	}
	for (const auto &extra_name : {"snapshot_time", "author", "commit_message", "commit_extra_info"}) {
		names.push_back(extra_name);
	}
	return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
}

std::pair<std::string, std::string> SplitQualifiedTableName(const std::string &table_name) {
	const auto dot = table_name.find('.');
	if (dot == std::string::npos) {
		return {"main", table_name};
	}
	return {table_name.substr(0, dot), table_name.substr(dot + 1)};
}

std::string DuckLakeTableChangesCall(const std::string &catalog_name, const std::string &table_name,
                                     int64_t start_snapshot, int64_t end_snapshot) {
	const auto parts = SplitQualifiedTableName(table_name);
	return "ducklake_table_changes(" + QuoteLiteral(catalog_name) + ", " + QuoteLiteral(parts.first) + ", " +
	       QuoteLiteral(parts.second) + ", " + std::to_string(start_snapshot) + ", " + std::to_string(end_snapshot) +
	       ")";
}

int64_t TimeoutMsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("timeout_ms");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return DEFAULT_WAIT_TIMEOUT_MS;
	}
	return entry->second.GetValue<int64_t>();
}

//! Read the consumer's single subscribed table out of its subscriptions.
//! One DML consumer = one table by contract, so this resolves
//! deterministically; it does NOT accept caller overrides. The
//! per-call API is intentionally narrow: the table identity is fixed at
//! `cdc_dml_consumer_create` time and follows renames automatically via
//! the subscription rows.
void ResolveSubscribedTable(const std::vector<ConsumerSubscriptionRow> &subscriptions, const std::string &consumer_name,
                            int64_t &schema_id, int64_t &table_id, std::string &table_name) {
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category == "dml" && subscription.scope_kind == "table" &&
		    !subscription.table_id.IsNull()) {
			table_id = subscription.table_id.GetValue<int64_t>();
			if (!subscription.schema_id.IsNull()) {
				schema_id = subscription.schema_id.GetValue<int64_t>();
			}
			if (!subscription.current_qualified_name.IsNull()) {
				table_name = subscription.current_qualified_name.ToString();
			} else if (!subscription.original_qualified_name.IsNull()) {
				table_name = subscription.original_qualified_name.ToString();
			}
			return;
		}
	}
	throw duckdb::InvalidInputException(
	    "cdc_dml_changes_read: consumer '%s' has no DML table subscription (use cdc_dml_consumer_create)",
	    consumer_name);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcChangesBindBase(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names, bool listen) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_dml_changes_read requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->listen = listen;
	result->timeout_ms = TimeoutMsParameter(input);
	result->poll_min_ms = PollMinMsParameter(input);
	auto coalesce_entry = input.named_parameters.find("coalesce");
	if (coalesce_entry != input.named_parameters.end() && !coalesce_entry->second.IsNull()) {
		result->coalesce = coalesce_entry->second.GetValue<bool>();
	}
	auto auto_commit_entry = input.named_parameters.find("auto_commit");
	if (auto_commit_entry != input.named_parameters.end() && !auto_commit_entry->second.IsNull()) {
		result->auto_commit = auto_commit_entry->second.GetValue<bool>();
	}
	result->max_snapshots = MaxSnapshotsParameter(input);
	auto start_snapshot_entry = input.named_parameters.find("start_snapshot");
	auto end_snapshot_entry = input.named_parameters.find("end_snapshot");
	const bool has_start_snapshot =
	    start_snapshot_entry != input.named_parameters.end() && !start_snapshot_entry->second.IsNull();
	const bool has_end_snapshot =
	    end_snapshot_entry != input.named_parameters.end() && !end_snapshot_entry->second.IsNull();
	if (has_start_snapshot != has_end_snapshot) {
		throw duckdb::BinderException(
		    "cdc_dml_changes_read requires both start_snapshot and end_snapshot when either is set");
	}
	if (has_start_snapshot) {
		result->explicit_window = true;
		result->start_snapshot = start_snapshot_entry->second.GetValue<int64_t>();
		result->end_snapshot = end_snapshot_entry->second.GetValue<int64_t>();
	}

	// Probe the underlying `<lake>.table_changes(...)` schema with a `LIMIT 0`
	// query so the planner sees the table's actual columns. `table_changes`
	// projects the schema as of the END snapshot of the range, so the probe
	// snapshot must match the END snapshot the runtime scan will use.
	//
	// We replicate `ReadWindow`'s window-end computation read-only here
	// (no lease acquisition, no metadata writes). Two simpler strategies that
	// don't work:
	//
	//   * `last_committed_snapshot + 1` (the historical strategy): assumes
	//     snapshot ids are dense and ignores `max_snapshots` and schema-boundary
	//     truncation. Bind must match the exact runtime end snapshot instead.
	//
	//   * `current_snapshot()` (intermediate fix): correct when the runtime
	//     range extends to the latest snapshot, wrong when the window is
	//     bounded BEFORE a subscribed-table schema-shape boundary. DML
	//     consumers terminate before that boundary; the runtime range
	//     projects the OLD schema, so bind would over-project NEW columns
	//     and the runtime query would fail to bind `tc.<new column>`.
	//
	// Trade-off that remains: a producer ALTER landing between Bind and Init
	// can still drift the runtime schema past the bind columns. The
	// single-reader lease keeps this as a strictly producer/consumer race;
	// fixing it would require rebinding on every Init.
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, result->catalog_name);
	const auto subscriptions = LoadConsumerSubscriptions(conn, result->catalog_name, result->consumer_name);
	ResolveSubscribedTable(subscriptions, result->consumer_name, result->schema_id, result->table_id,
	                       result->table_name);
	const auto matching_change_types = MatchingDmlChangeTypes(subscriptions, result->schema_id, result->table_id);
	if (matching_change_types.empty()) {
		throw duckdb::InvalidInputException(
		    "cdc_dml_changes_read: table '%s' is not covered by consumer '%s' subscriptions", result->table_name,
		    result->consumer_name);
	}
	result->change_types = matching_change_types;
	int64_t probe_snapshot;
	if (result->explicit_window) {
		probe_snapshot = result->end_snapshot >= result->start_snapshot ? result->end_snapshot : result->start_snapshot;
	} else {
		auto consumer_row = LoadConsumerOrThrow(conn, result->catalog_name, result->consumer_name);
		const auto last_snapshot = consumer_row.last_committed_snapshot;
		const auto current_snapshot = CurrentSnapshot(conn, result->catalog_name);
		const auto first_snapshot = FirstSnapshotAfter(conn, result->catalog_name, last_snapshot, current_snapshot);
		if (first_snapshot == -1) {
			// Consumer is caught up to head (or to the last external snap):
			// the next runtime range will be empty. Bind to the latest
			// known schema so a brand-new consumer or a fully-drained
			// consumer can still resolve columns.
			probe_snapshot = current_snapshot > 0 ? current_snapshot : CurrentSnapshot(conn, result->catalog_name);
		} else {
			const auto start_snapshot = first_snapshot;
			int64_t end_snapshot = std::min(current_snapshot, start_snapshot + result->max_snapshots - 1);
			if (start_snapshot <= current_snapshot) {
				// DML consumers are pinned to the schema shape of their
				// subscribed tables, so the bind probe must collapse at
				// the same boundary `cdc_window` does at runtime.
				const auto dml_subscriptions =
				    LoadDmlConsumerSubscriptions(conn, result->catalog_name, result->consumer_name);
				const auto dml_boundary = NextDmlSubscribedSchemaChangeSnapshot(
				    conn, result->catalog_name, last_snapshot, current_snapshot, dml_subscriptions);
				if (dml_boundary != -1 && dml_boundary <= end_snapshot) {
					end_snapshot = dml_boundary - 1;
				}
			}
			// `end_snapshot < start_snapshot` is the boundary-collapse case
			// (the consumer is parked one step before the subscribed-table
			// schema change). The runtime range will be empty; probe at
			// `start` so bind resolves under the schema that the *next*
			// non-empty window will see.
			probe_snapshot = end_snapshot >= start_snapshot ? end_snapshot : start_snapshot;
		}
	}
	if (!result->explicit_window) {
		// Renames track via subscriptions, so resolve the *current*
		// qualified name for the pinned table_id at the probe snapshot
		// when it differs from what we read out of the subscription
		// snapshot. Falling back to the latest catalog snapshot (where
		// the consumer is presumed to be heading) keeps the planner's
		// projected column list aligned with the runtime schema.
		const auto table_name_at_probe =
		    CurrentQualifiedTableName(conn, result->catalog_name, result->table_id, probe_snapshot);
		if (table_name_at_probe.empty() || table_name_at_probe != result->table_name) {
			probe_snapshot = CurrentSnapshot(conn, result->catalog_name);
		}
	}
	result->probe_schema_version = ResolveSchemaVersion(conn, result->catalog_name, probe_snapshot);
	const auto schema_cache_key =
	    TableChangesSchemaCacheKey(MetadataAttachmentCacheKey(conn, result->catalog_name), result->catalog_name,
	                               result->table_id, result->probe_schema_version);
	{
		std::lock_guard<std::mutex> guard(TABLE_CHANGES_SCHEMA_CACHE_MUTEX);
		const auto entry = TABLE_CHANGES_SCHEMA_CACHE.find(schema_cache_key);
		if (entry != TABLE_CHANGES_SCHEMA_CACHE.end()) {
			result->table_column_names = entry->second.column_names;
			result->table_column_types = entry->second.column_types;
		}
	}
	if (result->table_column_names.empty() && result->table_column_types.empty()) {
		auto probe = conn.Query(
		    "SELECT * FROM " +
		    DuckLakeTableChangesCall(result->catalog_name, result->table_name, probe_snapshot, probe_snapshot) +
		    " LIMIT 0");
		if (!probe || probe->HasError()) {
			throw duckdb::InvalidInputException("cdc_dml_changes_read failed to resolve table '%s' in catalog '%s': %s",
			                                    result->table_name, result->catalog_name,
			                                    probe ? probe->GetError()
			                                          : std::string("table_changes probe returned no result"));
		}
		if (probe->ColumnCount() < 3) {
			throw duckdb::InvalidInputException("cdc_dml_changes_read: unexpected table_changes schema for table "
			                                    "'%s' (got %u columns; expected at least 3)",
			                                    result->table_name, static_cast<unsigned>(probe->ColumnCount()));
		}
		for (duckdb::idx_t i = 3; i < probe->ColumnCount(); ++i) {
			result->table_column_names.push_back(probe->ColumnName(i));
			result->table_column_types.push_back(probe->types[i]);
		}
		TableChangesSchemaCacheEntry cache_entry;
		cache_entry.column_names = result->table_column_names;
		cache_entry.column_types = result->table_column_types;
		std::lock_guard<std::mutex> guard(TABLE_CHANGES_SCHEMA_CACHE_MUTEX);
		TABLE_CHANGES_SCHEMA_CACHE[schema_cache_key] = std::move(cache_entry);
	}
	DmlChangesReturnTypes(*result, return_types, names);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcChangesReadBind(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names) {
	return CdcChangesBindBase(context, input, return_types, names, false);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcChangesListenBind(duckdb::ClientContext &context,
                                                              duckdb::TableFunctionBindInput &input,
                                                              duckdb::vector<duckdb::LogicalType> &return_types,
                                                              duckdb::vector<duckdb::string> &names) {
	return CdcChangesBindBase(context, input, return_types, names, true);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcChangesInit(duckdb::ClientContext &context,
                                                                    duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<DmlSegmentScanState>();
	auto &data = input.bind_data->Cast<CdcChangesData>();
	auto max_snapshots = data.max_snapshots;
	duckdb::Connection conn(*context.db);
	const auto subscriptions = data.listen && !data.explicit_window
	                               ? LoadDmlConsumerSubscriptions(conn, data.catalog_name, data.consumer_name)
	                               : std::vector<ConsumerSubscriptionRow>();
	if (data.listen && !data.explicit_window) {
		auto ready = WaitForDmlConsumerSnapshot(context, conn, data.catalog_name, data.consumer_name, data.timeout_ms,
		                                        subscriptions, data.poll_min_ms);
		if (ready.empty() || ready[0].IsNull()) {
			return std::move(result);
		}
		if (data.coalesce) {
			MaybeCoalesceConsumerListenWithConnection(context, conn, data.catalog_name, data.consumer_name,
			                                          "dml_changes", data.timeout_ms, max_snapshots,
			                                          ready[0].GetValue<int64_t>());
		}
		if (ready.size() > 1 && !ready[1].IsNull()) {
			max_snapshots = std::max<int64_t>(max_snapshots, ready[1].GetValue<int64_t>());
		}
	}

	int64_t start_snapshot;
	int64_t end_snapshot;
	bool has_changes;
	if (data.explicit_window) {
		start_snapshot = data.start_snapshot;
		end_snapshot = data.end_snapshot;
		has_changes = end_snapshot >= start_snapshot;
	} else {
		CdcWindowData window_data;
		window_data.catalog_name = data.catalog_name;
		window_data.consumer_name = data.consumer_name;
		window_data.max_snapshots = max_snapshots;
		auto window = ReadWindowWithConnection(context, conn, window_data);
		start_snapshot = window[0].GetValue<int64_t>();
		end_snapshot = window[1].GetValue<int64_t>();
		has_changes = window[2].GetValue<bool>();
	}
	if (!has_changes) {
		return std::move(result);
	}

	if (data.change_types.empty()) {
		return std::move(result);
	}
	auto scan_table_name = data.table_name;
	const auto table_name_at_end = CurrentQualifiedTableName(conn, data.catalog_name, data.table_id, end_snapshot);
	if (!table_name_at_end.empty()) {
		scan_table_name = table_name_at_end;
	}

	std::ostringstream column_list;
	// `table_name` is the *current* qualified name at end_snapshot; it
	// is constant per row in this read but exposed on each row so a
	// downstream sink (or join) doesn't need a separate metadata
	// lookup. `table_id` is similarly self-describing for orchestrators.
	column_list << "tc.snapshot_id, tc.rowid, tc.change_type, " << std::to_string(data.table_id) << " AS table_id, "
	            << QuoteLiteral(scan_table_name) << " AS table_name";
	for (const auto &col_name : data.table_column_names) {
		column_list << ", tc." << QuoteIdentifier(col_name);
	}

	std::ostringstream where_clause;
	if (!data.change_types.empty()) {
		where_clause << " WHERE tc.change_type IN (";
		for (size_t i = 0; i < data.change_types.size(); ++i) {
			if (i > 0) {
				where_clause << ", ";
			}
			where_clause << QuoteLiteral(data.change_types[i]);
		}
		where_clause << ")";
	}

	const auto snapshot_table = MetadataTable(conn, data.catalog_name, "ducklake_snapshot");
	const auto changes_table = MetadataTable(conn, data.catalog_name, "ducklake_snapshot_changes");
	const auto cache_key = DmlSegmentCacheKey(MetadataAttachmentCacheKey(conn, data.catalog_name), data,
	                                          scan_table_name, start_snapshot, end_snapshot);
	bool build_segment = false;
	auto cache_entry = ReserveDmlSegment(cache_key, build_segment);
	if (!build_segment) {
		result->segment = cache_entry->segment;
		result->consumer_name = data.consumer_name;
		result->start_snapshot = start_snapshot;
		result->end_snapshot = end_snapshot;
		const auto row_count = result->segment ? static_cast<int64_t>(result->segment->RowCount()) : 0;
		if (data.listen && !data.explicit_window) {
			RecordConsumerListenResult(data.catalog_name, data.consumer_name, "dml_changes", row_count > 0,
			                           start_snapshot, end_snapshot, row_count, max_snapshots);
		}
		if (!data.explicit_window) {
			if (data.auto_commit || (data.listen && has_changes && row_count == 0)) {
				CommitConsumerSnapshotWithConnection(context, conn, data.catalog_name, data.consumer_name,
				                                     end_snapshot);
			}
		}
		return std::move(result);
	}

	auto query = std::string("WITH tc AS MATERIALIZED (SELECT * FROM ") +
	             DuckLakeTableChangesCall(data.catalog_name, scan_table_name, start_snapshot, end_snapshot) +
	             "), snapshot_meta AS MATERIALIZED ("
	             "SELECT ids.snapshot_id, s.snapshot_time, c.author, c.commit_message, c.commit_extra_info "
	             "FROM (SELECT DISTINCT snapshot_id FROM tc) ids LEFT JOIN " +
	             snapshot_table + " s ON s.snapshot_id = ids.snapshot_id LEFT JOIN " + changes_table +
	             " c ON c.snapshot_id = ids.snapshot_id) SELECT " + column_list.str() +
	             ", sm.snapshot_time, sm.author, sm.commit_message, sm.commit_extra_info FROM tc LEFT JOIN "
	             "snapshot_meta sm ON sm.snapshot_id = tc.snapshot_id" +
	             where_clause.str() + " ORDER BY tc.snapshot_id ASC, tc.rowid ASC";
	std::shared_ptr<duckdb::MaterializedQueryResult> segment;
	try {
		auto rows = conn.Query(query);
		if (!rows || rows->HasError()) {
			const auto error = rows ? rows->GetError() : std::string("cdc_dml_changes_read scan failed");
			throw duckdb::Exception(duckdb::ExceptionType::INVALID, error);
		}
		segment = std::shared_ptr<duckdb::MaterializedQueryResult>(rows.release());
		PublishDmlSegment(cache_key, cache_entry, segment);
	} catch (std::exception &ex) {
		FailDmlSegment(cache_key, cache_entry);
		throw;
	}
	const auto row_count = static_cast<int64_t>(segment->RowCount());
	result->segment = std::move(segment);
	result->consumer_name = data.consumer_name;
	result->start_snapshot = start_snapshot;
	result->end_snapshot = end_snapshot;
	if (data.listen && !data.explicit_window) {
		RecordConsumerListenResult(data.catalog_name, data.consumer_name, "dml_changes", row_count > 0, start_snapshot,
		                           end_snapshot, row_count, max_snapshots);
	}
	if (!data.explicit_window) {
		// Auto-advance through windows that report has_changes=true but
		// produce zero rows for this consumer's pinned table — those
		// snapshots only touched unsubscribed tables (e.g. a sibling
		// table's INSERT or an unrelated ALTER). Terminal windows have
		// has_changes=false from ReadWindow and are NOT auto-advanced;
		// the cursor stays parked at the boundary so cdc_commit raises
		// CDC_SCHEMA_TERMINATED as documented.
		if (data.auto_commit || (data.listen && has_changes && row_count == 0)) {
			CommitConsumerSnapshotWithConnection(context, conn, data.catalog_name, data.consumer_name, end_snapshot);
		}
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Stateless range helpers (used by cdc_dml_changes_query, cdc_dml_ticks_query)
//===--------------------------------------------------------------------===//

int64_t RangeToSnapshotParameter(duckdb::Connection &conn, duckdb::TableFunctionBindInput &input,
                                 const std::string &catalog_name, duckdb::idx_t positional_index) {
	if (input.inputs.size() > positional_index) {
		if (input.inputs[positional_index].IsNull()) {
			return CurrentSnapshot(conn, catalog_name);
		}
		return input.inputs[positional_index].GetValue<int64_t>();
	}
	auto entry = input.named_parameters.find("to_snapshot");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return CurrentSnapshot(conn, catalog_name);
	}
	return entry->second.GetValue<int64_t>();
}

void ValidateRangeBounds(duckdb::Connection &conn, const std::string &catalog_name, int64_t from_snapshot,
                         int64_t to_snapshot, const std::string &feature_name) {
	if (to_snapshot < from_snapshot) {
		throw duckdb::InvalidInputException("%s: from_snapshot must be <= to_snapshot", feature_name);
	}
	auto oldest_result = conn.Query("SELECT COALESCE(min(snapshot_id), 0) FROM " +
	                                MetadataTable(conn, catalog_name, "ducklake_snapshot"));
	const auto oldest_snapshot = SingleInt64(*oldest_result, "oldest available snapshot");
	if (from_snapshot < oldest_snapshot) {
		throw duckdb::InvalidInputException("%s: from_snapshot %lld is older than oldest available snapshot %lld",
		                                    feature_name, static_cast<long long>(from_snapshot),
		                                    static_cast<long long>(oldest_snapshot));
	}
}

duckdb::unique_ptr<duckdb::FunctionData> DmlTicksQueryBind(duckdb::ClientContext &context,
                                                           duckdb::TableFunctionBindInput &input,
                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                           duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2 && input.inputs.size() != 3) {
		throw duckdb::BinderException("cdc_dml_ticks_query requires catalog, from_snapshot, and optional to_snapshot");
	}
	auto result = duckdb::make_uniq<DmlTicksData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->from_snapshot = input.inputs[1].GetValue<int64_t>();
	result->stateless = true;
	result->table_ids = Int64ListNamedParameter(input, "table_ids");
	result->table_names = StringListNamedParameter(input, "table_names");
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, result->catalog_name);
	result->to_snapshot = RangeToSnapshotParameter(conn, input, result->catalog_name, 2);
	ValidateRangeBounds(conn, result->catalog_name, result->from_snapshot, result->to_snapshot, "cdc_dml_ticks_query");
	DmlTicksReturnTypes(return_types, names, false);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DmlTicksQueryInit(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<DmlTicksData>();
	duckdb::Connection conn(*context.db);
	std::unordered_set<int64_t> filter_table_ids(data.table_ids.begin(), data.table_ids.end());
	for (const auto &table_name_input : data.table_names) {
		auto table_name = table_name_input.find('.') == std::string::npos ? std::string("main.") + table_name_input
		                                                                  : table_name_input;
		int64_t schema_id = 0;
		int64_t table_id = 0;
		if (!ResolveCurrentTableName(conn, data.catalog_name, table_name, data.to_snapshot, schema_id, table_id)) {
			throw duckdb::InvalidInputException("cdc_dml_ticks_query failed to resolve table '%s'", table_name);
		}
		filter_table_ids.insert(table_id);
	}
	const auto *filter = filter_table_ids.empty() ? nullptr : &filter_table_ids;
	AppendDmlTickRows(conn, data.catalog_name, std::string(), data.from_snapshot, data.to_snapshot, nullptr, filter,
	                  result->rows, false);
	return std::move(result);
}

struct CdcRangeChangesData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string table_name;
	int64_t table_id = -1;
	int64_t from_snapshot;
	int64_t to_snapshot;
	bool allow_history_fallback = false;
	std::vector<std::string> table_column_names;
	std::vector<duckdb::LogicalType> table_column_types;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcRangeChangesData>();
		result->catalog_name = catalog_name;
		result->table_name = table_name;
		result->table_id = table_id;
		result->from_snapshot = from_snapshot;
		result->to_snapshot = to_snapshot;
		result->allow_history_fallback = allow_history_fallback;
		result->table_column_names = table_column_names;
		result->table_column_types = table_column_types;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcRangeChangesBind(duckdb::ClientContext &context,
                                                             duckdb::TableFunctionBindInput &input,
                                                             duckdb::vector<duckdb::LogicalType> &return_types,
                                                             duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2 && input.inputs.size() != 3) {
		throw duckdb::BinderException(
		    "cdc_dml_changes_query requires catalog, from_snapshot, and optional to_snapshot");
	}
	auto result = duckdb::make_uniq<CdcRangeChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->from_snapshot = input.inputs[1].GetValue<int64_t>();
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, result->catalog_name);
	result->to_snapshot = RangeToSnapshotParameter(conn, input, result->catalog_name, 2);
	ValidateRangeBounds(conn, result->catalog_name, result->from_snapshot, result->to_snapshot,
	                    "cdc_dml_changes_query");

	auto table_id_entry = input.named_parameters.find("table_id");
	auto table_name_entry = input.named_parameters.find("table_name");
	if (table_id_entry != input.named_parameters.end() && !table_id_entry->second.IsNull()) {
		result->table_id = table_id_entry->second.GetValue<int64_t>();
		result->table_name =
		    CurrentQualifiedTableName(conn, result->catalog_name, result->table_id, result->to_snapshot);
		if (result->table_name.empty()) {
			throw duckdb::InvalidInputException("cdc_dml_changes_query: table_id %lld is not active at to_snapshot",
			                                    static_cast<long long>(result->table_id));
		}
	} else if (table_name_entry != input.named_parameters.end() && !table_name_entry->second.IsNull()) {
		result->table_name = table_name_entry->second.GetValue<std::string>();
		result->allow_history_fallback = true;
		if (result->table_name.find('.') == std::string::npos) {
			result->table_name = std::string("main.") + result->table_name;
		}
		int64_t schema_id = 0;
		if (!ResolveCurrentTableName(conn, result->catalog_name, result->table_name, result->to_snapshot, schema_id,
		                             result->table_id)) {
			throw duckdb::InvalidInputException("cdc_dml_changes_query failed to resolve table '%s'",
			                                    result->table_name);
		}
	} else {
		throw duckdb::BinderException("cdc_dml_changes_query requires table_id or table_name");
	}

	auto probe = conn.Query(
	    "SELECT * FROM " +
	    DuckLakeTableChangesCall(result->catalog_name, result->table_name, result->to_snapshot, result->to_snapshot) +
	    " LIMIT 0");
	if (!probe || probe->HasError()) {
		throw duckdb::InvalidInputException(
		    "cdc_dml_changes_query failed to resolve table '%s' in catalog '%s': %s", result->table_name,
		    result->catalog_name, probe ? probe->GetError() : std::string("table_changes probe returned no result"));
	}
	if (probe->ColumnCount() < 3) {
		throw duckdb::InvalidInputException("cdc_dml_changes_query: unexpected table_changes schema for table "
		                                    "'%s' (got %u columns; expected at least 3)",
		                                    result->table_name, static_cast<unsigned>(probe->ColumnCount()));
	}
	for (duckdb::idx_t i = 3; i < probe->ColumnCount(); ++i) {
		result->table_column_names.push_back(probe->ColumnName(i));
		result->table_column_types.push_back(probe->types[i]);
	}

	names = {"snapshot_id", "rowid", "change_type"};
	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR};
	for (duckdb::idx_t i = 0; i < result->table_column_names.size(); ++i) {
		names.push_back(result->table_column_names[i]);
		return_types.push_back(result->table_column_types[i]);
	}
	for (const auto &extra : {"snapshot_time", "author", "commit_message", "commit_extra_info"}) {
		names.push_back(extra);
	}
	return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcRangeChangesInit(duckdb::ClientContext &context,
                                                                         duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcRangeChangesData>();
	duckdb::Connection conn(*context.db);

	std::ostringstream column_list;
	column_list << "tc.snapshot_id, tc.rowid, tc.change_type";
	for (const auto &col_name : data.table_column_names) {
		column_list << ", tc." << QuoteIdentifier(col_name);
	}
	auto scan_range = [&](int64_t from_snapshot) {
		auto query = std::string("SELECT ") + column_list.str() +
		             ", s.snapshot_time, c.author, c.commit_message, c.commit_extra_info FROM " +
		             DuckLakeTableChangesCall(data.catalog_name, data.table_name, from_snapshot, data.to_snapshot) +
		             " tc LEFT JOIN " + MetadataTable(conn, data.catalog_name, "ducklake_snapshot") +
		             " s ON s.snapshot_id = tc.snapshot_id LEFT JOIN " +
		             MetadataTable(conn, data.catalog_name, "ducklake_snapshot_changes") +
		             " c ON c.snapshot_id = tc.snapshot_id ORDER BY tc.snapshot_id ASC, tc.rowid ASC";
		return conn.Query(query);
	};
	auto rows = scan_range(data.from_snapshot);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_dml_changes_query scan failed");
	}
	if (rows->RowCount() == 0 && data.allow_history_fallback) {
		auto begin_result = conn.Query("SELECT COALESCE(min(begin_snapshot), " + std::to_string(data.from_snapshot) +
		                               ") FROM " + MetadataTable(conn, data.catalog_name, "ducklake_table") +
		                               " WHERE table_id = " + std::to_string(data.table_id));
		if (begin_result && !begin_result->HasError() && begin_result->RowCount() > 0 &&
		    !begin_result->GetValue(0, 0).IsNull()) {
			const auto begin_snapshot = begin_result->GetValue(0, 0).GetValue<int64_t>();
			if (begin_snapshot < data.from_snapshot) {
				rows = scan_range(begin_snapshot);
				if (!rows || rows->HasError()) {
					throw duckdb::Exception(duckdb::ExceptionType::INVALID,
					                        rows ? rows->GetError() : "cdc_dml_changes_query scan failed");
				}
			}
		}
	}
	for (duckdb::idx_t row_idx = 0; row_idx < rows->RowCount(); ++row_idx) {
		std::vector<duckdb::Value> values;
		values.reserve(rows->ColumnCount());
		for (duckdb::idx_t col_idx = 0; col_idx < rows->ColumnCount(); ++col_idx) {
			values.push_back(rows->GetValue(col_idx, row_idx));
		}
		result->rows.push_back(std::move(values));
	}
	return std::move(result);
}

} // namespace

void RegisterDmlFunctions(duckdb::ExtensionLoader &loader) {
	for (const auto &name : {"cdc_dml_ticks_read", "cdc_dml_ticks_listen"}) {
		const auto bind =
		    std::string(name).find("_listen") == std::string::npos ? DmlTicksReadBind : DmlTicksListenBind;
		duckdb::TableFunction events_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, bind, DmlTicksInit);
		events_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		events_function.named_parameters["timeout_ms"] = duckdb::LogicalType::BIGINT;
		events_function.named_parameters["poll_min_ms"] = duckdb::LogicalType::BIGINT;
		events_function.named_parameters["coalesce"] = duckdb::LogicalType::BOOLEAN;
		events_function.named_parameters["auto_commit"] = duckdb::LogicalType::BOOLEAN;
		loader.RegisterFunction(events_function);
	}

	// Single, typed DML row-level read/listen surface. A table-scoped
	// consumer's pinned table is implicit; catalogue-wide consumers are
	// rejected because they have no stable row schema.
	for (const auto &name : {"cdc_dml_changes_read", "cdc_dml_changes_listen"}) {
		const auto bind =
		    std::string(name).find("_listen") == std::string::npos ? CdcChangesReadBind : CdcChangesListenBind;
		duckdb::TableFunction changes_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    DmlSegmentScanExecute, bind, CdcChangesInit);
		changes_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["start_snapshot"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["end_snapshot"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["timeout_ms"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["poll_min_ms"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["coalesce"] = duckdb::LogicalType::BOOLEAN;
		changes_function.named_parameters["auto_commit"] = duckdb::LogicalType::BOOLEAN;
		loader.RegisterFunction(changes_function);
	}

	for (const auto &name : {"cdc_dml_ticks_query"}) {
		duckdb::TableFunction dml_ticks_query_function_2(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
		    RowScanExecute, DmlTicksQueryBind, DmlTicksQueryInit);
		dml_ticks_query_function_2.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		dml_ticks_query_function_2.named_parameters["table_ids"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		dml_ticks_query_function_2.named_parameters["table_names"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		loader.RegisterFunction(dml_ticks_query_function_2);
		duckdb::TableFunction dml_ticks_query_function_3(
		    name,
		    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
		                                         duckdb::LogicalType::BIGINT},
		    RowScanExecute, DmlTicksQueryBind, DmlTicksQueryInit);
		dml_ticks_query_function_3.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		dml_ticks_query_function_3.named_parameters["table_ids"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		dml_ticks_query_function_3.named_parameters["table_names"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		loader.RegisterFunction(dml_ticks_query_function_3);
	}

	// Stateless single-table DML changes lookback. Single source of
	// truth for ad-hoc range queries; the caller specifies which table
	// via `table_id` or `table_name`.
	for (const auto &name : {"cdc_dml_changes_query"}) {
		duckdb::TableFunction changes_query_2(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
		    RowScanExecute, CdcRangeChangesBind, CdcRangeChangesInit);
		changes_query_2.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		changes_query_2.named_parameters["table_id"] = duckdb::LogicalType::BIGINT;
		changes_query_2.named_parameters["table_name"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(changes_query_2);
		duckdb::TableFunction changes_query_3(name,
		                                      duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR,
		                                                                           duckdb::LogicalType::BIGINT,
		                                                                           duckdb::LogicalType::BIGINT},
		                                      RowScanExecute, CdcRangeChangesBind, CdcRangeChangesInit);
		changes_query_3.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		changes_query_3.named_parameters["table_id"] = duckdb::LogicalType::BIGINT;
		changes_query_3.named_parameters["table_name"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(changes_query_3);
	}
}

} // namespace duckdb_cdc
