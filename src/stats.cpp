//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// stats.cpp
//
// Implementation of the observability surface: cdc_consumer_stats and
// cdc_audit_events. These functions are read-only views over the same
// state tables `consumer.cpp` writes to: the consumer-state row for
// stats (cursor / lag / lease / subscription health) and the audit log
// for cdc_audit_events.
//===----------------------------------------------------------------------===//

#include "stats.hpp"

#include "compat_check.hpp"
#include "consumer.hpp"
#include "ducklake_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <sstream>
#include <unordered_set>

namespace duckdb_cdc {

namespace {

//===--------------------------------------------------------------------===//
// cdc_consumer_stats
//===--------------------------------------------------------------------===//

struct CdcConsumerStatsData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcConsumerStatsData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcConsumerStatsBind(duckdb::ClientContext &context,
                                                              duckdb::TableFunctionBindInput &input,
                                                              duckdb::vector<duckdb::LogicalType> &return_types,
                                                              duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_consumer_stats requires catalog");
	}
	auto result = duckdb::make_uniq<CdcConsumerStatsData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	auto entry = input.named_parameters.find("consumer");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->consumer_name = entry->second.GetValue<std::string>();
	}

	names = {"consumer_name",          "consumer_kind",      "consumer_id",          "last_committed_snapshot",
	         "current_snapshot",       "lag_snapshots",      "lag_seconds",          "oldest_available_snapshot",
	         "gap_distance",           "subscription_count", "subscriptions_active", "subscriptions_renamed",
	         "subscriptions_dropped",  "owner_token",        "owner_acquired_at",    "owner_heartbeat_at",
	         "lease_interval_seconds", "lease_alive"};
	return_types = {duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::DOUBLE,       duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,       duckdb::LogicalType::UUID,    duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcConsumerStatsInit(duckdb::ClientContext &context,
                                                                          duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcConsumerStatsData>();
	// Single-connection chain — see CreateConsumer for the H-022 rationale.
	duckdb::Connection conn(*context.db);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	const auto current_snapshot = CurrentSnapshot(conn, data.catalog_name);
	auto oldest_result = conn.Query("SELECT COALESCE(min(snapshot_id), 0) FROM " +
	                                MetadataTable(conn, data.catalog_name, "ducklake_snapshot"));
	const auto oldest_snapshot = SingleInt64(*oldest_result, "oldest snapshot");

	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	std::ostringstream where;
	if (!data.consumer_name.empty()) {
		where << " WHERE c.consumer_name = " << QuoteLiteral(data.consumer_name);
	}

	// Single big projection so the planner JOIN-and-aggregates against
	// ducklake_snapshot in one pass per consumer.
	auto query = std::string("SELECT c.consumer_name, c.consumer_kind, c.consumer_id, c.last_committed_snapshot, "
	                         "s.snapshot_time, c.owner_token, c.owner_acquired_at, "
	                         "c.owner_heartbeat_at, c.lease_interval_seconds FROM ") +
	             consumers + " c LEFT JOIN " + MetadataTable(conn, data.catalog_name, "ducklake_snapshot") +
	             " s ON s.snapshot_id = c.last_committed_snapshot" + where.str() + " ORDER BY c.consumer_name ASC";
	auto rows = conn.Query(query);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_consumer_stats scan failed");
	}

	for (duckdb::idx_t i = 0; i < rows->RowCount(); ++i) {
		const auto consumer_name_value = rows->GetValue(0, i);
		const auto consumer_kind_value = rows->GetValue(1, i);
		const auto consumer_id = rows->GetValue(2, i).GetValue<int64_t>();
		const auto last_committed_snapshot = rows->GetValue(3, i).GetValue<int64_t>();
		const auto last_snapshot_time = rows->GetValue(4, i);
		const auto owner_token_value = rows->GetValue(5, i);
		const auto owner_acquired_value = rows->GetValue(6, i);
		const auto owner_heartbeat_value = rows->GetValue(7, i);
		const auto lease_interval = rows->GetValue(8, i).GetValue<int64_t>();

		const auto lag_snapshots = current_snapshot - last_committed_snapshot;
		duckdb::Value lag_seconds_value;
		if (!last_snapshot_time.IsNull()) {
			auto lag_query = conn.Query(std::string("SELECT epoch(now()) - epoch(") +
			                            QuoteLiteral(last_snapshot_time.ToString()) + "::TIMESTAMP WITH TIME ZONE)");
			if (lag_query && !lag_query->HasError() && lag_query->RowCount() > 0 &&
			    !lag_query->GetValue(0, 0).IsNull()) {
				lag_seconds_value = duckdb::Value::DOUBLE(lag_query->GetValue(0, 0).GetValue<double>());
			}
		}

		const auto gap_distance = last_committed_snapshot - oldest_snapshot;

		const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, consumer_name_value.ToString());
		int64_t active = 0;
		int64_t renamed = 0;
		int64_t dropped = 0;
		for (const auto &subscription : subscriptions) {
			if (subscription.status == "active") {
				active++;
			} else if (subscription.status == "renamed") {
				renamed++;
			} else if (subscription.status == "dropped") {
				dropped++;
			}
		}

		// `lease_alive`: the lease is held AND the heartbeat is within
		// the lease interval. NULL owner_token means no holder, so dead
		// (false; not NULL) — the column is more useful as a 2-state
		// "is anyone reading right now" than a 3-state.
		bool lease_alive = false;
		if (!owner_token_value.IsNull() && !owner_heartbeat_value.IsNull()) {
			auto alive_query = conn.Query(std::string("SELECT epoch(now()) - epoch(") +
			                              QuoteLiteral(owner_heartbeat_value.ToString()) +
			                              "::TIMESTAMP WITH TIME ZONE) < " + std::to_string(lease_interval));
			if (alive_query && !alive_query->HasError() && alive_query->RowCount() > 0 &&
			    !alive_query->GetValue(0, 0).IsNull()) {
				lease_alive = alive_query->GetValue(0, 0).GetValue<bool>();
			}
		}

		std::vector<duckdb::Value> row;
		row.push_back(consumer_name_value);
		row.push_back(consumer_kind_value);
		row.push_back(duckdb::Value::BIGINT(consumer_id));
		row.push_back(duckdb::Value::BIGINT(last_committed_snapshot));
		row.push_back(duckdb::Value::BIGINT(current_snapshot));
		row.push_back(duckdb::Value::BIGINT(lag_snapshots));
		row.push_back(lag_seconds_value);
		row.push_back(duckdb::Value::BIGINT(oldest_snapshot));
		row.push_back(duckdb::Value::BIGINT(gap_distance));
		row.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(subscriptions.size())));
		row.push_back(duckdb::Value::BIGINT(active));
		row.push_back(duckdb::Value::BIGINT(renamed));
		row.push_back(duckdb::Value::BIGINT(dropped));
		row.push_back(owner_token_value);
		row.push_back(owner_acquired_value);
		row.push_back(owner_heartbeat_value);
		row.push_back(duckdb::Value::INTEGER(static_cast<int32_t>(lease_interval)));
		row.push_back(duckdb::Value::BOOLEAN(lease_alive));
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_audit_events
//===--------------------------------------------------------------------===//

struct CdcAuditEventsData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t since_seconds;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcAuditEventsData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->since_seconds = since_seconds;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcAuditEventsBind(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_audit_events requires catalog");
	}
	auto result = duckdb::make_uniq<CdcAuditEventsData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->since_seconds = SinceSecondsParameter(input, 86400);
	if (result->since_seconds < 0) {
		throw duckdb::InvalidInputException("cdc_audit_events since_seconds must be >= 0");
	}
	auto entry = input.named_parameters.find("consumer");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->consumer_name = entry->second.GetValue<std::string>();
	}

	names = {"ts", "audit_id", "actor", "action", "consumer_name", "consumer_id", "details"};
	return_types = {duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcAuditEventsInit(duckdb::ClientContext &context,
                                                                        duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcAuditEventsData>();
	// Single-connection chain — see CreateConsumer for the H-022 rationale.
	duckdb::Connection conn(*context.db);
	BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	const auto audit = StateTable(conn, data.catalog_name, AUDIT_TABLE);
	std::ostringstream where;
	where << " WHERE epoch(ts::TIMESTAMP WITH TIME ZONE) >= epoch(now()) - " << data.since_seconds;
	if (!data.consumer_name.empty()) {
		where << " AND consumer_name = " << QuoteLiteral(data.consumer_name);
	}
	auto query = std::string("SELECT ts, audit_id, actor, action, consumer_name, consumer_id, details FROM ") + audit +
	             where.str() + " ORDER BY ts DESC, audit_id DESC";
	auto rows = conn.Query(query);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_audit_events scan failed");
	}
	for (duckdb::idx_t i = 0; i < rows->RowCount(); ++i) {
		std::vector<duckdb::Value> row;
		row.reserve(rows->ColumnCount());
		for (duckdb::idx_t col_idx = 0; col_idx < rows->ColumnCount(); ++col_idx) {
			row.push_back(rows->GetValue(col_idx, i));
		}
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_doctor
//===--------------------------------------------------------------------===//

struct CdcDoctorData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcDoctorData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

void AddDoctorRow(RowScanState &state, const std::string &severity, const std::string &code,
                  const duckdb::Value &consumer_name, const std::string &message, const duckdb::Value &details) {
	state.rows.push_back(
	    {duckdb::Value(severity), duckdb::Value(code), consumer_name, duckdb::Value(message), details});
}

bool MetadataSchemaExists(duckdb::Connection &conn, const std::string &catalog_name) {
	auto direct = conn.Query("SELECT 1 FROM " + MetadataTable(conn, catalog_name, "ducklake_metadata") + " LIMIT 0");
	if (direct && !direct->HasError()) {
		return true;
	}
	auto result = conn.Query("SELECT count(*) FROM duckdb_schemas() WHERE database_name = " +
	                         QuoteLiteral("__ducklake_metadata_" + catalog_name));
	return result && !result->HasError() && result->RowCount() > 0 && !result->GetValue(0, 0).IsNull() &&
	       result->GetValue(0, 0).GetValue<int64_t>() > 0;
}

bool LeaseIsAlive(duckdb::Connection &conn, const duckdb::Value &heartbeat, int64_t lease_interval_seconds) {
	if (heartbeat.IsNull()) {
		return false;
	}
	auto alive = conn.Query(std::string("SELECT epoch(now()) - epoch(") + QuoteLiteral(heartbeat.ToString()) +
	                        "::TIMESTAMP WITH TIME ZONE) < " + std::to_string(lease_interval_seconds));
	return alive && !alive->HasError() && alive->RowCount() > 0 && !alive->GetValue(0, 0).IsNull() &&
	       alive->GetValue(0, 0).GetValue<bool>();
}

bool HasPendingSchemaBoundary(duckdb::Connection &conn, const std::string &catalog_name, const ConsumerRow &consumer,
                              int64_t current_snapshot) {
	const auto first_snapshot =
	    FirstSnapshotAfter(conn, catalog_name, consumer.last_committed_snapshot, current_snapshot);
	if (first_snapshot == -1) {
		return false;
	}
	if (consumer.consumer_kind == "dml") {
		// For DML consumers the boundary is scoped strictly to the
		// consumer's subscribed tables. A catalog-wide schema change on
		// some other table is not a doctor-level concern for this
		// consumer (the listen path auto-advances past it).
		const auto subscriptions = LoadDmlConsumerSubscriptions(conn, catalog_name, consumer.consumer_name);
		return NextDmlSubscribedSchemaChangeSnapshot(conn, catalog_name, consumer.last_committed_snapshot,
		                                             current_snapshot, subscriptions) != -1;
	}
	if (SnapshotIsExternalSchemaChange(conn, catalog_name, first_snapshot)) {
		return true;
	}
	int64_t base_schema_version = -1;
	try {
		base_schema_version = ResolveSchemaVersion(conn, catalog_name, consumer.last_committed_snapshot);
	} catch (...) {
		return false;
	}
	return NextExternalSchemaChangeSnapshot(conn, catalog_name, first_snapshot, current_snapshot,
	                                        base_schema_version) != -1;
}

duckdb::unique_ptr<duckdb::FunctionData> CdcDoctorBind(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionBindInput &input,
                                                       duckdb::vector<duckdb::LogicalType> &return_types,
                                                       duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_doctor requires catalog");
	}
	auto result = duckdb::make_uniq<CdcDoctorData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	auto entry = input.named_parameters.find("consumer");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->consumer_name = entry->second.GetValue<std::string>();
	}
	names = {"severity", "code", "consumer_name", "message", "details"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcDoctorInit(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcDoctorData>();
	duckdb::Connection conn(*context.db);

	if (!MetadataSchemaExists(conn, data.catalog_name)) {
		AddDoctorRow(*result, "error", "CDC_INCOMPATIBLE_CATALOG", duckdb::Value(),
		             "Catalog is not a compatible DuckLake catalog", duckdb::Value());
		return std::move(result);
	}
	try {
		CheckCatalogOrThrow(conn, data.catalog_name);
		BootstrapConsumerStateOrThrow(conn, data.catalog_name);
	} catch (std::exception &ex) {
		AddDoctorRow(*result, "error", "CDC_INCOMPATIBLE_CATALOG", duckdb::Value(), ex.what(), duckdb::Value());
		return std::move(result);
	}

	AddDoctorRow(*result, "info", "CDC_METADATA_OK", duckdb::Value(), "CDC metadata tables are present",
	             duckdb::Value());

	const auto current_snapshot = CurrentSnapshot(conn, data.catalog_name);
	auto oldest_result = conn.Query("SELECT COALESCE(min(snapshot_id), 0) FROM " +
	                                MetadataTable(conn, data.catalog_name, "ducklake_snapshot"));
	const auto oldest_snapshot = SingleInt64(*oldest_result, "oldest available snapshot");

	std::vector<std::string> consumer_names;
	const auto consumers_table = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	std::ostringstream consumer_query;
	consumer_query << "SELECT consumer_name FROM " << consumers_table;
	if (!data.consumer_name.empty()) {
		consumer_query << " WHERE consumer_name = " << QuoteLiteral(data.consumer_name);
	}
	consumer_query << " ORDER BY consumer_name ASC";
	auto consumer_rows = conn.Query(consumer_query.str());
	if (!consumer_rows || consumer_rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        consumer_rows ? consumer_rows->GetError() : "cdc_doctor consumer scan failed");
	}
	for (duckdb::idx_t i = 0; i < consumer_rows->RowCount(); ++i) {
		if (!consumer_rows->GetValue(0, i).IsNull()) {
			consumer_names.push_back(consumer_rows->GetValue(0, i).ToString());
		}
	}

	for (const auto &consumer_name : consumer_names) {
		const auto consumer = LoadConsumerOrThrow(conn, data.catalog_name, consumer_name);
		const auto consumer_value = duckdb::Value(consumer.consumer_name);
		const auto lag = current_snapshot - consumer.last_committed_snapshot;
		if (consumer.last_committed_snapshot < oldest_snapshot) {
			AddDoctorRow(
			    *result, "error", "CDC_GAP_RISK", consumer_value, "Consumer is behind the oldest available snapshot",
			    duckdb::Value("{\"last_committed_snapshot\":" + std::to_string(consumer.last_committed_snapshot) +
			                  ",\"oldest_available_snapshot\":" + std::to_string(oldest_snapshot) + "}"));
		} else if (lag > 0) {
			AddDoctorRow(*result, "warning", "CDC_CONSUMER_LAG", consumer_value, "Consumer is behind current head",
			             duckdb::Value("{\"lag_snapshots\":" + std::to_string(lag) + "}"));
		}

		if (!consumer.owner_token.IsNull()) {
			if (LeaseIsAlive(conn, consumer.owner_heartbeat_at, consumer.lease_interval_seconds)) {
				AddDoctorRow(*result, "info", "CDC_LEASE_LIVE", consumer_value, "Consumer lease is live",
				             duckdb::Value());
			} else {
				AddDoctorRow(*result, "warning", "CDC_LEASE_STALE", consumer_value, "Consumer lease is stale",
				             duckdb::Value());
			}
		}

		for (const auto &subscription : LoadConsumerSubscriptions(conn, data.catalog_name, consumer.consumer_name)) {
			if (subscription.status == "renamed") {
				AddDoctorRow(
				    *result, "warning", "CDC_SUBSCRIPTION_RENAMED", consumer_value, "A subscription target was renamed",
				    duckdb::Value("{\"subscription_id\":" + std::to_string(subscription.subscription_id) + "}"));
			} else if (subscription.status == "dropped") {
				AddDoctorRow(
				    *result, "error", "CDC_SUBSCRIPTION_DROPPED", consumer_value, "A subscription target was dropped",
				    duckdb::Value("{\"subscription_id\":" + std::to_string(subscription.subscription_id) + "}"));
			}
		}

		if (HasPendingSchemaBoundary(conn, data.catalog_name, consumer, current_snapshot)) {
			AddDoctorRow(*result, "warning", "CDC_SCHEMA_BOUNDARY", consumer_value,
			             "Consumer has a pending schema boundary", duckdb::Value());
		}
	}

	auto audit_query = std::string("SELECT count(*) FROM ") + StateTable(conn, data.catalog_name, AUDIT_TABLE) +
	                   " WHERE action = 'consumer_force_release' "
	                   "AND epoch(ts::TIMESTAMP WITH TIME ZONE) >= epoch(now()) - 86400";
	if (!data.consumer_name.empty()) {
		audit_query += " AND consumer_name = " + QuoteLiteral(data.consumer_name);
	}
	auto audit = conn.Query(audit_query);
	if (audit && !audit->HasError() && audit->RowCount() > 0 && !audit->GetValue(0, 0).IsNull() &&
	    audit->GetValue(0, 0).GetValue<int64_t>() > 0) {
		AddDoctorRow(*result, "warning", "CDC_RECENT_FORCE_RELEASE", duckdb::Value(),
		             "A consumer was force-released recently", duckdb::Value());
	}

	return std::move(result);
}

} // namespace

void RegisterStatsFunctions(duckdb::ExtensionLoader &loader) {
	for (const auto &name : {"cdc_consumer_stats"}) {
		duckdb::TableFunction stats_function(name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                     RowScanExecute, CdcConsumerStatsBind, CdcConsumerStatsInit);
		stats_function.named_parameters["consumer"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(stats_function);
	}

	for (const auto &name : {"cdc_audit_events"}) {
		duckdb::TableFunction audit_function(name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                     RowScanExecute, CdcAuditEventsBind, CdcAuditEventsInit);
		audit_function.named_parameters["since_seconds"] = duckdb::LogicalType::BIGINT;
		audit_function.named_parameters["consumer"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(audit_function);
	}

	for (const auto &name : {"cdc_doctor"}) {
		duckdb::TableFunction doctor_function(name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                      RowScanExecute, CdcDoctorBind, CdcDoctorInit);
		doctor_function.named_parameters["consumer"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(doctor_function);
	}
}

} // namespace duckdb_cdc
