//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// ddl.cpp
//
// Schema-change information surface. Owns:
//
//   * Token decoding for the typed `<lake>.snapshots().changes` MAP form
//     (Parse* / Lookup* helpers).
//
//   * Per-snapshot DDL row extraction (`ExtractDdlRows`,
//     `AddDdlRowFromMapEntry`, `SortDdlRowsForSnapshot`,
//     `ApplyDdlTableFilter`).
//
//   * Per-table column-level diff (`DiffColumns`, `DiffsToJson`,
//     `NormaliseDiffParentChildOrdering`).
//
//   * Table functions: cdc_ddl_changes_read, cdc_ddl_changes_query, cdc_schema_diff.
//
// `cdc_ddl_changes_read/listen` invokes `ReadWindow` from `consumer.cpp` to acquire the
// lease and bound the visible window; the stateless `cdc_ddl_changes_query` and
// `cdc_schema_diff` do not need a consumer.
//===----------------------------------------------------------------------===//

#include "ddl.hpp"

#include "compat_check.hpp"
#include "consumer.hpp"
#include "ducklake_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace duckdb_cdc {

namespace {

duckdb::unique_ptr<duckdb::MaterializedQueryResult> QueryWithH022Retry(duckdb::Connection &conn,
                                                                       const std::string &sql) {
	duckdb::unique_ptr<duckdb::MaterializedQueryResult> result;
	for (int attempt = 0; attempt < 5; ++attempt) {
		result = conn.Query(sql);
		if (result && !result->HasError()) {
			return result;
		}
		const auto message = duckdb::StringUtil::Lower(result ? result->GetError() : std::string());
		const bool h022 = message.find("resource deadlock avoided") != std::string::npos ||
		                  message.find("resource deadlock would occur") != std::string::npos ||
		                  message.find("thread::join failed") != std::string::npos;
		if (!h022 || attempt == 4) {
			return result;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Token decoding for DDL extraction
//===--------------------------------------------------------------------===//

bool ParseIdToken(const std::string &token, const std::string &prefix, int64_t &id) {
	if (!StartsWith(token, prefix)) {
		return false;
	}
	return TryParseInt64(token.substr(prefix.size()), id);
}

//===--------------------------------------------------------------------===//
// Schema/table/view lookups against `ducklake_*` metadata
//===--------------------------------------------------------------------===//

struct DdlObject {
	duckdb::Value schema_id;
	duckdb::Value schema_name;
	duckdb::Value object_id;
	duckdb::Value object_name;
};

DdlObject LookupSchemaByName(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                             const std::string &schema_name) {
	auto result =
	    conn.Query("SELECT schema_id, schema_name FROM " + MetadataTable(conn, catalog_name, "ducklake_schema") +
	               " WHERE begin_snapshot = " + std::to_string(snapshot_id) +
	               " AND schema_name = " + QuoteLiteral(schema_name) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(schema_name), duckdb::Value(), duckdb::Value(schema_name)};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(0, 0), result->GetValue(1, 0)};
}

//! Phase 2 follow-up #2: lookup the schema row alive at `snapshot_id`,
//! using the same `(begin <= snap AND (end IS NULL OR end > snap))`
//! predicate `LookupTableColumnsAt` already uses for column rows. The
//! prior snapshot-unbounded `LIMIT 1` returned the schema's *original*
//! name even after a rename — fine for `created.schema` (where the
//! caller already has the name) but wrong for `dropped.schema` after
//! a rename. Caller must pass `snapshot_id` for ALTERED events and
//! `snapshot_id - 1` for DROPPED events (the row whose `end_snapshot
//! = drop_snap` is the one alive immediately before the drop).
DdlObject LookupSchemaById(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                           int64_t schema_id) {
	auto result = conn.Query(
	    "SELECT schema_id, schema_name FROM " + MetadataTable(conn, catalog_name, "ducklake_schema") +
	    " WHERE schema_id = " + std::to_string(schema_id) + " AND begin_snapshot <= " + std::to_string(snapshot_id) +
	    " AND (end_snapshot IS NULL OR end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value::BIGINT(schema_id), duckdb::Value(), duckdb::Value::BIGINT(schema_id), duckdb::Value()};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(0, 0), result->GetValue(1, 0)};
}

DdlObject LookupTableByName(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                            const std::string &schema_name, const std::string &table_name) {
	auto result = conn.Query("SELECT s.schema_id, s.schema_name, t.table_id, t.table_name FROM " +
	                         MetadataTable(conn, catalog_name, "ducklake_table") + " t JOIN " +
	                         MetadataTable(conn, catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE t.begin_snapshot = " + std::to_string(snapshot_id) +
	                         " AND s.schema_name = " + QuoteLiteral(schema_name) +
	                         " AND t.table_name = " + QuoteLiteral(table_name) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(schema_name), duckdb::Value(), duckdb::Value(table_name)};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(2, 0), result->GetValue(3, 0)};
}

//! Phase 2 follow-up #2: lookup the table row alive at `snapshot_id`,
//! using the temporal predicate so ID-resolved tokens carry the name
//! as-of the snapshot. The previous snapshot-unbounded `LIMIT 1`
//! arbitrarily returned whichever rename row the storage layer iterated
//! first — typically the original name — which is wrong for
//! `tables_altered:<id>` after a RENAME (we want the post-rename name)
//! and for `tables_dropped:<id>` (we want the immediately-pre-drop
//! name, which the caller passes as `snapshot_id - 1`). Joins to
//! `ducklake_schema` use the same temporal alive-at predicate so the
//! schema row reflects its name at the right moment too.
DdlObject LookupTableById(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                          int64_t table_id) {
	auto result =
	    conn.Query("SELECT s.schema_id, s.schema_name, t.table_id, t.table_name FROM " +
	               MetadataTable(conn, catalog_name, "ducklake_table") + " t JOIN " +
	               MetadataTable(conn, catalog_name, "ducklake_schema") + " s USING (schema_id) WHERE t.table_id = " +
	               std::to_string(table_id) + " AND t.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(snapshot_id) + ")" +
	               " AND s.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (s.end_snapshot IS NULL OR s.end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(), duckdb::Value::BIGINT(table_id), duckdb::Value()};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(2, 0), result->GetValue(3, 0)};
}

DdlObject LookupViewByName(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                           const std::string &schema_name, const std::string &view_name) {
	auto result = conn.Query("SELECT s.schema_id, s.schema_name, v.view_id, v.view_name FROM " +
	                         MetadataTable(conn, catalog_name, "ducklake_view") + " v JOIN " +
	                         MetadataTable(conn, catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE v.begin_snapshot = " + std::to_string(snapshot_id) +
	                         " AND s.schema_name = " + QuoteLiteral(schema_name) +
	                         " AND v.view_name = " + QuoteLiteral(view_name) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(schema_name), duckdb::Value(), duckdb::Value(view_name)};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(2, 0), result->GetValue(3, 0)};
}

//! Phase 2 follow-up #2: same temporal alive-at-`snapshot_id` predicate
//! as `LookupTableById`. Used only for `views_dropped` today (Stage-1
//! does not surface ALTER VIEW; CREATE OR REPLACE VIEW is decomposed
//! by DuckLake into `views_dropped` + `views_created`). Caller passes
//! `snapshot_id - 1` for DROPPED so the lookup hits the row whose
//! `end_snapshot = drop_snap`.
DdlObject LookupViewById(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                         int64_t view_id) {
	auto result =
	    conn.Query("SELECT s.schema_id, s.schema_name, v.view_id, v.view_name FROM " +
	               MetadataTable(conn, catalog_name, "ducklake_view") + " v JOIN " +
	               MetadataTable(conn, catalog_name, "ducklake_schema") + " s USING (schema_id) WHERE v.view_id = " +
	               std::to_string(view_id) + " AND v.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (v.end_snapshot IS NULL OR v.end_snapshot > " + std::to_string(snapshot_id) + ")" +
	               " AND s.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (s.end_snapshot IS NULL OR s.end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(), duckdb::Value::BIGINT(view_id), duckdb::Value()};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(2, 0), result->GetValue(3, 0)};
}

std::vector<duckdb::Value> DdlRow(int64_t snapshot_id, const duckdb::Value &snapshot_time,
                                  const std::string &event_kind, const std::string &object_kind,
                                  const DdlObject &object, const std::string &details_json = "{}") {
	return {duckdb::Value::BIGINT(snapshot_id),
	        snapshot_time,
	        duckdb::Value(event_kind),
	        duckdb::Value(object_kind),
	        object.schema_id,
	        object.schema_name,
	        object.object_id,
	        object.object_name,
	        duckdb::Value(details_json)};
}

//===--------------------------------------------------------------------===//
// Per-table column diff
//===--------------------------------------------------------------------===//

//! Snapshot of a single column at a specific snapshot.
struct ColumnInfo {
	int64_t column_id;
	int64_t column_order;
	std::string column_name;
	std::string column_type;
	bool nullable;
	duckdb::Value initial_default;
	duckdb::Value default_value;
	duckdb::Value parent_column;
};

//! Difference between two ColumnInfo lists (one at snapshot S-1 vs S).
//! `parent_column_id` is the struct-parent's column_id; valid only when
//! `has_parent` is true (i.e. the diffed column is a nested-struct field
//! per `ducklake_column.parent_column`). Phase 2 deferral 3: the field
//! is populated for ADDED and DROPPED kinds so downstream consumers can
//! reconstruct the parent/child relationship without re-querying the
//! catalog.
struct ColumnDiff {
	enum class Kind { ADDED, DROPPED, RENAMED, TYPE_CHANGED, DEFAULT_CHANGED, NULLABLE_CHANGED };
	Kind kind;
	int64_t column_id;
	std::string old_name;
	std::string new_name;
	std::string old_type;
	std::string new_type;
	duckdb::Value old_default;
	duckdb::Value new_default;
	bool old_nullable;
	bool new_nullable;
	int64_t parent_column_id = 0;
	bool has_parent = false;
};

//! Read every column of `table_id` that is live at `snapshot_id` (i.e.
//! `begin_snapshot <= snapshot_id` and `end_snapshot IS NULL OR
//! end_snapshot >= snapshot_id`). Returned in column_order so the
//! per-table column list is stable for diffs.
std::vector<ColumnInfo> LookupTableColumnsAt(duckdb::Connection &conn, const std::string &catalog_name,
                                             int64_t table_id, int64_t snapshot_id) {
	std::vector<ColumnInfo> columns;
	// DuckLake's `end_snapshot` is *exclusive*: it stamps the snapshot at
	// which the row becomes invisible. So "alive at snap N" means
	// `begin <= N AND (end IS NULL OR end > N)`. SELECT ... AT (VERSION =>
	// dropped_snap) returns the post-drop column set, which matches.
	auto result = conn.Query(
	    "SELECT column_id, column_order, column_name, column_type, nulls_allowed, "
	    "initial_default, default_value, parent_column FROM " +
	    MetadataTable(conn, catalog_name, "ducklake_column") + " WHERE table_id = " + std::to_string(table_id) +
	    " AND begin_snapshot <= " + std::to_string(snapshot_id) + " AND (end_snapshot IS NULL OR end_snapshot > " +
	    std::to_string(snapshot_id) + ")" + " ORDER BY column_order ASC");
	if (!result || result->HasError()) {
		return columns;
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		ColumnInfo column;
		const auto col_id_value = result->GetValue(0, i);
		const auto col_order_value = result->GetValue(1, i);
		const auto col_name_value = result->GetValue(2, i);
		const auto col_type_value = result->GetValue(3, i);
		const auto nulls_allowed_value = result->GetValue(4, i);
		if (col_id_value.IsNull() || col_name_value.IsNull() || col_type_value.IsNull()) {
			continue;
		}
		column.column_id = col_id_value.GetValue<int64_t>();
		column.column_order = col_order_value.IsNull() ? 0 : col_order_value.GetValue<int64_t>();
		column.column_name = col_name_value.ToString();
		column.column_type = col_type_value.ToString();
		column.nullable = nulls_allowed_value.IsNull() ? true : nulls_allowed_value.GetValue<bool>();
		column.initial_default = result->GetValue(5, i);
		column.default_value = result->GetValue(6, i);
		column.parent_column = result->GetValue(7, i);
		columns.push_back(std::move(column));
	}
	return columns;
}

//! Compare two ColumnInfo lists and emit per-column diffs. The diff rule
//! lives entirely on column_id (DuckLake preserves the id across renames /
//! type changes / default changes / nullable flips) — names alone are not
//! enough because two distinct rename+add operations could produce the
//! same name set with different identities.
std::vector<ColumnDiff> DiffColumns(const std::vector<ColumnInfo> &old_columns,
                                    const std::vector<ColumnInfo> &new_columns) {
	std::vector<ColumnDiff> diffs;
	std::unordered_map<int64_t, const ColumnInfo *> by_id_old;
	std::unordered_map<int64_t, const ColumnInfo *> by_id_new;
	for (const auto &c : old_columns) {
		by_id_old[c.column_id] = &c;
	}
	for (const auto &c : new_columns) {
		by_id_new[c.column_id] = &c;
	}
	for (const auto &c : new_columns) {
		auto it = by_id_old.find(c.column_id);
		if (it == by_id_old.end()) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::ADDED;
			d.column_id = c.column_id;
			d.new_name = c.column_name;
			d.new_type = c.column_type;
			d.new_default = c.default_value;
			d.new_nullable = c.nullable;
			if (!c.parent_column.IsNull()) {
				d.parent_column_id = c.parent_column.GetValue<int64_t>();
				d.has_parent = true;
			}
			diffs.push_back(std::move(d));
			continue;
		}
		const auto &old_col = *it->second;
		if (old_col.column_name != c.column_name) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::RENAMED;
			d.column_id = c.column_id;
			d.old_name = old_col.column_name;
			d.new_name = c.column_name;
			diffs.push_back(std::move(d));
		}
		if (old_col.column_type != c.column_type) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::TYPE_CHANGED;
			d.column_id = c.column_id;
			d.old_name = old_col.column_name;
			d.new_name = c.column_name;
			d.old_type = old_col.column_type;
			d.new_type = c.column_type;
			diffs.push_back(std::move(d));
		}
		const auto old_default_str = old_col.default_value.IsNull() ? std::string() : old_col.default_value.ToString();
		const auto new_default_str = c.default_value.IsNull() ? std::string() : c.default_value.ToString();
		const bool old_default_null = old_col.default_value.IsNull();
		const bool new_default_null = c.default_value.IsNull();
		if (old_default_null != new_default_null || old_default_str != new_default_str) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::DEFAULT_CHANGED;
			d.column_id = c.column_id;
			d.old_name = old_col.column_name;
			d.new_name = c.column_name;
			d.old_default = old_col.default_value;
			d.new_default = c.default_value;
			diffs.push_back(std::move(d));
		}
		if (old_col.nullable != c.nullable) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::NULLABLE_CHANGED;
			d.column_id = c.column_id;
			d.old_name = old_col.column_name;
			d.new_name = c.column_name;
			d.old_nullable = old_col.nullable;
			d.new_nullable = c.nullable;
			diffs.push_back(std::move(d));
		}
	}
	for (const auto &c : old_columns) {
		if (by_id_new.find(c.column_id) == by_id_new.end()) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::DROPPED;
			d.column_id = c.column_id;
			d.old_name = c.column_name;
			d.old_type = c.column_type;
			d.old_default = c.default_value;
			d.old_nullable = c.nullable;
			if (!c.parent_column.IsNull()) {
				d.parent_column_id = c.parent_column.GetValue<int64_t>();
				d.has_parent = true;
			}
			diffs.push_back(std::move(d));
		}
	}
	return diffs;
}

//! Phase 2 deferral 3: stable-sort ADDED diffs so a struct parent appears
//! before any of its nested-field children, and DROPPED diffs so children
//! appear before their parent. Mirrors the cross-snapshot ordering rule
//! cdc_ddl_changes_read already uses for `(event_kind, object_kind)` (ADR 0008): a
//! consumer building / tearing down typed schemas observes the parent
//! struct exists before its fields are added, and observes a struct's
//! fields disappear before the struct itself is dropped.
//!
//! The sort is *stable* so the natural column_order ordering is preserved
//! among siblings; we only reorder when a parent/child pair is out of
//! position. Within ADDED, the predicate is "parents before children"
//! (top-level columns get rank 0; child columns get rank = depth from
//! root); within DROPPED, the rank is negated so the deepest leaves go
//! first.
void NormaliseDiffParentChildOrdering(std::vector<ColumnDiff> &diffs) {
	std::unordered_map<int64_t, int64_t> parent_of;
	for (const auto &d : diffs) {
		if ((d.kind == ColumnDiff::Kind::ADDED || d.kind == ColumnDiff::Kind::DROPPED) && d.has_parent) {
			parent_of[d.column_id] = d.parent_column_id;
		}
	}
	auto depth_of = [&](int64_t column_id) {
		int depth = 0;
		auto cursor = column_id;
		while (true) {
			auto it = parent_of.find(cursor);
			if (it == parent_of.end()) {
				return depth;
			}
			++depth;
			cursor = it->second;
			if (depth > 64) {
				// Defensive: cycles in parent_column would be a DuckLake
				// catalog corruption; cap traversal so we don't spin.
				return depth;
			}
		}
	};
	std::stable_sort(diffs.begin(), diffs.end(), [&](const ColumnDiff &a, const ColumnDiff &b) {
		if (a.kind != b.kind) {
			// Preserve grouping by kind — the JSON serializer sweeps each
			// kind independently anyway, but a stable order across kinds
			// keeps debug dumps deterministic.
			return false;
		}
		if (a.kind == ColumnDiff::Kind::ADDED) {
			return depth_of(a.column_id) < depth_of(b.column_id);
		}
		if (a.kind == ColumnDiff::Kind::DROPPED) {
			return depth_of(a.column_id) > depth_of(b.column_id);
		}
		return false;
	});
}

const char *DiffKindToString(ColumnDiff::Kind kind) {
	switch (kind) {
	case ColumnDiff::Kind::ADDED:
		return "added";
	case ColumnDiff::Kind::DROPPED:
		return "dropped";
	case ColumnDiff::Kind::RENAMED:
		return "renamed";
	case ColumnDiff::Kind::TYPE_CHANGED:
		return "type_changed";
	case ColumnDiff::Kind::DEFAULT_CHANGED:
		return "default_changed";
	case ColumnDiff::Kind::NULLABLE_CHANGED:
		return "nullable_changed";
	}
	return "unknown";
}

//! Serialize a ColumnInfo as a JSON object:
//! `{"id":N,"order":N,"name":"...","type":"...","nullable":bool,"default":"..."|null}`.
std::string ColumnInfoToJson(const ColumnInfo &c) {
	std::ostringstream out;
	out << "{\"id\":" << c.column_id << ",\"order\":" << c.column_order << ",\"name\":\"" << JsonEscape(c.column_name)
	    << "\",\"type\":\"" << JsonEscape(c.column_type) << "\",\"nullable\":" << (c.nullable ? "true" : "false")
	    << ",\"default\":";
	if (c.default_value.IsNull()) {
		out << "null";
	} else {
		out << "\"" << JsonEscape(c.default_value.ToString()) << "\"";
	}
	if (!c.parent_column.IsNull()) {
		// Field name aligned with ADR 0008 / Phase 2 deferral 3: the
		// nested-struct parent reference is `parent_column_id` (the
		// referenced column_id is itself a column_id, not the column's
		// name). Phase 1 emitted this as `parent_column` which conflicted
		// with the ADR text and with the cdc_schema_diff column name.
		out << ",\"parent_column_id\":" << c.parent_column.ToString();
	}
	out << "}";
	return out.str();
}

//! Serialize the diff list as a JSON object grouped by diff kind. Empty
//! groups are omitted so the payload stays compact for the common case
//! (most ALTER TABLE commits change exactly one column in exactly one
//! way). Phase 2 deferral 3: ADDED / DROPPED entries gain a
//! `parent_column_id` field for nested-struct children so consumers can
//! reconstruct the parent/child relationship without re-querying the
//! catalog; the diff list is also normalised so parents precede children
//! on ADDED and children precede parents on DROPPED.
std::string DiffsToJson(std::vector<ColumnDiff> diffs) {
	NormaliseDiffParentChildOrdering(diffs);
	auto kind_filter = [&diffs](ColumnDiff::Kind kind, std::ostringstream &out) {
		bool first = true;
		for (const auto &d : diffs) {
			if (d.kind != kind) {
				continue;
			}
			if (!first) {
				out << ",";
			}
			first = false;
			out << "{\"id\":" << d.column_id;
			switch (kind) {
			case ColumnDiff::Kind::ADDED:
				out << ",\"name\":\"" << JsonEscape(d.new_name) << "\",\"type\":\"" << JsonEscape(d.new_type)
				    << "\",\"nullable\":" << (d.new_nullable ? "true" : "false")
				    << ",\"default\":" << JsonOptionalString(d.new_default);
				if (d.has_parent) {
					out << ",\"parent_column_id\":" << d.parent_column_id;
				}
				break;
			case ColumnDiff::Kind::DROPPED:
				out << ",\"name\":\"" << JsonEscape(d.old_name) << "\",\"type\":\"" << JsonEscape(d.old_type)
				    << "\",\"nullable\":" << (d.old_nullable ? "true" : "false")
				    << ",\"default\":" << JsonOptionalString(d.old_default);
				if (d.has_parent) {
					out << ",\"parent_column_id\":" << d.parent_column_id;
				}
				break;
			case ColumnDiff::Kind::RENAMED:
				out << ",\"old_name\":\"" << JsonEscape(d.old_name) << "\",\"new_name\":\"" << JsonEscape(d.new_name)
				    << "\"";
				break;
			case ColumnDiff::Kind::TYPE_CHANGED:
				out << ",\"name\":\"" << JsonEscape(d.new_name) << "\",\"old_type\":\"" << JsonEscape(d.old_type)
				    << "\",\"new_type\":\"" << JsonEscape(d.new_type) << "\"";
				break;
			case ColumnDiff::Kind::DEFAULT_CHANGED:
				out << ",\"name\":\"" << JsonEscape(d.new_name)
				    << "\",\"old_default\":" << JsonOptionalString(d.old_default)
				    << ",\"new_default\":" << JsonOptionalString(d.new_default);
				break;
			case ColumnDiff::Kind::NULLABLE_CHANGED:
				out << ",\"name\":\"" << JsonEscape(d.new_name)
				    << "\",\"old_nullable\":" << (d.old_nullable ? "true" : "false")
				    << ",\"new_nullable\":" << (d.new_nullable ? "true" : "false");
				break;
			}
			out << "}";
		}
	};
	std::ostringstream out;
	const char *separator = "";
	const std::vector<std::pair<ColumnDiff::Kind, const char *>> kinds = {
	    {ColumnDiff::Kind::ADDED, "added"},
	    {ColumnDiff::Kind::DROPPED, "dropped"},
	    {ColumnDiff::Kind::RENAMED, "renamed"},
	    {ColumnDiff::Kind::TYPE_CHANGED, "type_changed"},
	    {ColumnDiff::Kind::DEFAULT_CHANGED, "default_changed"},
	    {ColumnDiff::Kind::NULLABLE_CHANGED, "nullable_changed"}};
	for (const auto &kind_pair : kinds) {
		bool any = false;
		for (const auto &d : diffs) {
			if (d.kind == kind_pair.first) {
				any = true;
				break;
			}
		}
		if (!any) {
			continue;
		}
		out << separator << "\"" << kind_pair.second << "\":[";
		kind_filter(kind_pair.first, out);
		out << "]";
		separator = ",";
	}
	return out.str();
}

//! Find the most-recent table_name DuckLake had for `table_id` strictly
//! before `snapshot_id`. Used for Finding-1 rename detection: if a
//! `created_table:"X"."Y"` token at snapshot S maps to a table_id whose
//! immediately-prior name is different, we surface the event as
//! `altered.table` with a rename payload rather than as `created.table`.
duckdb::Value PreviousTableName(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                                int64_t snapshot_id) {
	auto result = conn.Query("SELECT table_name FROM " + MetadataTable(conn, catalog_name, "ducklake_table") +
	                         " WHERE table_id = " + std::to_string(table_id) + " AND begin_snapshot < " +
	                         std::to_string(snapshot_id) + " ORDER BY begin_snapshot DESC LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return duckdb::Value();
	}
	return result->GetValue(0, 0);
}

//! Read view metadata (definition + dialect + column aliases) at the
//! snapshot the view first appears in.
struct ViewInfo {
	std::string sql;
	std::string dialect;
	duckdb::Value column_aliases;
};

ViewInfo LookupViewMetadata(duckdb::Connection &conn, const std::string &catalog_name, int64_t view_id,
                            int64_t snapshot_id) {
	ViewInfo info;
	auto result = conn.Query(
	    "SELECT sql, dialect, column_aliases FROM " + MetadataTable(conn, catalog_name, "ducklake_view") +
	    " WHERE view_id = " + std::to_string(view_id) + " AND begin_snapshot <= " + std::to_string(snapshot_id) +
	    " AND (end_snapshot IS NULL OR end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return info;
	}
	const auto sql_value = result->GetValue(0, 0);
	const auto dialect_value = result->GetValue(1, 0);
	info.sql = sql_value.IsNull() ? std::string() : sql_value.ToString();
	info.dialect = dialect_value.IsNull() ? std::string() : dialect_value.ToString();
	info.column_aliases = result->GetValue(2, 0);
	return info;
}

//! Build the typed `details` JSON payload for a `created.table` event.
//! Payload shape: `{"columns":[{...ColumnInfoToJson...}, ...]}`.
std::string BuildCreatedTableDetails(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                     int64_t table_id) {
	const auto columns = LookupTableColumnsAt(conn, catalog_name, table_id, snapshot_id);
	std::ostringstream out;
	out << "{\"columns\":[";
	for (size_t i = 0; i < columns.size(); ++i) {
		if (i > 0) {
			out << ",";
		}
		out << ColumnInfoToJson(columns[i]);
	}
	out << "]}";
	return out.str();
}

//! Phase 2 deferral 2: cheap "did `table_id` get a schema-version bump
//! at `snapshot_id`?" probe used to short-circuit the column-diff
//! reconstruction in `BuildAlteredTableDetails` and `cdc_schema_diff`.
//!
//! `ducklake_schema_versions` has one row per (table_id, begin_snapshot)
//! whenever a table's column list changed. A pure RENAME TABLE does not
//! bump schema_version, and (more importantly) the noisy case where
//! Stage-1 emits `altered_table:<id>` for an `altered_table:<id>` token
//! whose adjacent text actually came from a rename — the column diff
//! would be empty regardless. Either way, when no row exists for
//! (table_id, begin_snapshot=snapshot_id) the two `LookupTableColumnsAt`
//! calls + the `DiffColumns` traversal are guaranteed redundant; we
//! skip them and let downstream serialise the empty diff group.
bool TableSchemaVersionBumpedAt(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                                int64_t snapshot_id) {
	auto result = conn.Query("SELECT 1 FROM " + MetadataTable(conn, catalog_name, "ducklake_schema_versions") +
	                         " WHERE table_id = " + std::to_string(table_id) +
	                         " AND begin_snapshot = " + std::to_string(snapshot_id) + " LIMIT 1");
	if (!result || result->HasError()) {
		// On error, fall back to "assume bump" so the caller still runs
		// the full diff path. Better to do redundant work than to drop a
		// real ALTER on a transient catalog query failure.
		return true;
	}
	return result->RowCount() > 0;
}

//! Build the typed `details` JSON payload for an `altered.table` event.
//! When `old_table_name` is non-empty (Finding-1 rename), the rename pair
//! is included alongside the column-level diff. The diff itself uses
//! `LookupTableColumnsAt(snapshot_id - 1)` vs `LookupTableColumnsAt(snapshot_id)`
//! to pick up everything DuckLake committed at this snapshot.
//!
//! Phase 2 deferral 2: when `ducklake_schema_versions` shows no per-
//! table schema bump at `snapshot_id`, skip the column-diff lookups
//! entirely. The diff would be empty by construction; the rename
//! payload (if any) is still emitted.
std::string BuildAlteredTableDetails(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                     int64_t table_id, const std::string &old_table_name,
                                     const std::string &new_table_name) {
	std::vector<ColumnDiff> diffs;
	if (TableSchemaVersionBumpedAt(conn, catalog_name, table_id, snapshot_id)) {
		const auto old_columns = LookupTableColumnsAt(conn, catalog_name, table_id, snapshot_id - 1);
		const auto new_columns = LookupTableColumnsAt(conn, catalog_name, table_id, snapshot_id);
		diffs = DiffColumns(old_columns, new_columns);
	}
	std::ostringstream out;
	out << "{";
	const char *separator = "";
	if (!old_table_name.empty() && old_table_name != new_table_name) {
		out << separator << "\"old_table_name\":\"" << JsonEscape(old_table_name) << "\",\"new_table_name\":\""
		    << JsonEscape(new_table_name) << "\"";
		separator = ",";
	}
	const auto diffs_json = DiffsToJson(diffs);
	if (!diffs_json.empty()) {
		out << separator << diffs_json;
	}
	out << "}";
	return out.str();
}

//! Build the typed `details` JSON payload for a `created.view` event.
//! Payload shape: `{"definition":"...","dialect":"...","column_aliases":"..."|null}`.
std::string BuildCreatedViewDetails(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                    int64_t view_id) {
	const auto info = LookupViewMetadata(conn, catalog_name, view_id, snapshot_id);
	std::ostringstream out;
	out << "{\"definition\":\"" << JsonEscape(info.sql) << "\",\"dialect\":\"" << JsonEscape(info.dialect)
	    << "\",\"column_aliases\":" << JsonOptionalString(info.column_aliases) << "}";
	return out.str();
}

//! Phase 2 deferral 1: typed Stage-1 dispatcher driven by MAP keys
//! from `<lake>.snapshots().changes` rather than text tokens from
//! `__ducklake_metadata_<lake>.ducklake_snapshot_changes.changes_made`.
//!
//! Each MAP key signals a DDL kind; each value in the per-key list is
//! either an unquoted qualified name (`schema.table` for tables/views,
//! `schema` for schemas) or the stringified `<id>` for kinds that
//! reference catalog rows by id. Per the upstream probe in
//! `e2e/smoke/enumerate_changes_map.py` (committed JSONs in
//! `e2e/smoke/output/`), all three DuckLake catalog backends
//! (DuckDB, SQLite, Postgres) emit identical MAP key sets for every
//! DDL operation, so this dispatcher is backend-agnostic by
//! construction. ADR 0008 makes the MAP form the canonical Stage-1
//! source for typed DDL rows.
//!
//! DML / maintenance MAP keys (`tables_inserted_into`, `inlined_insert`,
//! `inlined_delete`, `tables_deleted_from`, `merge_adjacent`) are
//! silently skipped — Stage-1 is a typed DDL surface only, per
//! ADR 0008's `cdc_ddl_changes_read excludes compacted_table` rule.
//! Phase 2 follow-up #1: caller passes a per-snapshot set of table_ids
//! that the Finding-1 path has already promoted to `altered.table`.
//! When the `tables_altered:<id>` branch sees a hit, it skips emission
//! — otherwise the same `(snapshot_id, table_id)` would surface twice
//! (once via the rename-aware Finding-1 row and once via the
//! ID-resolved row). The Finding-1 row is strictly more informative
//! (carries the rename pair plus the same column diff), so dropping
//! the ID-resolved sibling loses no information. Caller guarantees
//! `tables_created` keys are processed before `tables_altered` within
//! the same snapshot via the SQL `ORDER BY` in `ExtractDdlRows`.
void AddDdlRowFromMapEntry(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                           const duckdb::Value &snapshot_time, const std::string &map_key, const std::string &value,
                           std::vector<std::vector<duckdb::Value>> &rows,
                           std::unordered_set<int64_t> &finding1_promoted_table_ids) {
	if (map_key == "schemas_created") {
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "created", "schema",
		                      LookupSchemaByName(conn, catalog_name, snapshot_id, value)));
		return;
	}
	if (map_key == "tables_created") {
		// Finding-1 (ADR 0008): a `tables_created` value `schema.name`
		// whose table_id already had a different name in the previous
		// snapshot is a RENAME, not a creation. Emit `altered.table`
		// with a rename payload in that case so downstream consumers
		// see the rename explicitly rather than as a paired drop +
		// create that would confuse identity.
		const auto dot = value.find('.');
		if (dot == std::string::npos) {
			return;
		}
		const auto schema_name = value.substr(0, dot);
		const auto object_name = value.substr(dot + 1);
		const auto object = LookupTableByName(conn, catalog_name, snapshot_id, schema_name, object_name);
		if (!object.object_id.IsNull()) {
			const auto table_id = object.object_id.GetValue<int64_t>();
			const auto previous_name = PreviousTableName(conn, catalog_name, table_id, snapshot_id);
			if (!previous_name.IsNull() && previous_name.ToString() != object_name) {
				const auto details = BuildAlteredTableDetails(conn, catalog_name, snapshot_id, table_id,
				                                              previous_name.ToString(), object_name);
				rows.push_back(DdlRow(snapshot_id, snapshot_time, "altered", "table", object, details));
				// Mark this table_id so a subsequent
				// `tables_altered:<table_id>` MAP entry in the same
				// snapshot is suppressed (Phase 2 follow-up #1).
				finding1_promoted_table_ids.insert(table_id);
			} else {
				const auto details = BuildCreatedTableDetails(conn, catalog_name, snapshot_id, table_id);
				rows.push_back(DdlRow(snapshot_id, snapshot_time, "created", "table", object, details));
			}
		} else {
			rows.push_back(DdlRow(snapshot_id, snapshot_time, "created", "table", object));
		}
		return;
	}
	if (map_key == "tables_altered") {
		int64_t id = 0;
		if (!TryParseInt64(value, id)) {
			return;
		}
		// Phase 2 follow-up #1: when the Finding-1 path already
		// emitted an `altered.table` for this id at this snapshot,
		// skip — the rename row carries the strictly larger payload
		// (`old_table_name` + `new_table_name` + the same column
		// diff). Without dedup, downstream consumers keying on
		// `(snapshot_id, object_kind, object_id)` would see two rows.
		if (finding1_promoted_table_ids.count(id) > 0) {
			return;
		}
		// ALTERED: the table is alive at `snapshot_id` (post-ALTER
		// state) and `LookupTableById` resolves the row alive at
		// `snapshot_id` per Phase 2 follow-up #2 — picking up any
		// rename that landed in the same transaction rather than the
		// pre-rename name the original `LIMIT 1` returned.
		const auto object = LookupTableById(conn, catalog_name, snapshot_id, id);
		const auto details =
		    BuildAlteredTableDetails(conn, catalog_name, snapshot_id, id, std::string(),
		                             object.object_name.IsNull() ? std::string() : object.object_name.ToString());
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "altered", "table", object, details));
		return;
	}
	if (map_key == "views_created") {
		const auto dot = value.find('.');
		if (dot == std::string::npos) {
			return;
		}
		const auto schema_name = value.substr(0, dot);
		const auto object_name = value.substr(dot + 1);
		const auto object = LookupViewByName(conn, catalog_name, snapshot_id, schema_name, object_name);
		std::string details = "{}";
		if (!object.object_id.IsNull()) {
			details = BuildCreatedViewDetails(conn, catalog_name, snapshot_id, object.object_id.GetValue<int64_t>());
		}
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "created", "view", object, details));
		return;
	}
	if (map_key == "schemas_dropped") {
		int64_t id = 0;
		if (!TryParseInt64(value, id)) {
			return;
		}
		// DROPPED: the row's `end_snapshot = snapshot_id`, so the row
		// alive at `snapshot_id - 1` is the one immediately before the
		// drop and carries the name we want to surface (Phase 2
		// follow-up #2). Same applies to tables_dropped /
		// views_dropped below.
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "dropped", "schema",
		                      LookupSchemaById(conn, catalog_name, snapshot_id - 1, id)));
		return;
	}
	if (map_key == "tables_dropped") {
		int64_t id = 0;
		if (!TryParseInt64(value, id)) {
			return;
		}
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "dropped", "table",
		                      LookupTableById(conn, catalog_name, snapshot_id - 1, id)));
		return;
	}
	if (map_key == "views_dropped") {
		int64_t id = 0;
		if (!TryParseInt64(value, id)) {
			return;
		}
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "dropped", "view",
		                      LookupViewById(conn, catalog_name, snapshot_id - 1, id)));
		return;
	}
	// Other MAP keys are DML / maintenance and not Stage-1 DDL events.
	// Silently skipped per ADR 0008 (`cdc_ddl_changes_read excludes compacted_table`).
}

//===--------------------------------------------------------------------===//
// DDL row ordering / filtering / extraction
//===--------------------------------------------------------------------===//

//! Per-snapshot DDL ordering rank per ADR 0008. Within one snapshot the
//! contract is `(object_kind, object_id)` ascending, with `object_kind`
//! sort order `schema, view, table` for `created`/`altered` so dependents
//! land after their parents, and the reverse order for `dropped` so
//! tables and views are dropped before their containing schemas. Within
//! the same `(event_kind, object_kind)` we tie-break on `object_id`.
int DdlObjectKindRank(const std::string &event_kind, const std::string &object_kind) {
	const bool reversed = event_kind == "dropped";
	if (object_kind == "schema") {
		return reversed ? 2 : 0;
	}
	if (object_kind == "view") {
		return 1;
	}
	if (object_kind == "table") {
		return reversed ? 0 : 2;
	}
	return 3;
}

//! Stable sort the rows in `[begin, end)` (one snapshot's worth of DDL
//! events) by the ADR 0008 per-snapshot ordering contract. The DdlRow
//! layout is `[snapshot_id, snapshot_time, event_kind, object_kind,
//! schema_id, schema_name, object_id, object_name, details]`.
void SortDdlRowsForSnapshot(std::vector<std::vector<duckdb::Value>> &rows, size_t begin, size_t end) {
	if (end - begin < 2) {
		return;
	}
	std::stable_sort(rows.begin() + static_cast<std::ptrdiff_t>(begin), rows.begin() + static_cast<std::ptrdiff_t>(end),
	                 [](const std::vector<duckdb::Value> &a, const std::vector<duckdb::Value> &b) {
		                 const auto a_event = a[2].IsNull() ? std::string() : a[2].ToString();
		                 const auto b_event = b[2].IsNull() ? std::string() : b[2].ToString();
		                 const auto a_kind = a[3].IsNull() ? std::string() : a[3].ToString();
		                 const auto b_kind = b[3].IsNull() ? std::string() : b[3].ToString();
		                 const auto a_rank = DdlObjectKindRank(a_event, a_kind);
		                 const auto b_rank = DdlObjectKindRank(b_event, b_kind);
		                 if (a_rank != b_rank) {
			                 return a_rank < b_rank;
		                 }
		                 const auto a_id = a[6].IsNull() ? INT64_MAX : a[6].GetValue<int64_t>();
		                 const auto b_id = b[6].IsNull() ? INT64_MAX : b[6].GetValue<int64_t>();
		                 return a_id < b_id;
	                 });
}

//! Apply the cdc_ddl_changes_read `for_table` filter to the rows in `[begin, end)`
//! (one snapshot's worth of typed DDL output). Schema-level events
//! have an empty `schema.object_name` identity and are dropped — the
//! `for_table` filter is a table-scoped projection. Used by both the
//! Stage-1 MAP scan (cdc_ddl_changes_read) and the Stage-1 text scan (cdc_dml_ticks_read
//! does not call this; it has its own per-token table filter).
void ApplyDdlTableFilter(std::vector<std::vector<duckdb::Value>> &rows, size_t begin, const std::string &table_filter) {
	if (table_filter.empty()) {
		return;
	}
	auto out_idx = begin;
	for (auto i = begin; i < rows.size(); ++i) {
		const auto &row = rows[i];
		if (row.size() < 8) {
			continue;
		}
		const auto schema_value = row[5];
		const auto name_value = row[7];
		if (schema_value.IsNull() || name_value.IsNull()) {
			continue;
		}
		const auto qualified = schema_value.ToString() + "." + name_value.ToString();
		if (qualified == table_filter) {
			if (out_idx != i) {
				rows[out_idx] = rows[i];
			}
			out_idx++;
		}
	}
	rows.resize(out_idx);
}

//! Phase 2 deferral 1: scan `<lake>.snapshots()` over `[start_snapshot,
//! end_snapshot]` via the typed `changes` MAP and reconstruct typed DDL
//! rows for every recognised MAP key. Replaces the prior text-token
//! scan of `__ducklake_metadata_<lake>.ducklake_snapshot_changes.changes_made`
//! for cdc_ddl_changes_read / cdc_ddl_changes_query. Backend-agnostic (the upstream probe in
//! `e2e/smoke/output/` confirms all three DuckLake backends emit
//! identical MAP key sets); future-proof against the comma-separated
//! text format moving.
void ExtractDdlRows(duckdb::Connection &conn, const std::string &catalog_name, int64_t start_snapshot,
                    int64_t end_snapshot, const std::string &table_filter,
                    std::vector<std::vector<duckdb::Value>> &rows) {
	// Project (snapshot_id, snapshot_time, key, value-list) by joining
	// `<catalog>.snapshots()` to its own MAP entries. ORDER BY the
	// snapshot_id so per-snapshot rows are contiguous; the secondary
	// `CASE` clause forces `tables_created` to be processed before
	// `tables_altered` within the same snapshot so the Finding-1
	// promotion can register its (snapshot_id, table_id) pair before
	// the ID-resolved branch decides whether to skip itself
	// (Phase 2 follow-up #1). Final ordering of emitted DdlRows
	// within a snapshot is reapplied via `SortDdlRowsForSnapshot`.
	const auto query = std::string("SELECT s.snapshot_id, s.snapshot_time, e.key AS map_key, e.value AS map_values "
	                               "FROM ") +
	                   QuoteIdentifier(catalog_name) +
	                   ".snapshots() s, "
	                   "UNNEST(map_entries(s.changes)) AS u(e) "
	                   "WHERE s.snapshot_id BETWEEN " +
	                   std::to_string(start_snapshot) + " AND " + std::to_string(end_snapshot) +
	                   " ORDER BY s.snapshot_id ASC, "
	                   "CASE e.key WHEN 'tables_created' THEN 0 ELSE 1 END, e.key";
	auto changes = conn.Query(query);
	if (!changes || changes->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, changes ? changes->GetError() : "DDL MAP scan failed");
	}
	int64_t current_snapshot = -1;
	size_t snapshot_begin = rows.size();
	std::unordered_set<int64_t> finding1_promoted_table_ids;
	auto flush_snapshot = [&]() {
		if (current_snapshot == -1) {
			return;
		}
		ApplyDdlTableFilter(rows, snapshot_begin, table_filter);
		SortDdlRowsForSnapshot(rows, snapshot_begin, rows.size());
	};
	for (duckdb::idx_t row_idx = 0; row_idx < changes->RowCount(); ++row_idx) {
		const auto snapshot_id = changes->GetValue(0, row_idx).GetValue<int64_t>();
		const auto snapshot_time = changes->GetValue(1, row_idx);
		const auto key_value = changes->GetValue(2, row_idx);
		const auto values_value = changes->GetValue(3, row_idx);
		if (snapshot_id != current_snapshot) {
			flush_snapshot();
			current_snapshot = snapshot_id;
			snapshot_begin = rows.size();
			finding1_promoted_table_ids.clear();
		}
		if (key_value.IsNull() || values_value.IsNull()) {
			continue;
		}
		const auto map_key = key_value.ToString();
		for (const auto &v : duckdb::ListValue::GetChildren(values_value)) {
			if (v.IsNull()) {
				continue;
			}
			AddDdlRowFromMapEntry(conn, catalog_name, snapshot_id, snapshot_time, map_key, v.ToString(), rows,
			                      finding1_promoted_table_ids);
		}
	}
	flush_snapshot();
}

void ApplyDdlSubscriptionFilter(std::vector<std::vector<duckdb::Value>> &rows,
                                const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	auto out_idx = duckdb::idx_t(0);
	for (duckdb::idx_t i = 0; i < rows.size(); ++i) {
		const auto &row = rows[i];
		if (row.size() < 7) {
			continue;
		}
		const auto schema_value = row[4];
		const auto object_value = row[6];
		const auto object_kind = row[3].IsNull() ? std::string() : row[3].ToString();
		bool keep = false;
		for (const auto &subscription : subscriptions) {
			if (subscription.event_category != "ddl") {
				continue;
			}
			if (subscription.scope_kind == "catalog") {
				keep = true;
			} else if (subscription.scope_kind == "schema" && !subscription.schema_id.IsNull() &&
			           !schema_value.IsNull() &&
			           subscription.schema_id.GetValue<int64_t>() == schema_value.GetValue<int64_t>()) {
				keep = true;
			} else if (subscription.scope_kind == "table" && object_kind == "table" &&
			           !subscription.table_id.IsNull() && !object_value.IsNull() &&
			           subscription.table_id.GetValue<int64_t>() == object_value.GetValue<int64_t>()) {
				keep = true;
			}
			if (keep) {
				break;
			}
		}
		if (keep) {
			if (out_idx != i) {
				rows[out_idx] = rows[i];
			}
			out_idx++;
		}
	}
	rows.resize(out_idx);
}

//===--------------------------------------------------------------------===//
// Stateful DDL changes and ticks
//===--------------------------------------------------------------------===//

struct CdcDdlData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t max_snapshots;
	bool auto_commit = false;
	bool listen = false;
	int64_t timeout_ms = DEFAULT_WAIT_TIMEOUT_MS;
	int64_t poll_min_ms = WAIT_INITIAL_INTERVAL_MS;
	bool explicit_window = false;
	int64_t start_snapshot = -1;
	int64_t end_snapshot = -1;
	bool ticks = false;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcDdlData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->max_snapshots = max_snapshots;
		result->auto_commit = auto_commit;
		result->listen = listen;
		result->timeout_ms = timeout_ms;
		result->poll_min_ms = poll_min_ms;
		result->explicit_window = explicit_window;
		result->start_snapshot = start_snapshot;
		result->end_snapshot = end_snapshot;
		result->ticks = ticks;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

int64_t DdlTimeoutMsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("timeout_ms");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return DEFAULT_WAIT_TIMEOUT_MS;
	}
	return entry->second.GetValue<int64_t>();
}

duckdb::Value DdlBigIntListValue(const std::vector<int64_t> &values) {
	duckdb::vector<duckdb::Value> children;
	children.reserve(values.size());
	for (const auto value : values) {
		children.push_back(duckdb::Value::BIGINT(value));
	}
	return duckdb::Value::LIST(duckdb::LogicalType::BIGINT, children);
}

struct DdlQueryFilter {
	std::unordered_set<int64_t> schema_ids;
	std::unordered_set<int64_t> table_ids;

	bool Empty() const {
		return schema_ids.empty() && table_ids.empty();
	}
};

void ApplyDdlQueryFilter(std::vector<std::vector<duckdb::Value>> &rows, const DdlQueryFilter &filter);

duckdb::unique_ptr<duckdb::FunctionData> CdcDdlBindBase(duckdb::ClientContext &context,
                                                        duckdb::TableFunctionBindInput &input,
                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                        duckdb::vector<duckdb::string> &names, bool listen,
                                                        bool ticks) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_ddl_changes/ticks requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcDdlData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->max_snapshots = MaxSnapshotsParameter(input);
	result->listen = listen;
	result->ticks = ticks;
	result->timeout_ms = DdlTimeoutMsParameter(input);
	result->poll_min_ms = PollMinMsParameter(input);
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
		    "cdc_ddl_changes/ticks requires both start_snapshot and end_snapshot when either is set");
	}
	if (has_start_snapshot) {
		result->explicit_window = true;
		result->start_snapshot = start_snapshot_entry->second.GetValue<int64_t>();
		result->end_snapshot = end_snapshot_entry->second.GetValue<int64_t>();
	}

	names = {"consumer_name", "start_snapshot", "end_snapshot"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	if (ticks) {
		for (const auto &name : {"snapshot_id", "snapshot_time", "schema_version", "changes_made", "schema_ids",
		                         "table_ids", "change_count"}) {
			names.push_back(name);
		}
		return_types.push_back(duckdb::LogicalType::BIGINT);
		return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
		return_types.push_back(duckdb::LogicalType::BIGINT);
		return_types.push_back(duckdb::LogicalType::VARCHAR);
		return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));
		return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));
		return_types.push_back(duckdb::LogicalType::BIGINT);
	} else {
		for (const auto &name : {"snapshot_id", "snapshot_time", "event_kind", "object_kind", "schema_id",
		                         "schema_name", "object_id", "object_name", "details"}) {
			names.push_back(name);
		}
		return_types.push_back(duckdb::LogicalType::BIGINT);
		return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
		return_types.push_back(duckdb::LogicalType::VARCHAR);
		return_types.push_back(duckdb::LogicalType::VARCHAR);
		return_types.push_back(duckdb::LogicalType::BIGINT);
		return_types.push_back(duckdb::LogicalType::VARCHAR);
		return_types.push_back(duckdb::LogicalType::BIGINT);
		return_types.push_back(duckdb::LogicalType::VARCHAR);
		return_types.push_back(duckdb::LogicalType::VARCHAR);
	}
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcDdlReadBind(duckdb::ClientContext &context,
                                                        duckdb::TableFunctionBindInput &input,
                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                        duckdb::vector<duckdb::string> &names) {
	return CdcDdlBindBase(context, input, return_types, names, false, false);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcDdlListenBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	return CdcDdlBindBase(context, input, return_types, names, true, false);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcDdlTicksReadBind(duckdb::ClientContext &context,
                                                             duckdb::TableFunctionBindInput &input,
                                                             duckdb::vector<duckdb::LogicalType> &return_types,
                                                             duckdb::vector<duckdb::string> &names) {
	return CdcDdlBindBase(context, input, return_types, names, false, true);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcDdlTicksListenBind(duckdb::ClientContext &context,
                                                               duckdb::TableFunctionBindInput &input,
                                                               duckdb::vector<duckdb::LogicalType> &return_types,
                                                               duckdb::vector<duckdb::string> &names) {
	return CdcDdlBindBase(context, input, return_types, names, true, true);
}

void AppendDdlTickRows(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                       int64_t start_snapshot, int64_t end_snapshot,
                       const std::vector<ConsumerSubscriptionRow> &subscriptions, const DdlQueryFilter *query_filter,
                       std::vector<std::vector<duckdb::Value>> &out, bool stateful) {
	auto snapshots =
	    conn.Query(std::string("SELECT s.snapshot_id, s.snapshot_time, s.schema_version, c.changes_made FROM ") +
	               MetadataTable(conn, catalog_name, "ducklake_snapshot") + " s JOIN " +
	               MetadataTable(conn, catalog_name, "ducklake_snapshot_changes") +
	               " c USING (snapshot_id) WHERE s.snapshot_id BETWEEN " + std::to_string(start_snapshot) + " AND " +
	               std::to_string(end_snapshot) + " ORDER BY s.snapshot_id ASC");
	if (!snapshots || snapshots->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        snapshots ? snapshots->GetError() : "DDL ticks scan failed");
	}
	for (duckdb::idx_t row_idx = 0; row_idx < snapshots->RowCount(); ++row_idx) {
		const auto snapshot_id = snapshots->GetValue(0, row_idx).GetValue<int64_t>();
		std::vector<std::vector<duckdb::Value>> ddl_rows;
		ExtractDdlRows(conn, catalog_name, snapshot_id, snapshot_id, std::string(), ddl_rows);
		if (!subscriptions.empty()) {
			ApplyDdlSubscriptionFilter(ddl_rows, subscriptions);
		}
		if (query_filter != nullptr) {
			ApplyDdlQueryFilter(ddl_rows, *query_filter);
		}
		if (ddl_rows.empty()) {
			continue;
		}
		std::unordered_set<int64_t> schema_ids;
		std::unordered_set<int64_t> table_ids;
		for (const auto &row : ddl_rows) {
			if (row.size() > 4 && !row[4].IsNull()) {
				schema_ids.insert(row[4].GetValue<int64_t>());
			}
			if (row.size() > 6 && !row[6].IsNull() && !row[3].IsNull() && row[3].ToString() == "table") {
				table_ids.insert(row[6].GetValue<int64_t>());
			}
		}
		std::vector<int64_t> schema_id_list(schema_ids.begin(), schema_ids.end());
		std::vector<int64_t> table_id_list(table_ids.begin(), table_ids.end());
		std::sort(schema_id_list.begin(), schema_id_list.end());
		std::sort(table_id_list.begin(), table_id_list.end());
		std::vector<duckdb::Value> tick;
		if (stateful) {
			tick.push_back(duckdb::Value(consumer_name));
			tick.push_back(duckdb::Value::BIGINT(start_snapshot));
			tick.push_back(duckdb::Value::BIGINT(end_snapshot));
		}
		tick.push_back(snapshots->GetValue(0, row_idx));
		tick.push_back(snapshots->GetValue(1, row_idx));
		tick.push_back(snapshots->GetValue(2, row_idx));
		tick.push_back(snapshots->GetValue(3, row_idx));
		tick.push_back(DdlBigIntListValue(schema_id_list));
		tick.push_back(DdlBigIntListValue(table_id_list));
		tick.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(ddl_rows.size())));
		out.push_back(std::move(tick));
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcDdlInit(duckdb::ClientContext &context,
                                                                duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcDdlData>();
	auto max_snapshots = data.max_snapshots;
	if (!data.explicit_window) {
		// Both stateful entry points must probe for subscription-relevant DDL.
		// A non-blocking read uses a zero timeout; the probe still durably
		// advances a DDL cursor across an inspected DML-only window.
		const auto timeout_ms = data.listen ? data.timeout_ms : 0;
		auto ready =
		    WaitForConsumerSnapshot(context, data.catalog_name, data.consumer_name, timeout_ms, data.poll_min_ms);
		if (ready.empty() || ready[0].IsNull()) {
			return std::move(result);
		}
		if (ready.size() > 1 && !ready[1].IsNull()) {
			max_snapshots = std::max<int64_t>(max_snapshots, ready[1].GetValue<int64_t>());
		}
	}

	duckdb::Connection conn_filter(*context.db);
	const auto subscriptions = LoadConsumerSubscriptions(conn_filter, data.catalog_name, data.consumer_name);
	bool has_ddl_subscription = false;
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category == "ddl") {
			has_ddl_subscription = true;
			break;
		}
	}
	if (!has_ddl_subscription) {
		return std::move(result);
	}
	int64_t start_snapshot;
	int64_t end_snapshot;
	bool has_changes;
	bool schema_changes_pending = false;
	if (data.explicit_window) {
		start_snapshot = data.start_snapshot;
		end_snapshot = data.end_snapshot;
		has_changes = end_snapshot >= start_snapshot;
	} else {
		CdcWindowData window_data;
		window_data.catalog_name = data.catalog_name;
		window_data.consumer_name = data.consumer_name;
		window_data.max_snapshots = max_snapshots;
		auto window = ReadWindow(context, window_data);
		start_snapshot = window[0].GetValue<int64_t>();
		end_snapshot = window[1].GetValue<int64_t>();
		has_changes = window[2].GetValue<bool>();
		schema_changes_pending = window[4].GetValue<bool>();
	}
	if (!has_changes && schema_changes_pending) {
		end_snapshot = start_snapshot;
	} else if (!has_changes) {
		return std::move(result);
	}

	duckdb::Connection conn(*context.db);
	if (data.ticks) {
		AppendDdlTickRows(conn, data.catalog_name, data.consumer_name, start_snapshot, end_snapshot, subscriptions,
		                  nullptr, result->rows, true);
	} else {
		std::vector<std::vector<duckdb::Value>> payload_rows;
		ExtractDdlRows(conn, data.catalog_name, start_snapshot, end_snapshot, std::string(), payload_rows);
		ApplyDdlSubscriptionFilter(payload_rows, subscriptions);
		for (const auto &payload : payload_rows) {
			std::vector<duckdb::Value> row;
			row.push_back(duckdb::Value(data.consumer_name));
			row.push_back(duckdb::Value::BIGINT(start_snapshot));
			row.push_back(duckdb::Value::BIGINT(end_snapshot));
			row.insert(row.end(), payload.begin(), payload.end());
			result->rows.push_back(std::move(row));
		}
	}
	if (data.auto_commit && !data.explicit_window) {
		CommitConsumerSnapshot(context, data.catalog_name, data.consumer_name, end_snapshot);
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_ddl_changes_query
//===--------------------------------------------------------------------===//

struct CdcRangeDdlData : public duckdb::TableFunctionData {
	std::string catalog_name;
	int64_t from_snapshot;
	int64_t to_snapshot;
	std::vector<std::string> schemas;
	std::vector<int64_t> schema_ids;
	std::vector<std::string> table_names;
	std::vector<int64_t> table_ids;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcRangeDdlData>();
		result->catalog_name = catalog_name;
		result->from_snapshot = from_snapshot;
		result->to_snapshot = to_snapshot;
		result->schemas = schemas;
		result->schema_ids = schema_ids;
		result->table_names = table_names;
		result->table_ids = table_ids;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

int64_t RangeDdlToSnapshotParameter(duckdb::Connection &conn, duckdb::TableFunctionBindInput &input,
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

void ValidateDdlRangeBounds(duckdb::Connection &conn, const std::string &catalog_name, int64_t from_snapshot,
                            int64_t to_snapshot) {
	if (to_snapshot < from_snapshot) {
		throw duckdb::InvalidInputException("cdc_ddl_changes_query: from_snapshot must be <= to_snapshot");
	}
	auto oldest_result = conn.Query("SELECT COALESCE(min(snapshot_id), 0) FROM " +
	                                MetadataTable(conn, catalog_name, "ducklake_snapshot"));
	const auto oldest_snapshot = SingleInt64(*oldest_result, "oldest available snapshot");
	if (from_snapshot < oldest_snapshot) {
		throw duckdb::InvalidInputException(
		    "cdc_ddl_changes_query: from_snapshot %lld is older than oldest available snapshot %lld",
		    static_cast<long long>(from_snapshot), static_cast<long long>(oldest_snapshot));
	}
}

std::vector<std::string> DdlStringListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name) {
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return {};
	}
	if (entry->second.type().id() != duckdb::LogicalTypeId::LIST) {
		throw duckdb::BinderException("%s must be a LIST<VARCHAR>", name);
	}
	std::vector<std::string> values;
	for (const auto &child : duckdb::ListValue::GetChildren(entry->second)) {
		if (!child.IsNull()) {
			values.push_back(child.ToString());
		}
	}
	return values;
}

std::vector<int64_t> DdlInt64ListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name) {
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return {};
	}
	if (entry->second.type().id() != duckdb::LogicalTypeId::LIST) {
		throw duckdb::BinderException("%s must be a LIST<BIGINT>", name);
	}
	std::vector<int64_t> values;
	for (const auto &child : duckdb::ListValue::GetChildren(entry->second)) {
		if (!child.IsNull()) {
			values.push_back(child.GetValue<int64_t>());
		}
	}
	return values;
}

int64_t ResolveDdlSchemaName(duckdb::Connection &conn, const std::string &catalog_name, const std::string &schema_name,
                             int64_t snapshot_id) {
	auto rows = conn.Query("SELECT schema_id FROM " + MetadataTable(conn, catalog_name, "ducklake_schema") +
	                       " WHERE schema_name = " + QuoteLiteral(schema_name) + " AND begin_snapshot <= " +
	                       std::to_string(snapshot_id) + " AND (end_snapshot IS NULL OR end_snapshot > " +
	                       std::to_string(snapshot_id) + ") ORDER BY schema_id DESC LIMIT 1");
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "DDL schema filter resolution failed");
	}
	if (rows->RowCount() == 0) {
		throw duckdb::InvalidInputException("failed to resolve schema '%s' for DDL query filter", schema_name);
	}
	return rows->GetValue(0, 0).GetValue<int64_t>();
}

DdlQueryFilter BuildDdlQueryFilter(duckdb::Connection &conn, const CdcRangeDdlData &data) {
	DdlQueryFilter filter;
	filter.schema_ids.insert(data.schema_ids.begin(), data.schema_ids.end());
	filter.table_ids.insert(data.table_ids.begin(), data.table_ids.end());
	for (const auto &schema_name : data.schemas) {
		filter.schema_ids.insert(ResolveDdlSchemaName(conn, data.catalog_name, schema_name, data.to_snapshot));
	}
	for (const auto &table_name_input : data.table_names) {
		auto table_name = table_name_input.find('.') == std::string::npos ? std::string("main.") + table_name_input
		                                                                  : table_name_input;
		int64_t schema_id = 0;
		int64_t table_id = 0;
		if (!ResolveCurrentTableName(conn, data.catalog_name, table_name, data.to_snapshot, schema_id, table_id)) {
			throw duckdb::InvalidInputException("failed to resolve table '%s' for DDL query filter", table_name);
		}
		filter.table_ids.insert(table_id);
	}
	return filter;
}

void ApplyDdlQueryFilter(std::vector<std::vector<duckdb::Value>> &rows, const DdlQueryFilter &filter) {
	if (filter.Empty()) {
		return;
	}
	rows.erase(std::remove_if(rows.begin(), rows.end(),
	                          [&](const std::vector<duckdb::Value> &row) {
		                          const bool schema_matches = !filter.schema_ids.empty() && row.size() > 4 &&
		                                                      !row[4].IsNull() &&
		                                                      filter.schema_ids.count(row[4].GetValue<int64_t>()) > 0;
		                          const bool table_matches = !filter.table_ids.empty() && row.size() > 6 &&
		                                                     !row[6].IsNull() && !row[3].IsNull() &&
		                                                     row[3].ToString() == "table" &&
		                                                     filter.table_ids.count(row[6].GetValue<int64_t>()) > 0;
		                          return !schema_matches && !table_matches;
	                          }),
	           rows.end());
}

duckdb::unique_ptr<duckdb::FunctionData> CdcRangeDdlBind(duckdb::ClientContext &context,
                                                         duckdb::TableFunctionBindInput &input,
                                                         duckdb::vector<duckdb::LogicalType> &return_types,
                                                         duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2 && input.inputs.size() != 3) {
		throw duckdb::BinderException(
		    "cdc_ddl_changes_query requires catalog, from_snapshot, and optional to_snapshot");
	}
	auto result = duckdb::make_uniq<CdcRangeDdlData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->from_snapshot = input.inputs[1].GetValue<int64_t>();
	result->schemas = DdlStringListNamedParameter(input, "schemas");
	result->schema_ids = DdlInt64ListNamedParameter(input, "schema_ids");
	result->table_names = DdlStringListNamedParameter(input, "table_names");
	result->table_ids = DdlInt64ListNamedParameter(input, "table_ids");
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, result->catalog_name);
	result->to_snapshot = RangeDdlToSnapshotParameter(conn, input, result->catalog_name, 2);
	ValidateDdlRangeBounds(conn, result->catalog_name, result->from_snapshot, result->to_snapshot);

	names = {"snapshot_id", "snapshot_time", "event_kind",  "object_kind", "schema_id",
	         "schema_name", "object_id",     "object_name", "details"};
	return_types = {duckdb::LogicalType::BIGINT,  duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,       duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcRangeDdlInit(duckdb::ClientContext &context,
                                                                     duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcRangeDdlData>();
	duckdb::Connection conn(*context.db);
	ExtractDdlRows(conn, data.catalog_name, data.from_snapshot, data.to_snapshot, std::string(), result->rows);
	const auto filter = BuildDdlQueryFilter(conn, data);
	ApplyDdlQueryFilter(result->rows, filter);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> DdlTicksQueryBind(duckdb::ClientContext &context,
                                                           duckdb::TableFunctionBindInput &input,
                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                           duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2 && input.inputs.size() != 3) {
		throw duckdb::BinderException("cdc_ddl_ticks_query requires catalog, from_snapshot, and optional to_snapshot");
	}
	auto result = duckdb::make_uniq<CdcRangeDdlData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->from_snapshot = input.inputs[1].GetValue<int64_t>();
	result->schemas = DdlStringListNamedParameter(input, "schemas");
	result->schema_ids = DdlInt64ListNamedParameter(input, "schema_ids");
	result->table_names = DdlStringListNamedParameter(input, "table_names");
	result->table_ids = DdlInt64ListNamedParameter(input, "table_ids");
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, result->catalog_name);
	result->to_snapshot = input.inputs.size() > 2 && !input.inputs[2].IsNull()
	                          ? input.inputs[2].GetValue<int64_t>()
	                          : CurrentSnapshot(conn, result->catalog_name);
	auto to_entry = input.named_parameters.find("to_snapshot");
	if (to_entry != input.named_parameters.end() && !to_entry->second.IsNull()) {
		result->to_snapshot = to_entry->second.GetValue<int64_t>();
	}
	ValidateDdlRangeBounds(conn, result->catalog_name, result->from_snapshot, result->to_snapshot);
	names = {"snapshot_id", "snapshot_time", "schema_version", "changes_made",
	         "schema_ids",  "table_ids",     "change_count"};
	return_types = {duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT),
	                duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT),
	                duckdb::LogicalType::BIGINT};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DdlTicksQueryInit(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcRangeDdlData>();
	duckdb::Connection conn(*context.db);
	const std::vector<ConsumerSubscriptionRow> no_subscriptions;
	const auto filter = BuildDdlQueryFilter(conn, data);
	const auto *filter_ptr = filter.Empty() ? nullptr : &filter;
	AppendDdlTickRows(conn, data.catalog_name, std::string(), data.from_snapshot, data.to_snapshot, no_subscriptions,
	                  filter_ptr, result->rows, false);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_schema_diff
//===--------------------------------------------------------------------===//

struct CdcSchemaDiffData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string table_filter;
	int64_t from_snapshot;
	int64_t to_snapshot;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcSchemaDiffData>();
		result->catalog_name = catalog_name;
		result->table_filter = table_filter;
		result->from_snapshot = from_snapshot;
		result->to_snapshot = to_snapshot;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcSchemaDiffBind(duckdb::ClientContext &context,
                                                           duckdb::TableFunctionBindInput &input,
                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                           duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 4) {
		throw duckdb::BinderException("cdc_schema_diff requires catalog, table, from_snapshot, and to_snapshot");
	}
	auto result = duckdb::make_uniq<CdcSchemaDiffData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->table_filter = GetStringArg(input.inputs[1], "table");
	if (result->table_filter.find('.') == std::string::npos) {
		result->table_filter = std::string("main.") + result->table_filter;
	}
	result->from_snapshot = input.inputs[2].GetValue<int64_t>();
	result->to_snapshot = input.inputs[3].GetValue<int64_t>();
	if (result->from_snapshot < 0 || result->to_snapshot < result->from_snapshot) {
		throw duckdb::InvalidInputException(
		    "cdc_schema_diff: invalid snapshot range [%lld, %lld]; require 0 <= from <= to",
		    static_cast<long long>(result->from_snapshot), static_cast<long long>(result->to_snapshot));
	}
	CheckCatalogOrThrow(context, result->catalog_name);

	names = {"snapshot_id",  "snapshot_time", "change_kind",     "column_id",   "old_name",
	         "new_name",     "old_type",      "new_type",        "old_default", "new_default",
	         "old_nullable", "new_nullable",  "parent_column_id"};
	return_types = {duckdb::LogicalType::BIGINT,  duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN,      duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BIGINT};
	return std::move(result);
}

//! Resolve the user-supplied `schema.table` filter into the set of
//! `table_id` values that *ever* had that fully-qualified name within
//! `[from_snapshot, to_snapshot]` (or that have it now). Renames preserve
//! `table_id`, so a single id is the typical case; the set form is here to
//! survive drop+recreate-with-same-name sequences without silently
//! conflating two distinct tables. Returning by id (not by name) means
//! cdc_schema_diff catches the original CREATE event under the old name
//! AND the rename + subsequent ALTERs under the new name in one sweep.
std::unordered_set<int64_t> ResolveTableIdsForFilter(duckdb::Connection &conn, const std::string &catalog_name,
                                                     const std::string &qualified_name, int64_t from_snapshot,
                                                     int64_t to_snapshot) {
	std::unordered_set<int64_t> ids;
	const auto dot = qualified_name.find('.');
	if (dot == std::string::npos) {
		return ids;
	}
	const auto schema_name = qualified_name.substr(0, dot);
	const auto table_name = qualified_name.substr(dot + 1);
	auto result = conn.Query("SELECT DISTINCT t.table_id FROM " + MetadataTable(conn, catalog_name, "ducklake_table") +
	                         " t JOIN " + MetadataTable(conn, catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE s.schema_name = " + QuoteLiteral(schema_name) +
	                         " AND t.table_name = " + QuoteLiteral(table_name) +
	                         " AND t.begin_snapshot <= " + std::to_string(to_snapshot) +
	                         " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(from_snapshot) + ")");
	if (!result || result->HasError()) {
		return ids;
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		const auto v = result->GetValue(0, i);
		if (!v.IsNull()) {
			ids.insert(v.GetValue<int64_t>());
		}
	}
	return ids;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcSchemaDiffInit(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcSchemaDiffData>();

	duckdb::Connection conn(*context.db);
	const auto target_ids =
	    ResolveTableIdsForFilter(conn, data.catalog_name, data.table_filter, data.from_snapshot, data.to_snapshot);
	if (target_ids.empty()) {
		// No table_id ever bore this qualified name within the range.
		// Empty result - safer than guessing an id.
		return std::move(result);
	}

	// Phase 2 deferral 1: Stage-1 source switched to the typed
	// `<lake>.snapshots().changes` MAP form. Each row of the join below
	// is `(snapshot_id, snapshot_time, map_key, value-list)`; we only
	// care about `tables_created` (Finding-1 detection promotes a
	// rename to `altered.table`) and `tables_altered` (direct ALTER on
	// a table id). Other MAP keys (`schemas_*`, `views_*`, DML, and
	// maintenance) are irrelevant for cdc_schema_diff. Each surfaced
	// table_id is checked against `target_ids` so renames do not
	// fragment the timeline. Per-snapshot output: zero or one
	// `table_rename` row + zero or more per-column diff rows.
	//
	// The secondary `CASE` ORDER BY mirrors `ExtractDdlRows`: process
	// `tables_created` first so the Finding-1 path can register its
	// (snapshot_id, table_id) pair before the ID-resolved branch
	// decides whether to skip itself (Phase 2 follow-up #1). Without
	// dedup, a transaction that renames + alters the same table would
	// emit two table_rename / per-column-diff blocks for one snapshot.
	const auto changes_sql =
	    std::string("SELECT s.snapshot_id, s.snapshot_time, e.key AS map_key, e.value AS map_values FROM ") +
	    QuoteIdentifier(data.catalog_name) +
	    ".snapshots() s, UNNEST(map_entries(s.changes)) AS u(e) WHERE s.snapshot_id BETWEEN " +
	    std::to_string(data.from_snapshot) + " AND " + std::to_string(data.to_snapshot) +
	    " AND e.key IN ('tables_created', 'tables_altered') ORDER BY s.snapshot_id ASC, "
	    "CASE e.key WHEN 'tables_created' THEN 0 ELSE 1 END";
	auto changes = QueryWithH022Retry(conn, changes_sql);
	if (!changes || changes->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        changes ? changes->GetError() : "cdc_schema_diff MAP scan failed");
	}
	int64_t current_snapshot = -1;
	std::unordered_set<int64_t> finding1_promoted_table_ids;
	for (duckdb::idx_t row_idx = 0; row_idx < changes->RowCount(); ++row_idx) {
		const auto snapshot_id = changes->GetValue(0, row_idx).GetValue<int64_t>();
		const auto snapshot_time = changes->GetValue(1, row_idx);
		const auto key_value = changes->GetValue(2, row_idx);
		const auto values_value = changes->GetValue(3, row_idx);
		if (snapshot_id != current_snapshot) {
			current_snapshot = snapshot_id;
			finding1_promoted_table_ids.clear();
		}
		if (key_value.IsNull() || values_value.IsNull()) {
			continue;
		}
		const auto map_key = key_value.ToString();
		for (const auto &v : duckdb::ListValue::GetChildren(values_value)) {
			if (v.IsNull()) {
				continue;
			}
			const auto value = v.ToString();
			std::string schema_name;
			std::string object_name;
			int64_t table_id = 0;
			std::string old_table_name;
			std::string new_table_name;
			bool is_pure_create = false;

			if (map_key == "tables_created") {
				const auto dot = value.find('.');
				if (dot == std::string::npos) {
					continue;
				}
				schema_name = value.substr(0, dot);
				object_name = value.substr(dot + 1);
				const auto object = LookupTableByName(conn, data.catalog_name, snapshot_id, schema_name, object_name);
				if (object.object_id.IsNull()) {
					continue;
				}
				table_id = object.object_id.GetValue<int64_t>();
				if (target_ids.find(table_id) == target_ids.end()) {
					continue;
				}
				const auto previous = PreviousTableName(conn, data.catalog_name, table_id, snapshot_id);
				if (previous.IsNull()) {
					is_pure_create = true;
				} else if (previous.ToString() != object_name) {
					old_table_name = previous.ToString();
					new_table_name = object_name;
					// Finding-1 promoted this id at this snapshot —
					// mark so the sibling `tables_altered:<id>` row
					// skips its own (smaller) emission.
					finding1_promoted_table_ids.insert(table_id);
				}
			} else {
				// map_key == "tables_altered"
				if (!TryParseInt64(value, table_id)) {
					continue;
				}
				if (target_ids.find(table_id) == target_ids.end()) {
					continue;
				}
				// Phase 2 follow-up #1: dedup against Finding-1 path.
				if (finding1_promoted_table_ids.count(table_id) > 0) {
					continue;
				}
			}

			if (is_pure_create) {
				const auto new_cols = LookupTableColumnsAt(conn, data.catalog_name, table_id, snapshot_id);
				// Stable order: parents before children, by depth in the
				// nested-struct tree. LookupTableColumnsAt already returns
				// in column_order which puts the parent struct ahead of
				// its fields naturally; the explicit sort makes the
				// guarantee independent of catalog ordering.
				std::unordered_map<int64_t, int64_t> create_parent_of;
				for (const auto &c : new_cols) {
					if (!c.parent_column.IsNull()) {
						create_parent_of[c.column_id] = c.parent_column.GetValue<int64_t>();
					}
				}
				auto create_depth = [&](int64_t column_id) {
					int depth = 0;
					auto cursor = column_id;
					while (true) {
						auto it = create_parent_of.find(cursor);
						if (it == create_parent_of.end()) {
							return depth;
						}
						++depth;
						cursor = it->second;
						if (depth > 64) {
							return depth;
						}
					}
				};
				std::vector<ColumnInfo> ordered = new_cols;
				std::stable_sort(ordered.begin(), ordered.end(), [&](const ColumnInfo &a, const ColumnInfo &b) {
					return create_depth(a.column_id) < create_depth(b.column_id);
				});
				for (const auto &c : ordered) {
					duckdb::Value parent_value = c.parent_column.IsNull()
					                                 ? duckdb::Value()
					                                 : duckdb::Value::BIGINT(c.parent_column.GetValue<int64_t>());
					result->rows.push_back(
					    {duckdb::Value::BIGINT(snapshot_id), snapshot_time, duckdb::Value("added"),
					     duckdb::Value::BIGINT(c.column_id), duckdb::Value(), duckdb::Value(c.column_name),
					     duckdb::Value(), duckdb::Value(c.column_type), duckdb::Value(),
					     c.default_value.IsNull() ? duckdb::Value() : duckdb::Value(c.default_value.ToString()),
					     duckdb::Value(), duckdb::Value::BOOLEAN(c.nullable), parent_value});
				}
				continue;
			}

			if (!old_table_name.empty() && old_table_name != new_table_name) {
				result->rows.push_back(
				    {duckdb::Value::BIGINT(snapshot_id), snapshot_time, duckdb::Value("table_rename"), duckdb::Value(),
				     duckdb::Value(old_table_name), duckdb::Value(new_table_name), duckdb::Value(), duckdb::Value(),
				     duckdb::Value(), duckdb::Value(), duckdb::Value(), duckdb::Value(), duckdb::Value()});
			}

			std::vector<ColumnDiff> diffs;
			if (TableSchemaVersionBumpedAt(conn, data.catalog_name, table_id, snapshot_id)) {
				const auto old_cols = LookupTableColumnsAt(conn, data.catalog_name, table_id, snapshot_id - 1);
				const auto new_cols = LookupTableColumnsAt(conn, data.catalog_name, table_id, snapshot_id);
				diffs = DiffColumns(old_cols, new_cols);
				NormaliseDiffParentChildOrdering(diffs);
			}
			for (const auto &d : diffs) {
				duckdb::Value old_default =
				    d.old_default.IsNull() ? duckdb::Value() : duckdb::Value(d.old_default.ToString());
				duckdb::Value new_default =
				    d.new_default.IsNull() ? duckdb::Value() : duckdb::Value(d.new_default.ToString());
				duckdb::Value old_name = d.old_name.empty() ? duckdb::Value() : duckdb::Value(d.old_name);
				duckdb::Value new_name = d.new_name.empty() ? duckdb::Value() : duckdb::Value(d.new_name);
				duckdb::Value old_type = d.old_type.empty() ? duckdb::Value() : duckdb::Value(d.old_type);
				duckdb::Value new_type = d.new_type.empty() ? duckdb::Value() : duckdb::Value(d.new_type);
				duckdb::Value old_nullable =
				    (d.kind == ColumnDiff::Kind::DROPPED || d.kind == ColumnDiff::Kind::NULLABLE_CHANGED)
				        ? duckdb::Value::BOOLEAN(d.old_nullable)
				        : duckdb::Value();
				duckdb::Value new_nullable =
				    (d.kind == ColumnDiff::Kind::ADDED || d.kind == ColumnDiff::Kind::NULLABLE_CHANGED)
				        ? duckdb::Value::BOOLEAN(d.new_nullable)
				        : duckdb::Value();
				duckdb::Value parent_value = d.has_parent ? duckdb::Value::BIGINT(d.parent_column_id) : duckdb::Value();
				result->rows.push_back({duckdb::Value::BIGINT(snapshot_id), snapshot_time,
				                        duckdb::Value(DiffKindToString(d.kind)), duckdb::Value::BIGINT(d.column_id),
				                        old_name, new_name, old_type, new_type, old_default, new_default, old_nullable,
				                        new_nullable, parent_value});
			}
		}
	}
	return std::move(result);
}

} // namespace

//===--------------------------------------------------------------------===//
// Public surface (declared in ddl.hpp)
//===--------------------------------------------------------------------===//

void RegisterDdlFunctions(duckdb::ExtensionLoader &loader) {
	for (const auto &name :
	     {"cdc_ddl_changes_read", "cdc_ddl_changes_listen", "cdc_ddl_ticks_read", "cdc_ddl_ticks_listen"}) {
		duckdb::table_function_bind_t bind;
		const auto name_string = std::string(name);
		if (name_string.find("_ticks_") != std::string::npos) {
			bind = name_string.find("_listen") == std::string::npos ? CdcDdlTicksReadBind : CdcDdlTicksListenBind;
		} else {
			bind = name_string.find("_listen") == std::string::npos ? CdcDdlReadBind : CdcDdlListenBind;
		}
		duckdb::TableFunction ddl_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, bind, CdcDdlInit);
		ddl_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		ddl_function.named_parameters["start_snapshot"] = duckdb::LogicalType::BIGINT;
		ddl_function.named_parameters["end_snapshot"] = duckdb::LogicalType::BIGINT;
		ddl_function.named_parameters["timeout_ms"] = duckdb::LogicalType::BIGINT;
		ddl_function.named_parameters["poll_min_ms"] = duckdb::LogicalType::BIGINT;
		ddl_function.named_parameters["auto_commit"] = duckdb::LogicalType::BOOLEAN;
		loader.RegisterFunction(ddl_function);
	}

	for (const auto &name : {"cdc_ddl_changes_query"}) {
		duckdb::TableFunction ddl_changes_query_function_2(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
		    RowScanExecute, CdcRangeDdlBind, CdcRangeDdlInit);
		ddl_changes_query_function_2.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		ddl_changes_query_function_2.named_parameters["schemas"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		ddl_changes_query_function_2.named_parameters["schema_ids"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		ddl_changes_query_function_2.named_parameters["table_names"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		ddl_changes_query_function_2.named_parameters["table_ids"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		loader.RegisterFunction(ddl_changes_query_function_2);
		duckdb::TableFunction ddl_changes_query_function_3(
		    name,
		    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
		                                         duckdb::LogicalType::BIGINT},
		    RowScanExecute, CdcRangeDdlBind, CdcRangeDdlInit);
		ddl_changes_query_function_3.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		ddl_changes_query_function_3.named_parameters["schemas"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		ddl_changes_query_function_3.named_parameters["schema_ids"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		ddl_changes_query_function_3.named_parameters["table_names"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		ddl_changes_query_function_3.named_parameters["table_ids"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		loader.RegisterFunction(ddl_changes_query_function_3);
	}

	for (const auto &name : {"cdc_ddl_ticks_query"}) {
		duckdb::TableFunction ticks_query_2(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
		    RowScanExecute, DdlTicksQueryBind, DdlTicksQueryInit);
		ticks_query_2.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		ticks_query_2.named_parameters["schemas"] = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		ticks_query_2.named_parameters["schema_ids"] = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		ticks_query_2.named_parameters["table_names"] = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		ticks_query_2.named_parameters["table_ids"] = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		loader.RegisterFunction(ticks_query_2);
		duckdb::TableFunction ticks_query_3(name,
		                                    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR,
		                                                                         duckdb::LogicalType::BIGINT,
		                                                                         duckdb::LogicalType::BIGINT},
		                                    RowScanExecute, DdlTicksQueryBind, DdlTicksQueryInit);
		ticks_query_3.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		ticks_query_3.named_parameters["schemas"] = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		ticks_query_3.named_parameters["schema_ids"] = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		ticks_query_3.named_parameters["table_names"] = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		ticks_query_3.named_parameters["table_ids"] = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		loader.RegisterFunction(ticks_query_3);
	}

	for (const auto &name : {"cdc_schema_diff"}) {
		duckdb::TableFunction schema_diff_function(
		    name,
		    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
		                                         duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
		    RowScanExecute, CdcSchemaDiffBind, CdcSchemaDiffInit);
		loader.RegisterFunction(schema_diff_function);
	}
}

} // namespace duckdb_cdc
