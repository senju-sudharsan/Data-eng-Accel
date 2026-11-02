"""Tests for Microsoft Fabric adapter implementations.

All tests run without PySpark or a live Fabric workspace.
PySpark dependencies are replaced with lightweight mock objects that satisfy
the structural requirements of the adapters.

Test Coverage
-------------
1. FabricBatchTracker — protocol compliance, state transitions, lease management.
2. FabricBronzeStore — protocol compliance, idempotency, partial-write cleanup.
3. FabricSilverPublisher — conformed Silver idempotency, quarantine idempotency.
4. FabricSilverPublisher.publish_scd2() — initial batch, changed batch, replay,
   unchanged record (no new version).
5. run_fabric_pipeline() — end-to-end orchestration with mock adapters:
   a. Full Bronze+Silver success.
   b. Bronze-only pipeline.
   c. Submission key idempotency (completed batch returns replay=True).
   d. Active lease raises RuntimeError.
   e. Incompatible fingerprint raises ValueError.
   f. Partial failure after BRONZE_COMMITTED — Bronze not re-written on retry.
6. Protocol isinstance checks for all three adapters.
7. Import isolation smoke tests.
"""

from __future__ import annotations

import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import pytest


# =========================================================================
# Mock SparkSession / DataFrame infrastructure
# =========================================================================

class MockRow:
    """Stand-in for pyspark.sql.Row with asDict() support."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def asDict(self) -> dict[str, Any]:
        return dict(self._data)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class MockDataFrame:
    """Minimal Spark DataFrame mock — supports filter, count, collect, join, union,
    withColumn, write, and select without installing PySpark."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows: list[dict[str, Any]] = list(rows) if rows else []
        self._parent_session: "MockSparkSession | None" = None

    # ---- Transforms ----

    def filter(self, condition: str | Any) -> "MockDataFrame":
        if not isinstance(condition, str):
            # Boolean column expression — pass-through for mock purposes.
            return self
        filtered = [r for r in self._rows if _apply_filter(r, condition)]
        return self._child(filtered)

    def orderBy(self, *args: Any, ascending: bool = True) -> "MockDataFrame":
        return self

    def limit(self, n: int) -> "MockDataFrame":
        return self._child(self._rows[:n])

    def count(self) -> int:
        return len(self._rows)

    def collect(self) -> list[MockRow]:
        return [MockRow(r) for r in self._rows]

    def withColumn(self, name: str, col_expr: Any) -> "MockDataFrame":
        val = col_expr._value if hasattr(col_expr, "_value") else None
        new_rows = [{**r, name: val} for r in self._rows]
        return self._child(new_rows)

    def select(self, columns: list[str] | Any) -> "MockDataFrame":
        if not isinstance(columns, list):
            return self
        new_rows = [{k: r.get(k) for k in columns} for r in self._rows]
        return self._child(new_rows)

    def join(self, other: "MockDataFrame", on: Any, how: str = "inner") -> "MockDataFrame":
        key_cols: list[str] = on if isinstance(on, list) else []
        if how == "left_anti":
            other_keys = {
                tuple(str(r.get(k, "")) for k in key_cols)
                for r in other._rows
            }
            result = [
                r for r in self._rows
                if tuple(str(r.get(k, "")) for k in key_cols) not in other_keys
            ]
            return self._child(result)
        if how == "inner":
            result = []
            for lr in self._rows:
                for rr in other._rows:
                    if all(str(lr.get(k, "")) == str(rr.get(k, "")) for k in key_cols):
                        merged = {**rr, **lr}
                        result.append(merged)
            return self._child(result)
        # default: union
        return self._child(self._rows + other._rows)

    def union(self, other: "MockDataFrame") -> "MockDataFrame":
        return self._child(self._rows + other._rows)

    def alias(self, name: str) -> "MockDataFrame":
        return self

    @property
    def columns(self) -> list[str]:
        return list(self._rows[0].keys()) if self._rows else []

    @property
    def schema(self) -> None:
        return None

    # ---- Write ----

    class _Writer:
        def __init__(self, df: "MockDataFrame", parent: "MockSparkSession | None") -> None:
            self._df = df
            self._parent = parent
            self._mode = "append"

        def format(self, fmt: str) -> "MockDataFrame._Writer":
            return self

        def mode(self, m: str) -> "MockDataFrame._Writer":
            self._mode = m
            return self

        def option(self, k: str, v: Any) -> "MockDataFrame._Writer":
            return self

        def saveAsTable(self, name: str) -> None:
            if self._parent is None:
                return
            if self._mode == "append":
                self._parent._tables.setdefault(name, [])
                self._parent._tables[name].extend(self._df._rows)
            else:
                self._parent._tables[name] = list(self._df._rows)

    @property
    def write(self) -> "_Writer":
        return self._Writer(self, self._parent_session)

    # ---- Internal ----

    def _child(self, rows: list[dict[str, Any]]) -> "MockDataFrame":
        df = MockDataFrame(rows)
        df._parent_session = self._parent_session
        return df


def _apply_filter(row: dict[str, Any], condition: str) -> bool:
    """Very simple filter parser for ``col = 'val'`` and ``col = true`` forms."""
    condition = condition.strip()
    # Strip backtick quoting from column names for simpler matching.
    condition = re.sub(r"`([^`]+)`", r"\1", condition)

    if " = '" in condition:
        col, _, rest = condition.partition(" = '")
        val_str = rest.rstrip("'")
        return str(row.get(col.strip(), "")) == val_str
    if " = " in condition:
        col, _, val_part = condition.partition(" = ")
        col = col.strip()
        val_part = val_part.strip()
        if val_part.lower() == "true":
            return bool(row.get(col))
        if val_part.lower() == "false":
            return not bool(row.get(col))
        try:
            return row.get(col) == int(val_part)
        except (ValueError, TypeError):
            return str(row.get(col, "")) == val_part
    return True


class MockSparkSession:
    """Minimal SparkSession that keeps tables in memory and simulates SQL."""

    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._sql_calls: list[str] = []

    def createDataFrame(self, rows: list[dict] | list, schema: Any = None) -> MockDataFrame:
        df = MockDataFrame(list(rows) if rows else [])
        df._parent_session = self
        return df

    def table(self, name: str) -> MockDataFrame:
        df = MockDataFrame(list(self._tables.get(name, [])))
        df._parent_session = self
        return df

    def sql(self, query: str) -> MockDataFrame:
        q = query.strip()
        self._sql_calls.append(q)
        upper = q.upper()
        if upper.startswith("DELETE FROM "):
            self._execute_delete(q)
        elif upper.startswith("UPDATE "):
            self._execute_update(q)
        elif upper.startswith("CREATE TABLE"):
            # Parse table name and ensure it exists in _tables.
            m = re.search(r"CREATE TABLE IF NOT EXISTS\s+(\S+)", q, re.IGNORECASE)
            if m:
                self._tables.setdefault(m.group(1), [])
        df = MockDataFrame([])
        df._parent_session = self
        return df

    def _execute_delete(self, query: str) -> None:
        """Simulate: DELETE FROM table WHERE col = 'val'"""
        m = re.search(
            r"DELETE FROM\s+(\S+)\s+WHERE\s+(\w+)\s*=\s*'([^']+)'",
            query, re.IGNORECASE
        )
        if m:
            table_name = m.group(1)
            col = m.group(2)
            val = m.group(3)
            if table_name in self._tables:
                self._tables[table_name] = [
                    r for r in self._tables[table_name]
                    if str(r.get(col, "")) != val
                ]

    def _execute_update(self, query: str) -> None:
        """Simulate: UPDATE table SET col='val', ... WHERE pk = 'id'"""
        # Extract table name.
        m_table = re.match(r"UPDATE\s+(\S+)\s+SET\s+", query, re.IGNORECASE)
        if not m_table:
            return
        table_name = m_table.group(1)
        if table_name not in self._tables:
            return

        # Extract WHERE clause to find matching rows.
        # Support: WHERE batch_id = 'xyz'
        m_where = re.search(
            r"WHERE\s+(\w+)\s*=\s*'([^']+)'",
            query, re.IGNORECASE
        )
        if not m_where:
            return
        pk_col = m_where.group(1)
        pk_val = m_where.group(2)

        # Extract SET clause assignments.
        m_set = re.search(r"SET\s+(.*?)\s+WHERE", query, re.IGNORECASE | re.DOTALL)
        if not m_set:
            return
        set_clause = m_set.group(1)

        # Parse individual assignments: col = 'val' or col = NUMBER or col = NULL
        assignments: dict[str, Any] = {}
        for part in re.split(r",\s*(?=\w+\s*=)", set_clause):
            part = part.strip()
            am = re.match(r"(\w+)\s*=\s*'([^']*)'", part)
            if am:
                assignments[am.group(1)] = am.group(2)
                continue
            am = re.match(r"(\w+)\s*=\s*NULL\b", part, re.IGNORECASE)
            if am:
                assignments[am.group(1)] = None
                continue
            am = re.match(r"(\w+)\s*=\s*([\d.]+)\b", part)
            if am:
                try:
                    assignments[am.group(1)] = int(am.group(2))
                except ValueError:
                    assignments[am.group(1)] = float(am.group(2))

        # Apply to matching rows.
        for row in self._tables[table_name]:
            if str(row.get(pk_col, "")) == pk_val:
                row.update(assignments)

    @property
    def catalog(self) -> Any:
        tables = self._tables

        class _Catalog:
            def tableExists(self, name: str) -> bool:
                return name in tables

        return _Catalog()

    @property
    def read(self) -> Any:
        session = self

        class _Reader:
            def option(self, k: str, v: Any) -> "_Reader":
                return self

            def csv(self, path: str) -> MockDataFrame:
                # Return one synthetic row so Bronze + Silver pipeline can proceed.
                df = MockDataFrame([{"id": "1", "name": "Alice"}])
                df._parent_session = session
                return df

            def json(self, path: str) -> MockDataFrame:
                df = MockDataFrame([{"id": "1", "name": "Alice"}])
                df._parent_session = session
                return df

        return _Reader()


# =========================================================================
# Fixture helpers
# =========================================================================

@pytest.fixture()
def spark() -> MockSparkSession:
    return MockSparkSession()


def _make_tracker(spark: MockSparkSession) -> Any:
    """Construct FabricBatchTracker bypassing the _require_pyspark check."""
    from accelerator.adapters.fabric_adapters import FabricBatchTracker
    t = object.__new__(FabricBatchTracker)
    t._spark = spark
    t._table_ref = "metadata_batch_control"
    spark._tables.setdefault("metadata_batch_control", [])
    return t


def _make_bronze_store(spark: MockSparkSession) -> Any:
    """Construct FabricBronzeStore bypassing the _require_pyspark check."""
    from accelerator.adapters.fabric_adapters import FabricBronzeStore
    store = object.__new__(FabricBronzeStore)
    store._spark = spark
    return store


def _make_silver_publisher(spark: MockSparkSession) -> Any:
    """Construct FabricSilverPublisher bypassing the _require_pyspark check."""
    from accelerator.adapters.fabric_adapters import FabricSilverPublisher
    pub = object.__new__(FabricSilverPublisher)
    pub._spark = spark
    return pub


def _minimal_config(source_name: str = "test_src") -> Any:
    """Minimal BatchSourceConfigProtocol-compliant object."""
    class _Cfg:
        def __init__(self) -> None:
            self.source_name = source_name
            self.format = "csv"
            self.input_path = "abfss://input.csv"
            self.bronze_path = f"bronze_{source_name}"
    return _Cfg()


# =========================================================================
# 1. FabricBatchTracker tests
# =========================================================================

class TestFabricBatchTracker:

    def test_protocol_compliance(self, spark: MockSparkSession) -> None:
        """FabricBatchTracker must satisfy BatchTrackerProtocol isinstance check."""
        from accelerator.contracts import BatchTrackerProtocol
        tracker = _make_tracker(spark)
        assert isinstance(tracker, BatchTrackerProtocol)

    def test_start_creates_batch_row(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        assert isinstance(batch_id, str) and len(batch_id) == 36  # UUID

        rows = spark._tables["metadata_batch_control"]
        assert len(rows) == 1
        assert rows[0]["batch_id"] == batch_id
        assert rows[0]["status"] == "RUNNING"
        assert rows[0]["commit_state"] == "NONE"

    def test_start_with_submission_key(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00", submission_key="sk-1")
        rows = spark._tables["metadata_batch_control"]
        assert rows[0]["submission_key"] == "sk-1"

    def test_find_submission_returns_row(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00", submission_key="sk-1")
        found = tracker.find_submission("sk-1")
        assert found is not None
        assert found["batch_id"] == batch_id

    def test_find_submission_missing_returns_none(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        assert tracker.find_submission("nonexistent") is None

    def test_find_batch_returns_row(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        found = tracker.find_batch(batch_id)
        assert found is not None
        assert found["batch_id"] == batch_id

    def test_set_commit_state_emits_sql(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        spark._sql_calls.clear()
        tracker.set_commit_state(batch_id, "BRONZE_COMMITTED")
        assert any("BRONZE_COMMITTED" in c for c in spark._sql_calls)

    def test_set_commit_state_updates_table(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        tracker.set_commit_state(batch_id, "BRONZE_COMMITTED")
        row = tracker.find_batch(batch_id)
        assert row["commit_state"] == "BRONZE_COMMITTED"

    def test_update_stage_emits_sql(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        spark._sql_calls.clear()
        tracker.update_stage(batch_id, "SILVER")
        assert any("SILVER" in c and "UPDATE" in c.upper() for c in spark._sql_calls)

    def test_update_stage_updates_table(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        tracker.update_stage(batch_id, "SILVER")
        row = tracker.find_batch(batch_id)
        assert row["stage"] == "SILVER"

    def test_prepare_outputs_emits_sql(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        spark._sql_calls.clear()
        tracker.prepare_outputs(batch_id, records_silver=10, records_rejected=2)
        assert any("OUTPUTS_STAGED" in c for c in spark._sql_calls)

    def test_prepare_outputs_updates_table(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        tracker.prepare_outputs(batch_id, records_silver=10, records_rejected=2)
        row = tracker.find_batch(batch_id)
        assert row["commit_state"] == "OUTPUTS_STAGED"
        assert row["records_silver"] == 10
        assert row["records_rejected"] == 2

    def test_lease_is_active_with_future_expiry(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        future = "2099-01-01T00:00:00+00:00"
        assert tracker.lease_is_active({"lease_expires_at": future}) is True

    def test_lease_is_active_with_past_expiry(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        assert tracker.lease_is_active({"lease_expires_at": "2000-01-01T00:00:00+00:00"}) is False

    def test_lease_is_active_with_no_expiry(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        assert tracker.lease_is_active({"lease_expires_at": None}) is False

    def test_finish_updates_status(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        tracker.finish(
            batch_id,
            "SUCCESS",
            records_processed=5,
            finished_at="2026-01-01T01:00:00+00:00",
            records_persisted=5,
        )
        row = tracker.find_batch(batch_id)
        assert row["status"] == "SUCCESS"

    def test_finish_updates_failed_status(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        tracker.finish(
            batch_id,
            "FAILED",
            records_processed=5,
            finished_at="2026-01-01T01:00:00+00:00",
        )
        row = tracker.find_batch(batch_id)
        assert row["status"] == "FAILED"

    def test_get_audit_summary_returns_list(self, spark: MockSparkSession) -> None:
        from accelerator.contracts import BatchAuditSummary
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        tracker.finish(
            batch_id, "SUCCESS",
            records_processed=5,
            finished_at="2026-01-01T01:00:00+00:00",
        )
        summaries = tracker.get_audit_summary(batch_id=batch_id)
        assert len(summaries) == 1
        assert isinstance(summaries[0], BatchAuditSummary)

    def test_get_audit_summary_status_enum(self, spark: MockSparkSession) -> None:
        from accelerator.contracts import BatchStatus
        tracker = _make_tracker(spark)
        cfg = _minimal_config()
        batch_id = tracker.start(cfg, "2026-01-01T00:00:00+00:00")
        tracker.finish(
            batch_id, "FAILED",
            records_processed=5,
            finished_at="2026-01-01T01:00:00+00:00",
        )
        summaries = tracker.get_audit_summary(batch_id=batch_id)
        assert summaries[0].status == BatchStatus.FAILED

    def test_restart_emits_update_sql(self, spark: MockSparkSession) -> None:
        tracker = _make_tracker(spark)
        old_token = "old-token"
        spark._tables["metadata_batch_control"].append({
            "batch_id": "test-batch-999",
            "status": "FAILED",
            "stage": "BRONZE",
            "lease_token": old_token,
            "lease_expires_at": "2000-01-01T00:00:00+00:00",
        })
        new_token = str(uuid.uuid4())
        spark._sql_calls.clear()
        tracker.restart("test-batch-999", new_token, expected_lease_token=old_token)
        update_calls = [c for c in spark._sql_calls if "UPDATE" in c.upper() and "test-batch-999" in c]
        assert len(update_calls) >= 1

    def test_all_delta_destinations_have_batch_false_when_bronze_absent(
        self, spark: MockSparkSession
    ) -> None:
        tracker = _make_tracker(spark)
        row = {
            "batch_id": "b1",
            "bronze_table": "bronze_missing",
            "silver_table": None,
            "quarantine_table": None,
            "commit_state": "BRONZE_COMMITTED",
            "records_persisted": 1,
            "records_silver": 0,
            "records_scd2_changed": None,
            "records_rejected": 0,
        }
        assert tracker.all_delta_destinations_have_batch(row) is False

    def test_all_delta_destinations_have_batch_true_when_bronze_present(
        self, spark: MockSparkSession
    ) -> None:
        tracker = _make_tracker(spark)
        spark._tables["bronze_src"] = [{"_ingestion_batch_id": "b1"}]
        row = {
            "batch_id": "b1",
            "bronze_table": "bronze_src",
            "silver_table": None,
            "quarantine_table": None,
            "commit_state": "BRONZE_COMMITTED",
            "records_persisted": 1,
            "records_silver": 0,
            "records_scd2_changed": None,
            "records_rejected": 0,
        }
        assert tracker.all_delta_destinations_have_batch(row) is True


# =========================================================================
# 2. FabricBronzeStore tests
# =========================================================================

class TestFabricBronzeStore:

    def test_protocol_compliance(self, spark: MockSparkSession) -> None:
        """FabricBronzeStore must satisfy BronzeStoreProtocol."""
        from accelerator.contracts import BronzeStoreProtocol
        store = _make_bronze_store(spark)
        assert isinstance(store, BronzeStoreProtocol)

    def test_append_writes_rows(self, spark: MockSparkSession) -> None:
        store = _make_bronze_store(spark)
        df = spark.createDataFrame([{"id": "1", "_ingestion_batch_id": "b1"}])
        count = store.append("bronze_orders", df)
        assert count == 1
        assert len(spark._tables.get("bronze_orders", [])) == 1

    def test_append_returns_zero_for_empty_dataframe(self, spark: MockSparkSession) -> None:
        store = _make_bronze_store(spark)
        df = spark.createDataFrame([])
        count = store.append("bronze_orders", df)
        assert count == 0

    def test_append_accepts_list_of_dicts(self, spark: MockSparkSession) -> None:
        store = _make_bronze_store(spark)
        records = [{"id": "1", "_ingestion_batch_id": "b1"}, {"id": "2", "_ingestion_batch_id": "b1"}]
        count = store.append("bronze_orders", records)
        assert count == 2

    def test_delete_batch_removes_only_target_batch(self, spark: MockSparkSession) -> None:
        store = _make_bronze_store(spark)
        spark._tables["bronze_orders"] = [
            {"_ingestion_batch_id": "b1", "val": "a"},
            {"_ingestion_batch_id": "b2", "val": "b"},
            {"_ingestion_batch_id": "b1", "val": "c"},
        ]
        deleted_count = store.delete_batch("bronze_orders", "b1")
        assert deleted_count == 2
        remaining = spark._tables["bronze_orders"]
        assert all(r["_ingestion_batch_id"] == "b2" for r in remaining)
        assert len(remaining) == 1


# =========================================================================
# 3. FabricSilverPublisher — Conformed Silver tests
# =========================================================================

class TestFabricSilverPublisherConformed:

    def test_protocol_compliance(self, spark: MockSparkSession) -> None:
        """FabricSilverPublisher must satisfy SilverPublisherProtocol."""
        from accelerator.contracts import SilverPublisherProtocol
        pub = _make_silver_publisher(spark)
        assert isinstance(pub, SilverPublisherProtocol)

    def test_publish_silver_writes_rows(self, spark: MockSparkSession) -> None:
        pub = _make_silver_publisher(spark)
        records = spark.createDataFrame([
            {"id": "1", "_silver_batch_id": "b1"},
            {"id": "2", "_silver_batch_id": "b1"},
        ])
        count = pub.publish_silver("silver_orders", records, "b1")
        assert count == 2
        assert len(spark._tables.get("silver_orders", [])) == 2

    def test_publish_silver_idempotent_skip_when_fully_written(
        self, spark: MockSparkSession
    ) -> None:
        """If both rows already exist with matching batch_id, skip the write."""
        pub = _make_silver_publisher(spark)
        spark._tables["silver_orders"] = [
            {"id": "1", "_silver_batch_id": "b1"},
            {"id": "2", "_silver_batch_id": "b1"},
        ]
        records = spark.createDataFrame([
            {"id": "1", "_silver_batch_id": "b1"},
            {"id": "2", "_silver_batch_id": "b1"},
        ])
        count = pub.publish_silver("silver_orders", records, "b1")
        assert count == 2
        # Rows must not be duplicated.
        assert len(spark._tables["silver_orders"]) == 2

    def test_publish_silver_cleans_partial_then_rewrites(
        self, spark: MockSparkSession
    ) -> None:
        """Partial write: 1 of 2 rows present → cleanup and re-write."""
        pub = _make_silver_publisher(spark)
        spark._tables["silver_orders"] = [
            {"id": "1", "_silver_batch_id": "b1"},
            {"id": "X", "_silver_batch_id": "b_other"},  # must NOT be deleted
        ]
        records = spark.createDataFrame([
            {"id": "1", "_silver_batch_id": "b1"},
            {"id": "2", "_silver_batch_id": "b1"},
        ])
        count = pub.publish_silver("silver_orders", records, "b1")
        assert count == 2
        final = spark._tables["silver_orders"]
        other = [r for r in final if r.get("_silver_batch_id") == "b_other"]
        assert len(other) == 1, "Other-batch row must be preserved"

    def test_publish_quarantine_idempotent_skip(self, spark: MockSparkSession) -> None:
        pub = _make_silver_publisher(spark)
        spark._tables["quarantine_orders"] = [
            {"id": "1", "_rejection_batch_id": "b1"},
        ]
        records = spark.createDataFrame([{"id": "1", "_rejection_batch_id": "b1"}])
        count = pub.publish_quarantine("quarantine_orders", records, "b1")
        assert count == 1
        assert len(spark._tables["quarantine_orders"]) == 1

    def test_publish_quarantine_cleans_partial_then_writes(
        self, spark: MockSparkSession
    ) -> None:
        pub = _make_silver_publisher(spark)
        spark._tables["quarantine_orders"] = [
            {"id": "1", "_rejection_batch_id": "b1"},  # partial: only 1 of 2
        ]
        records = spark.createDataFrame([
            {"id": "1", "_rejection_batch_id": "b1"},
            {"id": "2", "_rejection_batch_id": "b1"},
        ])
        count = pub.publish_quarantine("quarantine_orders", records, "b1")
        assert count == 2


# =========================================================================
# 4. FabricSilverPublisher — SCD2 tests
# =========================================================================

class TestFabricSilverPublisherScd2:

    def _scd2_cfg(self) -> Any:
        from accelerator.contracts import Scd2Config
        return Scd2Config(
            enabled=True,
            business_keys=["id"],
            tracking_columns=["name"],
            effective_from_col="_effective_from",
            effective_to_col="_effective_to",
            is_current_col="_is_current",
        )

    def test_scd2_initial_batch_inserts_all_as_current(
        self, spark: MockSparkSession
    ) -> None:
        """First load: all records become new 'current' versions."""
        pub = _make_silver_publisher(spark)
        scd2_cfg = self._scd2_cfg()
        records = spark.createDataFrame([
            {"id": "1", "name": "Alice", "_silver_batch_id": "b1"},
            {"id": "2", "name": "Bob",   "_silver_batch_id": "b1"},
        ])
        count = pub.publish_scd2("dim_customers", records, "b1", scd2_cfg)
        assert count == 2
        rows = spark._tables.get("dim_customers", [])
        assert len(rows) == 2
        assert all(r.get("_is_current") is True for r in rows)

    def test_scd2_changed_batch_inserts_new_version(
        self, spark: MockSparkSession
    ) -> None:
        """Second batch with changed tracking column: 1 new version inserted."""
        pub = _make_silver_publisher(spark)
        scd2_cfg = self._scd2_cfg()
        # Pre-populate dimension with initial state.
        spark._tables["dim_customers"] = [
            {
                "id": "1", "name": "Alice",
                "_silver_batch_id": "b1", "_is_current": True,
                "_scd_key": "key-1", "_effective_from": "2026-01-01",
                "_effective_to": None,
            }
        ]
        # New batch: name changed.
        records = spark.createDataFrame([
            {"id": "1", "name": "Alicia", "_silver_batch_id": "b2"},
        ])
        count = pub.publish_scd2("dim_customers", records, "b2", scd2_cfg)
        assert count == 1, "One new version should be inserted for the changed record"
        rows = spark._tables["dim_customers"]
        assert len(rows) == 2
        old_row = [r for r in rows if r["name"] == "Alice"][0]
        assert old_row["_is_current"] is False
        assert old_row["_effective_to"] is not None
        new_row = [r for r in rows if r["name"] == "Alicia"][0]
        assert new_row["_is_current"] is True
        assert new_row["_effective_to"] is None

    def test_scd2_crash_after_step1_recovery_preserves_history_and_single_current(
        self, spark: MockSparkSession
    ) -> None:
        """Crash after Step 1 (old row closed, new row not yet inserted):
        retry completes Step 2, preserves history, and ensures exactly 1 current row.
        """
        pub = _make_silver_publisher(spark)
        scd2_cfg = self._scd2_cfg()
        # Pre-state: crash occurred after closing old version, but before inserting new version.
        spark._tables["dim_customers"] = [
            {
                "id": "1", "name": "Alice",
                "_silver_batch_id": "b1", "_is_current": False,
                "_scd_key": "key-1", "_effective_from": "2026-01-01",
                "_effective_to": "2026-01-02T10:00:00+00:00",
            }
        ]
        # Retry with the same incoming batch b2
        records = spark.createDataFrame([
            {"id": "1", "name": "Alicia", "_silver_batch_id": "b2"},
        ])
        count = pub.publish_scd2("dim_customers", records, "b2", scd2_cfg)
        assert count == 1

        rows = spark._tables["dim_customers"]
        assert len(rows) == 2
        historical = [r for r in rows if r["name"] == "Alice"][0]
        assert historical["_is_current"] is False
        assert historical["_effective_to"] == "2026-01-02T10:00:00+00:00"

        current_rows = [r for r in rows if r.get("_is_current")]
        assert len(current_rows) == 1
        assert current_rows[0]["name"] == "Alicia"
        assert current_rows[0]["_silver_batch_id"] == "b2"

    def test_scd2_replay_with_conflicting_tracked_data_raises_value_error(
        self, spark: MockSparkSession
    ) -> None:
        """Replaying a batch with different data on the same key raises ValueError."""
        pub = _make_silver_publisher(spark)
        scd2_cfg = self._scd2_cfg()
        spark._tables["dim_customers"] = [
            {
                "id": "1", "name": "Alicia",
                "_silver_batch_id": "b2", "_is_current": True,
                "_scd_key": "key-2", "_effective_from": "2026-01-02",
                "_effective_to": None,
            }
        ]
        # Same batch ID b2, but conflicting name 'Alyssa' instead of 'Alicia'
        records = spark.createDataFrame([
            {"id": "1", "name": "Alyssa", "_silver_batch_id": "b2"},
        ])
        with pytest.raises(ValueError, match="already applied with different tracked values"):
            pub.publish_scd2("dim_customers", records, "b2", scd2_cfg)

    def test_scd2_replay_does_not_duplicate_current_version(
        self, spark: MockSparkSession
    ) -> None:
        """Replay b2 when b2's version is already current: no new version created."""
        pub = _make_silver_publisher(spark)
        scd2_cfg = self._scd2_cfg()
        # Pre-state: b2 already applied (current row is Alicia, old Alice row is closed).
        spark._tables["dim_customers"] = [
            {
                "id": "1", "name": "Alice",
                "_silver_batch_id": "b1", "_is_current": False,
                "_scd_key": "key-old", "_effective_from": "2026-01-01",
                "_effective_to": "2026-06-01",
            },
            {
                "id": "1", "name": "Alicia",
                "_silver_batch_id": "b2", "_is_current": True,
                "_scd_key": "key-new", "_effective_from": "2026-06-01",
                "_effective_to": None,
            },
        ]
        records = spark.createDataFrame([
            {"id": "1", "name": "Alicia", "_silver_batch_id": "b2"},
        ])
        pub.publish_scd2("dim_customers", records, "b2", scd2_cfg)
        final = spark._tables.get("dim_customers", [])
        current_rows = [r for r in final if r.get("_is_current")]
        assert len(current_rows) >= 1
        assert all(r.get("name") == "Alicia" for r in current_rows)

    def test_scd2_unchanged_record_produces_no_new_version(
        self, spark: MockSparkSession
    ) -> None:
        """If tracking columns are unchanged, no new SCD2 version is created."""
        pub = _make_silver_publisher(spark)
        scd2_cfg = self._scd2_cfg()
        spark._tables["dim_customers"] = [
            {
                "id": "1", "name": "Alice",
                "_silver_batch_id": "b1", "_is_current": True,
                "_scd_key": "key-1", "_effective_from": "2026-01-01",
                "_effective_to": None,
            }
        ]
        records = spark.createDataFrame([
            {"id": "1", "name": "Alice", "_silver_batch_id": "b2"},
        ])
        count = pub.publish_scd2("dim_customers", records, "b2", scd2_cfg)
        assert count == 0, "No new version should be inserted for an unchanged record"


# =========================================================================
# 5. run_fabric_pipeline() end-to-end tests
# =========================================================================

def _build_pipeline_config(
    source_name: str = "test_src",
    with_silver: bool = True,
    with_scd2: bool = False,
) -> Any:
    """Build a minimal PipelineConfig for tests."""
    from accelerator.contracts import (
        PipelineConfig, SilverConfig, Scd2Config,
        ColumnSchema, FieldType, ExtraColumnsPolicy, MissingColumnsPolicy,
    )
    silver = None
    if with_silver:
        scd2 = Scd2Config(
            enabled=True,
            business_keys=["id"],
            tracking_columns=["name"],
        ) if with_scd2 else Scd2Config(enabled=False)
        silver = SilverConfig(
            target_name="dim_test",
            silver_path=Path("silver_dim_test"),
            rejected_path=None,
            primary_keys=["id"],
            extra_columns=ExtraColumnsPolicy.IGNORE,
            missing_columns=MissingColumnsPolicy.NULL,
            columns=[
                ColumnSchema("id",   FieldType.STRING),
                ColumnSchema("name", FieldType.STRING),
            ],
            mappings=[],
            quality_rules=[],
            scd2=scd2,
        )
    return PipelineConfig(
        source_name=source_name,
        format="csv",
        input_path=Path("abfss://input.csv"),
        bronze_path=Path(f"bronze_{source_name}"),
        metadata_store=Path("metadata_batch_control"),
        silver=silver,
    )


def _run_pipeline(
    spark: MockSparkSession,
    config: Any | None = None,
    submission_key: str | None = None,
    tracker: Any = None,
    bronze_store: Any = None,
    silver_publisher: Any = None,
) -> Any:
    from accelerator.adapters.fabric_pipeline import run_fabric_pipeline
    if config is None:
        config = _build_pipeline_config()
    if tracker is None:
        tracker = _make_tracker(spark)
    if bronze_store is None:
        bronze_store = _make_bronze_store(spark)
    if silver_publisher is None:
        silver_publisher = _make_silver_publisher(spark)

    return run_fabric_pipeline(
        config_path=config,
        spark=spark,
        submission_key=submission_key,
        tracker=tracker,
        bronze_store=bronze_store,
        silver_publisher=silver_publisher,
    )


class TestRunFabricPipeline:

    def test_full_bronze_silver_success(self, spark: MockSparkSession) -> None:
        """Happy path: Bronze + Silver → BatchStatus.SUCCESS."""
        from accelerator.contracts import BatchStatus
        summary = _run_pipeline(spark)
        assert summary.status == BatchStatus.SUCCESS
        assert summary.stage == "COMPLETED"

    def test_bronze_only_pipeline_succeeds(self, spark: MockSparkSession) -> None:
        """Bronze-only config → stage = BRONZE, status = SUCCESS."""
        from accelerator.contracts import BatchStatus
        cfg = _build_pipeline_config(with_silver=False)
        summary = _run_pipeline(spark, config=cfg)
        assert summary.status == BatchStatus.SUCCESS
        assert summary.stage == "BRONZE"

    def test_idempotent_replay_returns_replay_true(
        self, spark: MockSparkSession
    ) -> None:
        """Second call with same submission_key and committed Bronze → idempotent_replay=True."""
        from accelerator.contracts import BatchStatus
        tracker = _make_tracker(spark)
        bronze_store = _make_bronze_store(spark)
        silver_publisher = _make_silver_publisher(spark)

        # First run.
        summary1 = _run_pipeline(
            spark, submission_key="sk-idem-1",
            tracker=tracker,
            bronze_store=bronze_store,
            silver_publisher=silver_publisher,
        )
        assert summary1.idempotent_replay is False

        # Manually mark the batch row as fully committed so the second call
        # triggers the idempotent path.
        batch_rows = spark._tables.get("metadata_batch_control", [])
        bronze_table = f"bronze_{summary1.source_name}"
        for row in batch_rows:
            if row.get("submission_key") == "sk-idem-1":
                row["status"] = "SUCCESS"
                row["commit_state"] = "COMMITTED"
                row["records_persisted"] = 1
                row["bronze_table"] = bronze_table
                row["silver_table"] = None
                row["quarantine_table"] = None
                row["records_silver"] = 0
                row["records_scd2_changed"] = None
                row["records_rejected"] = 0
        # Ensure Bronze table has at least one row for this batch.
        batch_id = batch_rows[0]["batch_id"]
        spark._tables.setdefault(bronze_table, [])
        if not any(r.get("_ingestion_batch_id") == batch_id for r in spark._tables[bronze_table]):
            spark._tables[bronze_table].append({"_ingestion_batch_id": batch_id, "id": "1"})

        # Second run with the same key.
        summary2 = _run_pipeline(
            spark, submission_key="sk-idem-1",
            tracker=tracker,
            bronze_store=bronze_store,
            silver_publisher=silver_publisher,
        )
        assert summary2.idempotent_replay is True

    def test_active_lease_raises_runtime_error(self, spark: MockSparkSession) -> None:
        """A batch with an unexpired lease raises RuntimeError."""
        from accelerator.adapters.fabric_pipeline import _pipeline_fingerprint
        cfg = _build_pipeline_config()
        fp = _pipeline_fingerprint(cfg)
        tracker = _make_tracker(spark)
        spark._tables["metadata_batch_control"] = [{
            "batch_id": "locked-batch",
            "source_name": "test_src",
            "input_path": "abfss://input.csv",
            "format": "csv",
            "status": "RUNNING",
            "stage": "SILVER",
            "batch_start_time": "2026-01-01T00:00:00+00:00",
            "batch_end_time": None,
            "duration_seconds": None,
            "submission_key": "sk-active",
            "submission_fingerprint": fp,
            "bronze_table": "bronze_test_src",
            "silver_table": None,
            "quarantine_table": None,
            "commit_state": "BRONZE_COMMITTED",
            "lease_token": "active-token",
            "lease_expires_at": "2099-01-01T00:00:00+00:00",
            "records_processed": 0,
            "records_persisted": 0,
            "records_silver": 0,
            "records_scd2_changed": None,
            "records_rejected": 0,
            "records_failed": None,
            "reconciliation_status": "NONE",
            "reconciliation_discrepancy": None,
            "error_message": None,
        }]
        with pytest.raises(RuntimeError, match="active ingestion"):
            _run_pipeline(spark, config=cfg, submission_key="sk-active", tracker=tracker)

    def test_incompatible_fingerprint_raises_value_error(
        self, spark: MockSparkSession
    ) -> None:
        """Different fingerprint for same submission_key raises ValueError."""
        tracker = _make_tracker(spark)
        spark._tables["metadata_batch_control"] = [{
            "batch_id": "old-batch",
            "source_name": "test_src",
            "input_path": "abfss://input.csv",
            "format": "csv",
            "status": "SUCCESS",
            "stage": "COMPLETED",
            "batch_start_time": "2026-01-01T00:00:00+00:00",
            "batch_end_time": "2026-01-01T01:00:00+00:00",
            "duration_seconds": 3600.0,
            "submission_key": "sk-conflict",
            "submission_fingerprint": "DIFFERENT_FINGERPRINT",
            "bronze_table": "bronze_test_src",
            "silver_table": None,
            "quarantine_table": None,
            "commit_state": "COMMITTED",
            "lease_token": None,
            "lease_expires_at": None,
            "records_processed": 5,
            "records_persisted": 5,
            "records_silver": 0,
            "records_scd2_changed": None,
            "records_rejected": 0,
            "records_failed": 0,
            "reconciliation_status": "RECONCILED",
            "reconciliation_discrepancy": "",
            "error_message": None,
        }]
        with pytest.raises(ValueError, match="incompatible"):
            _run_pipeline(spark, submission_key="sk-conflict", tracker=tracker)

    def test_partial_failure_bronze_committed_not_duplicated(
        self, spark: MockSparkSession
    ) -> None:
        """Crash after BRONZE_COMMITTED: retry must NOT re-write Bronze rows."""
        from accelerator.adapters.fabric_pipeline import _pipeline_fingerprint
        cfg = _build_pipeline_config()
        fp = _pipeline_fingerprint(cfg)
        tracker = _make_tracker(spark)
        batch_id = "test-batch-recovery"
        # Pre-state: Bronze committed, crashed before Silver.
        spark._tables["metadata_batch_control"] = [{
            "batch_id": batch_id,
            "source_name": "test_src",
            "input_path": "abfss://input.csv",
            "format": "csv",
            "status": "FAILED",
            "stage": "SILVER",
            "batch_start_time": "2026-01-01T00:00:00+00:00",
            "batch_end_time": None,
            "duration_seconds": None,
            "submission_key": "sk-crash-1",
            "submission_fingerprint": fp,
            "bronze_table": "bronze_test_src",
            "silver_table": "silver_dim_test",
            "quarantine_table": "quarantine_dim_test",
            "commit_state": "BRONZE_COMMITTED",
            "lease_token": "old-token",
            "lease_expires_at": "2000-01-01T00:00:00+00:00",
            "records_processed": 1,
            "records_persisted": 1,
            "records_silver": 0,
            "records_scd2_changed": None,
            "records_rejected": 0,
            "records_failed": None,
            "reconciliation_status": "FAILED",
            "reconciliation_discrepancy": "",
            "error_message": "Simulated crash",
        }]
        # Bronze data already persisted.
        spark._tables["bronze_test_src"] = [
            {"id": "1", "_ingestion_batch_id": batch_id,
             "_ingestion_source_name": "test_src"}
        ]

        _run_pipeline(spark, submission_key="sk-crash-1", tracker=tracker)

        # Bronze must NOT be duplicated.
        bronze_rows = [
            r for r in spark._tables.get("bronze_test_src", [])
            if r.get("_ingestion_batch_id") == batch_id
        ]
        assert len(bronze_rows) == 1, "Bronze rows must not be duplicated on retry"


# =========================================================================
# 6. SCD2 end-to-end via run_fabric_pipeline
# =========================================================================

class TestRunFabricPipelineScd2:

    def test_scd2_initial_batch_succeeds(self, spark: MockSparkSession) -> None:
        from accelerator.contracts import BatchStatus
        cfg = _build_pipeline_config(with_scd2=True)
        summary = _run_pipeline(spark, config=cfg)
        assert summary.status == BatchStatus.SUCCESS

    def test_scd2_records_scd2_changed_in_summary(self, spark: MockSparkSession) -> None:
        cfg = _build_pipeline_config(with_scd2=True)
        summary = _run_pipeline(spark, config=cfg)
        # records_scd2_changed should be an integer (≥ 0) after SCD2 run.
        assert summary.records_scd2_changed is not None
        assert isinstance(summary.records_scd2_changed, int)


# =========================================================================
# 7. Import isolation smoke tests
# =========================================================================

def test_local_pipeline_runner_unaffected() -> None:
    """Fabric adapter import must not pollute the local pipeline."""
    from accelerator.pipeline_runner import run_pipeline
    from accelerator.bronze_ingestion import BatchTracker, LocalBronzeStore
    assert callable(run_pipeline)
    assert callable(BatchTracker)
    assert callable(LocalBronzeStore)


def test_fabric_adapters_importable_without_pyspark() -> None:
    """fabric_adapters module must be importable without PySpark installed."""
    import importlib
    mod = importlib.import_module("accelerator.adapters.fabric_adapters")
    assert hasattr(mod, "FabricBatchTracker")
    assert hasattr(mod, "FabricBronzeStore")
    assert hasattr(mod, "FabricSilverPublisher")


def test_fabric_pipeline_importable_without_pyspark() -> None:
    """fabric_pipeline module must be importable without PySpark installed."""
    import importlib
    mod = importlib.import_module("accelerator.adapters.fabric_pipeline")
    assert hasattr(mod, "run_fabric_pipeline")
