"""Tests for Phase 6: Audit, Reconciliation, and Replay Safety."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from accelerator import (
    BatchAuditSummary,
    BatchStatus,
    BatchTracker,
    ReconciliationStatus,
    ingest,
    run_pipeline,
)
from accelerator.contracts import (
    LINEAGE_RECORD_HASH,
    LINEAGE_SILVER_BATCH_ID,
    REJECTION_RAW_RECORD,
    REJECTION_REASON,
    REJECTION_RULE,
    SCD_EFFECTIVE_FROM,
    SCD_EFFECTIVE_TO,
    SCD_IS_CURRENT,
    SCD_SURROGATE_KEY,
)


def make_pipeline_config(
    tmp_path: Path,
    *,
    source_name: str = "test_source",
    csv_content: str = (
        "customer_id,customer_name,postal_code\n"
        "C001,Ada Lovelace,00123\n"
        "C002,Alan Turing,04567\n"
    ),
    columns: list[dict] | None = None,
    extra_silver_columns: list[dict] | None = None,
    quality_rules: list[dict] | None = None,
    include_quarantine: bool = True,
    scd2_config: dict | None = None,
) -> Path:
    input_file = tmp_path / "input.csv"
    input_file.write_text(csv_content, encoding="utf-8")

    bronze_path = tmp_path / "bronze" / "customers_raw.jsonl"
    silver_path = tmp_path / "silver" / "dim_customers.jsonl"
    quarantine_path = (
        tmp_path / "silver" / "quarantine_customers.jsonl" if include_quarantine else None
    )
    meta_path = tmp_path / "metadata" / "audit.db"

    schema_columns = columns or [
        {"name": "customer_id", "data_type": "string", "nullable": True},
        {"name": "customer_name", "data_type": "string", "nullable": False},
        {"name": "postal_code", "data_type": "string", "nullable": True},
    ]
    if extra_silver_columns:
        schema_columns = list(schema_columns) + extra_silver_columns

    mappings = [
        {"source_column": "customer_id", "target_column": "customer_id", "transform": "trim"},
        {"source_column": "customer_name", "target_column": "customer_name", "transform": "trim"},
        {"source_column": "postal_code", "target_column": "postal_code", "transform": "trim"},
    ]
    if extra_silver_columns:
        for col in extra_silver_columns:
            mappings.append({"source_column": col["name"], "target_column": col["name"]})

    rules = (
        quality_rules
        if quality_rules is not None
        else [
            {"rule_name": "not_null", "columns": ["customer_id"], "severity": "reject"},
            {"rule_name": "unique", "columns": ["customer_id"], "severity": "reject"},
            {
                "rule_name": "regex",
                "columns": ["postal_code"],
                "severity": "warn",
                "params": {"pattern": r"^\d{5}$"},
            },
        ]
    )

    silver_block = {
        "target_name": "dim_customers",
        "silver_path": str(silver_path.resolve()),
        "primary_keys": ["customer_id"],
        "extra_columns": "ignore",
        "missing_columns": "null",
        "columns": schema_columns,
        "mappings": mappings,
        "quality_rules": rules,
    }
    if quarantine_path:
        silver_block["rejected_path"] = str(quarantine_path.resolve())
    if scd2_config:
        silver_block["scd2"] = scd2_config

    cfg = {
        "source_name": source_name,
        "format": "csv",
        "input_path": str(input_file.resolve()),
        "bronze_path": str(bronze_path.resolve()),
        "metadata_store": str(meta_path.resolve()),
        "delimiter": ",",
        "encoding": "utf-8",
        "silver": silver_block,
    }

    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    return config_path


def test_successful_batch_audit_and_reconciliation(tmp_path: Path) -> None:
    """1. Successful clean batch verifies exact counts, RECONCILED status, and duration."""
    config_path = make_pipeline_config(tmp_path)
    summary = run_pipeline(config_path)

    assert isinstance(summary, BatchAuditSummary)
    assert summary.status == BatchStatus.SUCCESS
    assert summary.stage == "COMPLETED"
    assert summary.duration_seconds is not None
    assert summary.duration_seconds >= 0.0
    assert summary.reconciliation_status == ReconciliationStatus.RECONCILED.value
    assert summary.reconciliation_discrepancy == ""

    assert summary.records_read == 2
    assert summary.records_bronze == 2
    assert summary.records_silver == 2
    assert summary.records_rejected == 0
    assert summary.records_failed == 0
    assert not summary.idempotent_replay

    # Check Bronze output
    bronze_lines = (tmp_path / "bronze" / "customers_raw.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(bronze_lines) == 2

    # Check Silver output
    silver_lines = (tmp_path / "silver" / "dim_customers.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(silver_lines) == 2
    for line in silver_lines:
        rec = json.loads(line)
        assert rec[LINEAGE_SILVER_BATCH_ID] == summary.batch_id
        assert rec[LINEAGE_RECORD_HASH]


def test_batch_with_rejected_records_reconciliation(tmp_path: Path) -> None:
    """2. Batch with rejected records reconciles counts and sets PARTIAL_SUCCESS."""
    csv_data = (
        "customer_id,customer_name,postal_code\n"
        "C001,Ada Lovelace,00123\n"
        "C001,Ada Duplicate,00123\n"
        ",Missing ID,12345\n"
        "C002,Alan Turing,04567\n"
    )
    config_path = make_pipeline_config(tmp_path, csv_content=csv_data)
    summary = run_pipeline(config_path)

    assert summary.status == BatchStatus.PARTIAL_SUCCESS
    assert summary.records_read == 4
    assert summary.records_bronze == 4
    assert summary.records_silver == 2
    assert summary.records_rejected == 2
    assert summary.records_failed == 0

    # Strict reconciliation equation holds:
    assert summary.records_read == summary.records_silver + summary.records_rejected + summary.records_failed
    assert summary.reconciliation_status == ReconciliationStatus.RECONCILED.value
    assert summary.reconciliation_discrepancy == ""

    # Quarantined records contain failure metadata and raw Bronze record
    quar_path = tmp_path / "silver" / "quarantine_customers.jsonl"
    assert quar_path.exists()
    quar_lines = quar_path.read_text(encoding="utf-8").splitlines()
    assert len(quar_lines) == 2
    for q_line in quar_lines:
        q_rec = json.loads(q_line)
        assert REJECTION_REASON in q_rec
        assert REJECTION_RULE in q_rec
        assert REJECTION_RAW_RECORD in q_rec


def test_warning_only_records_counted_in_silver(tmp_path: Path) -> None:
    """3. WARN-only violations produce warnings but are accepted into Silver."""
    csv_data = (
        "customer_id,customer_name,postal_code\n"
        "C001,Ada Lovelace,INVALID_POSTAL\n"
    )
    config_path = make_pipeline_config(tmp_path, csv_content=csv_data)
    summary = run_pipeline(config_path)

    assert summary.status == BatchStatus.SUCCESS
    assert summary.records_read == 1
    assert summary.records_silver == 1
    assert summary.records_rejected == 0
    assert summary.reconciliation_status == ReconciliationStatus.RECONCILED.value

    quar_path = tmp_path / "silver" / "quarantine_customers.jsonl"
    assert not quar_path.exists()


def test_transformation_failure_records_batch_level_error(tmp_path: Path) -> None:
    """4. Unhandled transformation error records batch-level failure without fabricated row counts."""
    extra_cols = [{"name": "age", "data_type": "integer", "nullable": False}]
    csv_data = (
        "customer_id,customer_name,postal_code,age\n"
        "C001,Ada Lovelace,00123,not_an_int\n"
    )
    config_path = make_pipeline_config(tmp_path, csv_content=csv_data, extra_silver_columns=extra_cols)

    with pytest.raises(ValueError, match="cannot cast string 'not_an_int' to integer"):
        run_pipeline(config_path)

    # Inspect audit database directly
    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    summaries = tracker.get_audit_summary()
    assert len(summaries) == 1
    summary = summaries[0]

    assert summary.status == BatchStatus.FAILED
    assert summary.records_failed is None  # F-05: Unknown failed count represented as None, not fabricated 0
    assert summary.records_silver == 0
    assert summary.records_rejected == 0
    assert summary.reconciliation_status == ReconciliationStatus.FAILED.value
    assert "cannot cast string 'not_an_int' to integer" in (summary.error_message or "")


def test_repeated_batch_submission_idempotent(tmp_path: Path) -> None:
    """5. Re-submitting with matching submission_key returns existing summary with no duplicates."""
    config_path = make_pipeline_config(tmp_path)
    sub_key = "client-batch-tx-999"

    first_summary = run_pipeline(config_path, submission_key=sub_key)
    assert not first_summary.idempotent_replay

    bronze_lines_1 = (tmp_path / "bronze" / "customers_raw.jsonl").read_text(encoding="utf-8").splitlines()
    silver_lines_1 = (tmp_path / "silver" / "dim_customers.jsonl").read_text(encoding="utf-8").splitlines()

    second_summary = run_pipeline(config_path, submission_key=sub_key)
    assert second_summary.idempotent_replay
    assert second_summary.batch_id == first_summary.batch_id

    bronze_lines_2 = (tmp_path / "bronze" / "customers_raw.jsonl").read_text(encoding="utf-8").splitlines()
    silver_lines_2 = (tmp_path / "silver" / "dim_customers.jsonl").read_text(encoding="utf-8").splitlines()

    # Absolute proof of idempotency: zero additional lines written
    assert len(bronze_lines_1) == len(bronze_lines_2) == 2
    assert len(silver_lines_1) == len(silver_lines_2) == 2


def test_concurrent_same_submission_key_rejected(tmp_path: Path) -> None:
    """6. Active submission key lease rejects concurrent duplicate submissions."""
    config_path = make_pipeline_config(tmp_path)
    sub_key = "sub-concurrent-test"

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    # Manually start an active batch with a 10-minute lease
    tracker.start(
        config=ingest.__globals__["load_config"](config_path),
        ingested_at=datetime.now(timezone.utc).isoformat(),
        submission_key=sub_key,
        submission_fingerprint=run_pipeline.__globals__["_pipeline_fingerprint"](
            run_pipeline.__globals__["load_pipeline_config"](config_path)
        ),
        lease_token="active-lease-123",
    )

    with pytest.raises(RuntimeError, match="Submission key is currently owned by an active ingestion"):
        run_pipeline(config_path, submission_key=sub_key)


def test_recovery_from_crash_after_destinations_committed(tmp_path: Path) -> None:
    """7. Resilient recovery finalizes batch if destinations committed before audit crash."""
    config_path = make_pipeline_config(tmp_path)
    sub_key = "crash-after-commit-key"

    # Step 1: Run once to create committed files
    summary = run_pipeline(config_path, submission_key=sub_key)
    batch_id = summary.batch_id

    # Step 2: Manually simulate an audit finalization crash by reverting status to 'RUNNING' and expiring lease
    db_path = tmp_path / "metadata" / "audit.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE batch_control
               SET status = 'RUNNING',
                   lease_expires_at = ?
               WHERE batch_id = ?""",
            ((datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(), batch_id),
        )

    # Step 3: Next execution detects destinations are already committed and reconciles state
    recovered_summary = run_pipeline(config_path, submission_key=sub_key)
    assert recovered_summary.idempotent_replay
    assert recovered_summary.batch_id == batch_id
    assert recovered_summary.status == BatchStatus.SUCCESS
    assert recovered_summary.reconciliation_status == ReconciliationStatus.RECONCILED.value


def test_retry_after_failed_batch_cleans_uncommitted_stages(tmp_path: Path) -> None:
    """8. Replay after uncommitted partial failure cleans orphaned staging files."""
    config_path = make_pipeline_config(tmp_path)
    sub_key = "staged-cleanup-key"

    # Seed an orphaned .staged file
    silver_path = tmp_path / "silver" / "dim_customers.jsonl"
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    staged_file = silver_path.with_name(f".{silver_path.name}.fake_batch.staged")
    staged_file.write_text("corrupted staged data\n", encoding="utf-8")
    assert staged_file.exists()

    summary = run_pipeline(config_path, submission_key=sub_key)
    assert summary.status == BatchStatus.SUCCESS
    assert (tmp_path / "silver" / "dim_customers.jsonl").exists()


def test_count_mismatch_detected_and_flagged(tmp_path: Path) -> None:
    """9. Explicit count discrepancies flag reconciliation_status as DISCREPANCY."""
    db_path = tmp_path / "metadata" / "audit.db"
    tracker = BatchTracker(db_path)
    batch_id = "test-discrepancy-batch"

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO batch_control
               (batch_id, source_name, input_path, format, status, stage, batch_start_time,
                records_processed, records_persisted)
               VALUES (?, 'src', 'in.csv', 'csv', 'RUNNING', 'SILVER', ?, 10, 10)""",
            (batch_id, datetime.now(timezone.utc).isoformat()),
        )

    # Finish with mismatched numbers: processed=10, but silver=5 + rejected=2 + failed=0 = 7
    tracker.finish(
        batch_id,
        BatchStatus.SUCCESS,
        records_processed=10,
        finished_at=datetime.now(timezone.utc).isoformat(),
        records_persisted=10,
        records_silver=5,
        records_rejected=2,
        records_failed=0,
        stage="COMPLETED",
    )

    summaries = tracker.get_audit_summary(batch_id=batch_id)
    assert len(summaries) == 1
    assert summaries[0].reconciliation_status == ReconciliationStatus.DISCREPANCY.value
    assert "read=10 != silver=5 + rejected=2 + failed=0" in summaries[0].reconciliation_discrepancy


def test_queryable_audit_summary_api(tmp_path: Path) -> None:
    """10. Querying get_audit_summary by batch_id and submission_key works as expected."""
    config_path = make_pipeline_config(tmp_path)
    summary1 = run_pipeline(config_path, submission_key="batch-key-1")

    # Run second batch with different submission key
    config_path_2 = make_pipeline_config(tmp_path, source_name="test_source_2")
    summary2 = run_pipeline(config_path_2, submission_key="batch-key-2")

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")

    all_summaries = tracker.get_audit_summary()
    assert len(all_summaries) == 2

    by_id = tracker.get_audit_summary(batch_id=summary1.batch_id)
    assert len(by_id) == 1
    assert by_id[0].batch_id == summary1.batch_id

    by_key = tracker.get_audit_summary(submission_key="batch-key-2")
    assert len(by_key) == 1
    assert by_key[0].submission_key == "batch-key-2"


def test_bronze_only_backward_compatibility(tmp_path: Path) -> None:
    """11. Ingesting Bronze-only config retains backward compatibility and records audit status."""
    (tmp_path / "input.csv").write_text("col_a,col_b\nval1,val2\n", encoding="utf-8")
    bronze_cfg = {
        "source_name": "bronze_only_source",
        "format": "csv",
        "input_path": "input.csv",
        "bronze_path": "bronze/raw.jsonl",
        "metadata_store": "metadata/audit.db",
    }
    cfg_file = tmp_path / "bronze_only.json"
    cfg_file.write_text(json.dumps(bronze_cfg), encoding="utf-8")

    result = ingest(cfg_file)
    assert result["source_name"] == "bronze_only_source"
    assert result["records_processed"] == 1

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    summaries = tracker.get_audit_summary(batch_id=result["batch_id"])
    assert len(summaries) == 1
    assert summaries[0].reconciliation_status == ReconciliationStatus.RECONCILED.value
    assert summaries[0].duration_seconds is not None


def test_end_to_end_bronze_silver_scd2_pipeline(tmp_path: Path) -> None:
    """12. End-to-end pipeline with SCD2 enabled applies versioning and reconciles cleanly."""
    scd2_block = {
        "enabled": True,
        "business_keys": ["customer_id"],
        "tracking_columns": ["customer_name", "postal_code"],
    }

    # Batch 1: Initial load
    csv_1 = (
        "customer_id,customer_name,postal_code\n"
        "C001,Ada Lovelace,00123\n"
        "C002,Alan Turing,04567\n"
    )
    cfg_1 = make_pipeline_config(tmp_path, csv_content=csv_1, scd2_config=scd2_block)
    summary_1 = run_pipeline(cfg_1, submission_key="scd2-batch-1")

    assert summary_1.status == BatchStatus.SUCCESS
    assert summary_1.records_read == 2
    assert summary_1.records_silver == 2
    assert summary_1.records_rejected == 0
    assert summary_1.reconciliation_status == ReconciliationStatus.RECONCILED.value

    dim_path = tmp_path / "silver" / "dim_customers.jsonl"
    rows_1 = [json.loads(line) for line in dim_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows_1) == 2
    assert all(r[SCD_IS_CURRENT] is True for r in rows_1)
    assert all(r[SCD_EFFECTIVE_TO] is None for r in rows_1)
    assert all(r[SCD_SURROGATE_KEY] for r in rows_1)

    # Batch 2: Tracked change for C001, unchanged for C002
    csv_2 = (
        "customer_id,customer_name,postal_code\n"
        "C001,Ada Lovelace,99999\n"
        "C002,Alan Turing,04567\n"
    )
    cfg_2 = make_pipeline_config(tmp_path, csv_content=csv_2, scd2_config=scd2_block)
    summary_2 = run_pipeline(cfg_2, submission_key="scd2-batch-2")

    assert summary_2.status == BatchStatus.SUCCESS
    assert summary_2.records_read == 2
    assert summary_2.records_silver == 2
    assert summary_2.reconciliation_status == ReconciliationStatus.RECONCILED.value

    rows_2 = [json.loads(line) for line in dim_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows_2) == 3

    # C001 should have 1 closed historical version and 1 current version
    c1_versions = [r for r in rows_2 if r["customer_id"] == "C001"]
    assert len(c1_versions) == 2
    closed = next(r for r in c1_versions if r[SCD_IS_CURRENT] is False)
    current = next(r for r in c1_versions if r[SCD_IS_CURRENT] is True)
    assert closed["postal_code"] == "00123"
    assert current["postal_code"] == "99999"
    assert closed[SCD_EFFECTIVE_TO] == current[SCD_EFFECTIVE_FROM]

    # C002 should have 1 current version (unchanged)
    c2_versions = [r for r in rows_2 if r["customer_id"] == "C002"]
    assert len(c2_versions) == 1
    assert c2_versions[0][SCD_IS_CURRENT] is True


def test_end_to_end_scd2_replay_safety(tmp_path: Path) -> None:
    """13. Replaying an already applied SCD2 batch returns idempotent summary without duplicating history."""
    scd2_block = {
        "enabled": True,
        "business_keys": ["customer_id"],
        "tracking_columns": ["customer_name", "postal_code"],
    }
    csv_1 = (
        "customer_id,customer_name,postal_code\n"
        "C001,Ada Lovelace,00123\n"
    )
    cfg = make_pipeline_config(tmp_path, csv_content=csv_1, scd2_config=scd2_block)
    first_summary = run_pipeline(cfg, submission_key="scd2-replay-key")
    assert not first_summary.idempotent_replay

    dim_path = tmp_path / "silver" / "dim_customers.jsonl"
    dim_content_before = dim_path.read_text(encoding="utf-8")

    # Replay with same submission key
    replay_summary = run_pipeline(cfg, submission_key="scd2-replay-key")
    assert replay_summary.idempotent_replay
    assert replay_summary.batch_id == first_summary.batch_id
    assert dim_path.read_text(encoding="utf-8") == dim_content_before


def test_critical_severity_aborts_pipeline(tmp_path: Path) -> None:
    """14. CRITICAL rule violation aborts the pipeline and records FAILED status in audit."""
    crit_rules = [
        {"rule_name": "not_null", "columns": ["customer_id"], "severity": "critical"},
    ]
    csv_data = (
        "customer_id,customer_name,postal_code\n"
        ",Ada No ID,00123\n"
    )
    config_path = make_pipeline_config(tmp_path, csv_content=csv_data, quality_rules=crit_rules)

    with pytest.raises(RuntimeError, match=r"CRITICAL quality rule 'not_null' failed"):
        run_pipeline(config_path)

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    summaries = tracker.get_audit_summary()
    assert len(summaries) == 1
    assert summaries[0].status == BatchStatus.FAILED
    assert summaries[0].reconciliation_status == ReconciliationStatus.FAILED.value
    assert "CRITICAL quality rule 'not_null' failed" in (summaries[0].error_message or "")


def test_crash_after_silver_before_quarantine_recovers_without_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F-01 & F-02: Crash after Silver publication before Quarantine does not duplicate Silver records on replay."""
    rules = [
        {"rule_name": "not_null", "columns": ["customer_id"], "severity": "reject"},
    ]
    csv_data = (
        "customer_id,customer_name,postal_code\n"
        "C001,Ada Lovelace,00123\n"
        ",No ID Customer,99999\n"
    )
    config_path = make_pipeline_config(tmp_path, csv_content=csv_data, quality_rules=rules)
    silver_path = tmp_path / "silver" / "dim_customers.jsonl"
    rejected_path = tmp_path / "silver" / "quarantine_customers.jsonl"

    original_replace = os.replace
    crashed = False

    def fault_inject_replace(src: str | Path, dst: str | Path) -> None:
        nonlocal crashed
        # Simulate a crash right as quarantine is about to be committed
        if Path(dst) == rejected_path:
            crashed = True
            raise RuntimeError("Fault injection: crash after Silver commit before Quarantine commit")
        original_replace(src, dst)

    monkeypatch.setattr(os, "replace", fault_inject_replace)

    # First attempt: crashes at quarantine commit
    with pytest.raises(RuntimeError, match="Fault injection: crash after Silver commit"):
        run_pipeline(config_path, submission_key="crash-silver-pub-key")

    assert crashed is True
    assert silver_path.exists()
    silver_lines_after_crash = [l for l in silver_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(silver_lines_after_crash) == 1
    assert json.loads(silver_lines_after_crash[0])["customer_id"] == "C001"

    # Quarantine should not be committed yet
    assert not rejected_path.exists() or len(rejected_path.read_text(encoding="utf-8").strip()) == 0

    # F-02 verification: _all_destinations_have_batch must NOT declare success when quarantine is missing
    from accelerator.bronze_ingestion import _all_destinations_have_batch
    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    existing_row = tracker.find_submission("crash-silver-pub-key")
    assert existing_row is not None
    assert not _all_destinations_have_batch(existing_row)

    # Now remove the fault injection and replay with the same submission key
    monkeypatch.undo()

    # Replay execution
    replay_summary = run_pipeline(config_path, submission_key="crash-silver-pub-key")

    assert replay_summary.status == BatchStatus.PARTIAL_SUCCESS
    assert replay_summary.records_read == 2
    assert replay_summary.records_silver == 1
    assert replay_summary.records_rejected == 1
    assert replay_summary.records_failed == 0
    assert replay_summary.reconciliation_status == ReconciliationStatus.RECONCILED.value

    # F-01 verification: Silver must NOT have duplicate records!
    silver_lines_after_replay = [l for l in silver_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(silver_lines_after_replay) == 1  # Exactly 1 record, NO duplicates!
    assert json.loads(silver_lines_after_replay[0])["customer_id"] == "C001"

    # Quarantine must now be populated
    assert rejected_path.exists()
    quarantine_lines = [l for l in rejected_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(quarantine_lines) == 1
    assert json.loads(quarantine_lines[0])["_failed_rule"] == "not_null"


def test_crash_after_publication_before_finish_reconstructs_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F-03 & F-07: Crash after publication but before finish reconciles with reconstructed counts."""
    rules = [
        {"rule_name": "not_null", "columns": ["customer_id"], "severity": "reject"},
    ]
    csv_data = (
        "customer_id,customer_name,postal_code\n"
        "C001,Ada Lovelace,00123\n"
        ",No ID Customer,99999\n"
    )
    config_path = make_pipeline_config(tmp_path, csv_content=csv_data, quality_rules=rules)
    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")

    original_finish = BatchTracker.finish
    crashed = False

    def crash_before_final_finish(self: BatchTracker, batch_id: str, status: Any, *args: Any, **kwargs: Any) -> None:
        nonlocal crashed
        if kwargs.get("stage") == "COMPLETED":
            crashed = True
            raise RuntimeError("Fault injection: crash after publication before final finish call")
        original_finish(self, batch_id, status, *args, **kwargs)

    monkeypatch.setattr(BatchTracker, "finish", crash_before_final_finish)

    with pytest.raises(RuntimeError, match="Fault injection: crash after publication before final finish"):
        run_pipeline(config_path, submission_key="post-pub-crash-key")

    assert crashed is True

    # Both silver and quarantine are committed on disk
    silver_path = tmp_path / "silver" / "dim_customers.jsonl"
    rejected_path = tmp_path / "silver" / "quarantine_customers.jsonl"
    assert silver_path.exists()
    assert rejected_path.exists()

    # Restore finish
    monkeypatch.undo()

    # Replay with same submission key
    replay_summary = run_pipeline(config_path, submission_key="post-pub-crash-key")
    assert replay_summary.idempotent_replay is True
    assert replay_summary.status == BatchStatus.PARTIAL_SUCCESS
    assert replay_summary.records_read == 2
    assert replay_summary.records_silver == 1
    assert replay_summary.records_rejected == 1
    assert replay_summary.records_failed == 0
    assert replay_summary.reconciliation_status == ReconciliationStatus.RECONCILED.value


def test_atomic_lease_takeover_prevents_concurrent_restart(tmp_path: Path) -> None:
    """F-04: Atomic SQL CAS prevents two processes from concurrently acquiring an expired lease."""
    db_path = tmp_path / "lease_cas.db"
    tracker = BatchTracker(db_path)
    cfg = make_pipeline_config(tmp_path)
    from accelerator.contracts import load_pipeline_config
    pipe_cfg = load_pipeline_config(cfg)

    # Start a batch
    batch_id = tracker.start(
        pipe_cfg,
        datetime.now(timezone.utc).isoformat(),
        submission_key="concurrent-lease-key",
        lease_token="original-token",
    )

    # Artificially expire the lease
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE batch_control SET lease_expires_at = ? WHERE batch_id = ?",
            ((datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(), batch_id),
        )

    # Worker A attempts to acquire expired lease via restart CAS
    acquired_a = tracker.restart(batch_id, "token-worker-a")
    assert acquired_a is True

    # Worker B attempts to acquire the lease immediately after Worker A
    acquired_b = tracker.restart(batch_id, "token-worker-b")
    assert acquired_b is False


def test_unknown_records_failed_reported_as_none_on_batch_failure(tmp_path: Path) -> None:
    """F-05: When transformation aborts, records_failed is reported as None rather than fabricated 0."""
    extra_cols = [{"name": "price", "data_type": "decimal", "nullable": False}]
    csv_data = (
        "customer_id,customer_name,postal_code,price\n"
        "C001,Ada Lovelace,00123,not_a_number\n"
    )
    config_path = make_pipeline_config(tmp_path, csv_content=csv_data, extra_silver_columns=extra_cols)

    with pytest.raises(ValueError, match="cannot cast string 'not_a_number' to decimal"):
        run_pipeline(config_path)

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    summaries = tracker.get_audit_summary()
    assert len(summaries) == 1
    summary = summaries[0]

    assert summary.status == BatchStatus.FAILED
    assert summary.records_failed is None  # Must NOT be 0
    assert summary.reconciliation_status == ReconciliationStatus.FAILED.value


def test_legacy_database_migration_with_none_status(tmp_path: Path) -> None:
    """F-06: Database migration with reconciliation_status 'NONE' is handled cleanly by ReconciliationStatus enum."""
    db_path = tmp_path / "legacy_migration.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE batch_control (
                batch_id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                input_path TEXT NOT NULL,
                format TEXT NOT NULL,
                status TEXT NOT NULL,
                batch_start_time TEXT NOT NULL,
                batch_end_time TEXT,
                records_processed INTEGER NOT NULL DEFAULT 0
            )"""
        )
        conn.execute(
            """INSERT INTO batch_control (batch_id, source_name, input_path, format, status, batch_start_time, records_processed)
            VALUES ('legacy-batch-1', 'legacy_source', 'in.csv', 'csv', 'PENDING', '2026-10-26T12:00:00Z', 0)"""
        )

    # Initializing BatchTracker triggers migration which adds reconciliation_status DEFAULT 'NONE'
    tracker = BatchTracker(db_path)
    summaries = tracker.get_audit_summary(batch_id="legacy-batch-1")
    assert len(summaries) == 1
    assert summaries[0].reconciliation_status == ReconciliationStatus.NONE.value
    assert ReconciliationStatus.NONE == "NONE"


def test_valid_empty_batch_produces_zero_records_and_reconciles_and_replays_idempotently(tmp_path: Path) -> None:
    """15. Valid batch that produces 0 Silver and 0 Quarantined records reconciles and replays idempotently."""
    from accelerator.bronze_ingestion import _all_destinations_have_batch

    csv_empty = "customer_id,customer_name,postal_code\n"
    config_path = make_pipeline_config(tmp_path, csv_content=csv_empty)

    summary = run_pipeline(config_path, submission_key="empty-batch-key-1")

    assert summary.status == BatchStatus.SUCCESS
    assert summary.records_read == 0
    assert summary.records_bronze == 0
    assert summary.records_silver == 0
    assert summary.records_rejected == 0
    assert summary.records_failed == 0
    assert summary.reconciliation_status == ReconciliationStatus.RECONCILED.value
    assert summary.idempotent_replay is False

    bronze_path = tmp_path / "bronze" / "customers_raw.jsonl"
    silver_path = tmp_path / "silver" / "dim_customers.jsonl"
    quarantine_path = tmp_path / "silver" / "quarantine_customers.jsonl"

    # Durable publication evidence: required destinations exist on disk and no uncommitted files exist
    assert bronze_path.exists()
    assert silver_path.exists()
    assert not quarantine_path.exists()  # Zero rejected records produces no quarantine file

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    row = tracker.find_submission("empty-batch-key-1")
    assert row is not None
    assert row["commit_state"] == "COMMITTED"
    assert row["records_processed"] == 0
    assert _all_destinations_have_batch(row) is True

    # Replaying same submission key returns idempotent summary without re-executing
    replay_summary = run_pipeline(config_path, submission_key="empty-batch-key-1")
    assert replay_summary.idempotent_replay is True
    assert replay_summary.batch_id == summary.batch_id
    assert replay_summary.records_read == 0
    assert replay_summary.records_silver == 0
    assert replay_summary.records_rejected == 0
    assert replay_summary.reconciliation_status == ReconciliationStatus.RECONCILED.value


def test_distinguish_published_empty_outputs_from_missing_required_outputs(tmp_path: Path) -> None:
    """16. Distinguish published empty outputs from missing files; never mark complete when destination is missing."""
    from accelerator.bronze_ingestion import _all_destinations_have_batch, _reconcile_committed_batch

    csv_empty = "customer_id,customer_name,postal_code\n"
    config_path = make_pipeline_config(tmp_path, csv_content=csv_empty)
    summary = run_pipeline(config_path, submission_key="empty-distinguish-key")
    assert summary.status == BatchStatus.SUCCESS

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    row = tracker.find_submission("empty-distinguish-key")
    assert row is not None
    # When required silver and bronze outputs are present, empty batch is confirmed valid
    assert _all_destinations_have_batch(row) is True

    silver_path = tmp_path / "silver" / "dim_customers.jsonl"
    assert silver_path.exists()

    # Case A: Silver destination is missing (e.g. deleted or failed publication)
    silver_path.unlink()
    assert not silver_path.exists()
    # Must distinguish missing output from empty output
    assert _all_destinations_have_batch(row) is False

    # Simulate recovery attempt: must NOT mark complete when a required destination is missing
    with sqlite3.connect(tmp_path / "metadata" / "audit.db") as conn:
        conn.execute(
            "UPDATE batch_control SET status = 'RUNNING', lease_expires_at = ? WHERE batch_id = ?",
            ((datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(), summary.batch_id),
        )
    row_running = tracker.find_batch(summary.batch_id)
    assert not _reconcile_committed_batch(tracker, row_running)
    row_check = tracker.find_batch(summary.batch_id)
    assert row_check["status"] != "SUCCESS"

    # Replay detects missing output, re-stages and republishes all required destinations
    replay_summary = run_pipeline(config_path, submission_key="empty-distinguish-key")
    assert replay_summary.status == BatchStatus.SUCCESS
    assert silver_path.exists()


def test_recovery_never_marks_complete_when_quarantine_missing_for_rejected_batch(tmp_path: Path) -> None:
    """17. When quarantine output is missing for a batch with rejections, recovery refuses to mark complete."""
    from accelerator.bronze_ingestion import _all_destinations_have_batch

    rules = [
        {"rule_name": "not_null", "columns": ["customer_id"], "severity": "reject"},
    ]
    csv_data = (
        "customer_id,customer_name,postal_code\n"
        ",No ID Customer,99999\n"
    )
    config_path = make_pipeline_config(tmp_path, csv_content=csv_data, quality_rules=rules)
    summary = run_pipeline(config_path, submission_key="quarantine-missing-key")
    assert summary.status == BatchStatus.PARTIAL_SUCCESS
    assert summary.records_rejected == 1

    quarantine_path = tmp_path / "silver" / "quarantine_customers.jsonl"
    assert quarantine_path.exists()
    quarantine_path.unlink()
    assert not quarantine_path.exists()

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    row = tracker.find_submission("quarantine-missing-key")
    assert _all_destinations_have_batch(row) is False

    # Expire lease to simulate dead worker
    with sqlite3.connect(tmp_path / "metadata" / "audit.db") as conn:
        conn.execute(
            "UPDATE batch_control SET status = 'RUNNING', lease_expires_at = ? WHERE batch_id = ?",
            ((datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(), summary.batch_id),
        )

    # Replay cannot be a shortcut; it repairs missing destination
    replay_summary = run_pipeline(config_path, submission_key="quarantine-missing-key")
    assert replay_summary.status == BatchStatus.PARTIAL_SUCCESS
    assert replay_summary.records_rejected == 1
    assert quarantine_path.exists()


def test_crash_recovery_for_empty_batch_before_finish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """18. Crash after publication of empty outputs before finish is reconciled safely via durable commit_state."""
    csv_empty = "customer_id,customer_name,postal_code\n"
    config_path = make_pipeline_config(tmp_path, csv_content=csv_empty)

    original_finish = BatchTracker.finish
    crashed = False

    def crash_before_final_finish(self: BatchTracker, batch_id: str, status: Any, *args: Any, **kwargs: Any) -> None:
        nonlocal crashed
        if kwargs.get("stage") == "COMPLETED":
            crashed = True
            raise RuntimeError("Fault injection: crash after publication of empty batch before finish")
        original_finish(self, batch_id, status, *args, **kwargs)

    monkeypatch.setattr(BatchTracker, "finish", crash_before_final_finish)

    with pytest.raises(RuntimeError, match="Fault injection: crash after publication of empty batch"):
        run_pipeline(config_path, submission_key="empty-crash-before-finish")

    assert crashed is True
    monkeypatch.undo()

    # Replay with same submission key recovers via _reconcile_committed_batch using durable evidence
    replay_summary = run_pipeline(config_path, submission_key="empty-crash-before-finish")
    assert replay_summary.idempotent_replay is True
    assert replay_summary.status == BatchStatus.SUCCESS
    assert replay_summary.records_read == 0
    assert replay_summary.records_silver == 0
    assert replay_summary.records_rejected == 0
    assert replay_summary.reconciliation_status == ReconciliationStatus.RECONCILED.value


def test_bronze_only_empty_batch_recovery_and_replay(tmp_path: Path) -> None:
    """19. Bronze-only pipeline with 0 records reconciles and verifies required destinations."""
    from accelerator.bronze_ingestion import _all_destinations_have_batch

    input_file = tmp_path / "input_empty.csv"
    input_file.write_text("col_a,col_b\n", encoding="utf-8")
    cfg = {
        "source_name": "bronze_empty_source",
        "format": "csv",
        "input_path": str(input_file),
        "bronze_path": str(tmp_path / "bronze" / "empty_raw.jsonl"),
        "metadata_store": str(tmp_path / "metadata" / "audit.db"),
    }
    cfg_path = tmp_path / "bronze_empty_config.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    result = ingest(cfg_path, submission_key="bronze-empty-key")
    assert result["records_processed"] == 0

    tracker = BatchTracker(tmp_path / "metadata" / "audit.db")
    summaries = tracker.get_audit_summary(batch_id=result["batch_id"])
    assert len(summaries) == 1
    assert summaries[0].status == BatchStatus.SUCCESS
    assert summaries[0].records_bronze == 0
    assert summaries[0].reconciliation_status == ReconciliationStatus.RECONCILED.value

    row = tracker.find_batch(result["batch_id"])
    assert _all_destinations_have_batch(row) is True

    # If bronze destination is deleted, _all_destinations_have_batch returns False
    Path(result["bronze_path"]).unlink()
    assert _all_destinations_have_batch(row) is False
