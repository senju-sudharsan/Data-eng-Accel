"""Tests for Stage 1: Pipeline Runner Dependency Injection Boundary.

Verifies that run_pipeline() accepts optional custom tracker and Bronze store
implementations while maintaining 100% backward compatibility when omitted.
Ensures rigorous assertions across:
- Ingestion lineage and raw preservation
- Batch tracker lifecycle transitions and audit metrics
- Quality rule quarantine splitting and partial success status
- Transformation failure recording
- Idempotent submission replay
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pytest

from accelerator.contracts import (
    BatchAuditSummary,
    BatchSourceConfigProtocol,
    BatchStatus,
    BatchTrackerProtocol,
    BronzeStoreProtocol,
    PipelineConfig,
    load_pipeline_config,
)
from accelerator.pipeline_runner import run_pipeline


class RecordingBronzeStore:
    """Mock store implementing BronzeStoreProtocol using append()."""

    def __init__(self) -> None:
        self.appended_records: list[dict[str, Any]] = []
        self.destinations: list[Path] = []

    def append(self, destination: Path, records: Iterable[dict[str, Any]]) -> int:
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        rec_list = list(records)
        self.destinations.append(destination)
        self.appended_records.extend(rec_list)
        # Write to destination so downstream BronzeReader / checks can read if needed
        with destination.open("a", encoding="utf-8") as f:
            for r in rec_list:
                f.write(json.dumps(r, default=str) + "\n")
        return len(rec_list)


class RecordingBatchTracker:
    """Mock tracker implementing BatchTrackerProtocol with submission support."""

    def __init__(self) -> None:
        self.started_batches: list[dict[str, Any]] = []
        self.prepared_outputs: list[dict[str, Any]] = []
        self.finished_batches: list[dict[str, Any]] = []
        self.submissions: dict[str, dict[str, Any]] = {}
        self.batches: dict[str, dict[str, Any]] = {}
        self.last_batch_id: str | None = None

    def start(
        self,
        config: BatchSourceConfigProtocol,
        ingested_at: str,
        submission_key: str | None = None,
        submission_fingerprint: str | None = None,
        lease_token: str | None = None,
        *,
        silver_path: str | Path | None = None,
        rejected_path: str | Path | None = None,
        stage: str = "BRONZE",
    ) -> str:
        batch_id = f"mock-batch-{len(self.started_batches) + 1}"
        self.last_batch_id = batch_id
        batch_info = {
            "batch_id": batch_id,
            "source_name": config.source_name,
            "input_path": str(config.input_path),
            "format": config.format,
            "status": "RUNNING",
            "stage": stage,
            "batch_start_time": ingested_at,
            "submission_key": submission_key,
            "submission_fingerprint": submission_fingerprint,
            "bronze_path": str(config.bronze_path),
            "silver_path": str(silver_path) if silver_path else None,
            "rejected_path": str(rejected_path) if rejected_path else None,
            "lease_token": lease_token,
            "records_processed": 0,
            "records_persisted": 0,
            "records_silver": 0,
            "records_scd2_changed": None,
            "records_rejected": 0,
            "records_failed": 0,
            "commit_state": "NONE",
        }
        self.started_batches.append(dict(batch_info))
        self.batches[batch_id] = dict(batch_info)
        if submission_key:
            self.submissions[submission_key] = self.batches[batch_id]
        return batch_id

    def find_submission(self, submission_key: str) -> dict[str, Any] | None:
        return self.submissions.get(submission_key)

    def find_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self.batches.get(batch_id)

    def lease_is_active(self, row: dict[str, Any]) -> bool:
        return False

    def restart(
        self,
        batch_id: str,
        lease_token: str,
        *,
        silver_path: str | Path | None = None,
        rejected_path: str | Path | None = None,
        stage: str = "BRONZE",
        expected_lease_token: str | None = None,
    ) -> bool:
        b = self.batches.get(batch_id)
        if b:
            b["stage"] = stage
            b["status"] = "RUNNING"
            b["lease_token"] = lease_token
            return True
        return False

    def prepare(self, batch_id: str, records_processed: int, staged_path: Path | str) -> None:
        b = self.batches.get(batch_id)
        if b:
            b["records_processed"] = records_processed
            b["staged_path"] = str(staged_path)
            b["commit_state"] = "STAGED"

    def mark_committed(self, batch_id: str) -> None:
        b = self.batches.get(batch_id)
        if b:
            b["commit_state"] = "COMMITTED"

    def set_commit_state(self, batch_id: str, commit_state: str) -> None:
        b = self.batches.get(batch_id)
        if b:
            b["commit_state"] = commit_state

    def update_stage(self, batch_id: str, stage: str, commit_state: str | None = None) -> None:
        b = self.batches.get(batch_id)
        if b:
            b["stage"] = stage
            if commit_state:
                b["commit_state"] = commit_state

    def set_scd2_changed(self, batch_id: str, records_scd2_changed: int) -> None:
        b = self.batches.get(batch_id)
        if b:
            b["records_scd2_changed"] = records_scd2_changed

    def prepare_outputs(
        self,
        batch_id: str,
        records_silver: int,
        records_rejected: int,
        stage: str = "PUBLICATION",
        commit_state: str = "OUTPUTS_STAGED",
        records_scd2_changed: int | None = None,
    ) -> None:
        self.prepared_outputs.append({
            "batch_id": batch_id,
            "records_silver": records_silver,
            "records_rejected": records_rejected,
            "records_scd2_changed": records_scd2_changed,
            "stage": stage,
            "commit_state": commit_state,
        })
        b = self.batches.get(batch_id)
        if b:
            b["records_silver"] = records_silver
            b["records_rejected"] = records_rejected
            b["records_scd2_changed"] = records_scd2_changed
            b["stage"] = stage
            b["commit_state"] = commit_state

    def finish(
        self,
        batch_id: str,
        status: BatchStatus | str,
        records_processed: int,
        finished_at: str,
        records_persisted: int | None = None,
        error_message: str | None = None,
        *,
        records_silver: int = 0,
        records_scd2_changed: int | None = None,
        records_rejected: int = 0,
        records_failed: int | None = 0,
        stage: str = "COMPLETED",
        reconciliation_status: str | None = None,
        silver_path: str | Path | None = None,
        rejected_path: str | Path | None = None,
    ) -> None:
        fin_info = {
            "batch_id": batch_id,
            "status": status.value if hasattr(status, "value") else str(status),
            "records_processed": records_processed,
            "records_persisted": records_persisted or records_processed,
            "records_silver": records_silver,
            "records_scd2_changed": records_scd2_changed,
            "records_rejected": records_rejected,
            "records_failed": records_failed,
            "stage": stage,
            "reconciliation_status": reconciliation_status or "RECONCILED",
            "error_message": error_message,
            "finished_at": finished_at,
        }
        self.finished_batches.append(fin_info)
        b = self.batches.get(batch_id)
        if b:
            b.update(fin_info)
            b["status"] = fin_info["status"]
            b["commit_state"] = "COMMITTED"

    def get_audit_summary(
        self,
        batch_id: str | None = None,
        submission_key: str | None = None,
    ) -> list[BatchAuditSummary]:
        target_id = batch_id or self.last_batch_id or "mock-batch-1"
        finished = next((b for b in self.finished_batches if b["batch_id"] == target_id), None)
        started = next((b for b in self.started_batches if b["batch_id"] == target_id), None)
        status_val = finished["status"] if finished else (started["status"] if started else "SUCCESS")
        try:
            status_enum = BatchStatus(status_val)
        except ValueError:
            status_enum = BatchStatus.FAILED

        return [
            BatchAuditSummary(
                batch_id=target_id,
                source_name=started["source_name"] if started else "injected_source",
                status=status_enum,
                stage=finished["stage"] if finished else "COMPLETED",
                batch_start_time=started["batch_start_time"] if started else "2026-10-29T12:00:00Z",
                batch_end_time=finished["finished_at"] if finished else "2026-10-29T12:01:00Z",
                duration_seconds=1.0,
                records_read=finished["records_processed"] if finished else 2,
                records_bronze=finished["records_persisted"] if finished else 2,
                records_silver=finished["records_silver"] if finished else 2,
                records_scd2_changed=finished.get("records_scd2_changed") if finished else None,
                records_rejected=finished["records_rejected"] if finished else 0,
                records_failed=finished.get("records_failed") if finished else 0,
                reconciliation_status=finished["reconciliation_status"] if finished else "RECONCILED",
                reconciliation_discrepancy="",
                error_message=finished.get("error_message") if finished else None,
                submission_key=submission_key or (started.get("submission_key") if started else None),
                idempotent_replay=False,
            )
        ]


def _make_test_config(tmp_path: Path, csv_content: str = "id,name\n1,Alice\n2,Bob\n") -> Path:
    """Create a minimal pipeline configuration for testing injection."""
    input_file = tmp_path / "raw.csv"
    input_file.write_text(csv_content, encoding="utf-8")

    cfg_data = {
        "source_name": "test_di_source",
        "format": "csv",
        "input_path": str(input_file),
        "bronze_path": str(tmp_path / "bronze.jsonl"),
        "metadata_store": str(tmp_path / "meta.db"),
        "silver": {
            "target_name": "dim_test_di",
            "silver_path": str(tmp_path / "dim_test.jsonl"),
            "rejected_path": str(tmp_path / "quarantine_test.jsonl"),
            "primary_keys": ["id"],
            "columns": [
                {"name": "id", "data_type": "string", "nullable": False},
                {"name": "name", "data_type": "string", "nullable": False},
            ],
            "mappings": [
                {"source_column": "id", "target_column": "id"},
                {"source_column": "name", "target_column": "name"},
            ],
            "quality_rules": [
                {"rule_name": "not_null", "columns": ["id"], "severity": "reject"},
                {"rule_name": "regex", "columns": ["name"], "severity": "reject", "params": {"pattern": "^[A-Za-z]+$"}},
            ],
        },
    }
    cfg_file = tmp_path / "pipeline_di.json"
    cfg_file.write_text(json.dumps(cfg_data), encoding="utf-8")
    return cfg_file


def test_run_pipeline_default_parameters_backward_compatibility(tmp_path: Path) -> None:
    """Verify run_pipeline() continues to work when tracker and bronze_store are omitted."""
    cfg_file = _make_test_config(tmp_path)
    summary = run_pipeline(cfg_file)

    assert isinstance(summary, BatchAuditSummary)
    assert summary.status == BatchStatus.SUCCESS
    assert summary.records_read == 2
    assert summary.records_silver == 2
    assert summary.records_rejected == 0
    assert summary.reconciliation_status == "RECONCILED"
    assert Path(tmp_path / "bronze.jsonl").exists()
    assert Path(tmp_path / "dim_test.jsonl").exists()
    assert Path(tmp_path / "meta.db").exists()


def test_run_pipeline_injected_bronze_store_records_lineage(tmp_path: Path) -> None:
    """Verify run_pipeline() invokes store.append() with accurate Bronze lineage decoration."""
    cfg_file = _make_test_config(tmp_path)
    custom_store = RecordingBronzeStore()
    assert isinstance(custom_store, BronzeStoreProtocol)

    summary = run_pipeline(cfg_file, bronze_store=custom_store)

    assert summary.status == BatchStatus.SUCCESS
    assert len(custom_store.destinations) == 1
    assert custom_store.destinations[0] == (tmp_path / "bronze.jsonl").resolve()
    assert len(custom_store.appended_records) == 2

    # Assert lineage metadata attached to raw payload
    record_1 = custom_store.appended_records[0]
    assert record_1["id"] == "1"
    assert record_1["name"] == "Alice"
    assert record_1["_ingestion_source_name"] == "test_di_source"
    assert record_1["_ingestion_batch_id"] == summary.batch_id
    assert "_ingestion_timestamp" in record_1

    record_2 = custom_store.appended_records[1]
    assert record_2["id"] == "2"
    assert record_2["name"] == "Bob"
    assert record_2["_ingestion_batch_id"] == summary.batch_id


def test_run_pipeline_injected_tracker_lifecycle_metrics(tmp_path: Path) -> None:
    """Verify run_pipeline() invokes tracker start, prepare_outputs, and finish with exact metrics."""
    cfg_file = _make_test_config(tmp_path)
    custom_tracker = RecordingBatchTracker()
    assert isinstance(custom_tracker, BatchTrackerProtocol)

    summary = run_pipeline(cfg_file, tracker=custom_tracker)

    assert summary.status == BatchStatus.SUCCESS
    # Check start() call details
    assert len(custom_tracker.started_batches) == 1
    start_info = custom_tracker.started_batches[0]
    assert start_info["source_name"] == "test_di_source"
    assert start_info["stage"] == "BRONZE"

    # Check prepare_outputs() call details
    assert len(custom_tracker.prepared_outputs) == 1
    prep_info = custom_tracker.prepared_outputs[0]
    assert prep_info["records_silver"] == 2
    assert prep_info["records_rejected"] == 0
    assert prep_info["stage"] == "PUBLICATION"

    # Check finish() call details
    assert len(custom_tracker.finished_batches) == 1
    finish_info = custom_tracker.finished_batches[0]
    assert finish_info["status"] == "SUCCESS"
    assert finish_info["records_processed"] == 2
    assert finish_info["records_silver"] == 2
    assert finish_info["records_rejected"] == 0
    assert finish_info["stage"] == "COMPLETED"


def test_run_pipeline_injected_tracker_captures_quarantine_metrics(tmp_path: Path) -> None:
    """Verify that records rejected by quality rules report PARTIAL_SUCCESS to the injected tracker."""
    # "Bob123" fails regex rule "^[A-Za-z]+$"
    cfg_file = _make_test_config(tmp_path, csv_content="id,name\n1,Alice\n2,Bob123\n")
    custom_tracker = RecordingBatchTracker()

    summary = run_pipeline(cfg_file, tracker=custom_tracker)

    assert summary.status == BatchStatus.PARTIAL_SUCCESS
    assert summary.records_read == 2
    assert summary.records_silver == 1
    assert summary.records_rejected == 1

    # Injected tracker must record the quarantine counts
    prep_info = custom_tracker.prepared_outputs[0]
    assert prep_info["records_silver"] == 1
    assert prep_info["records_rejected"] == 1

    finish_info = custom_tracker.finished_batches[0]
    assert finish_info["status"] == "PARTIAL_SUCCESS"
    assert finish_info["records_silver"] == 1
    assert finish_info["records_rejected"] == 1


def test_run_pipeline_injected_tracker_records_failure_on_exception(tmp_path: Path) -> None:
    """Verify that transformation errors call tracker.finish(status='FAILED') with error details."""
    input_file = tmp_path / "raw.csv"
    input_file.write_text("id,name\n1,Alice\n", encoding="utf-8")

    # Pipeline config with non-existent source column mapped to required target -> triggers exception
    bad_cfg_data = {
        "source_name": "fail_source",
        "format": "csv",
        "input_path": str(input_file),
        "bronze_path": str(tmp_path / "bronze.jsonl"),
        "metadata_store": str(tmp_path / "meta.db"),
        "silver": {
            "target_name": "dim_fail",
            "silver_path": str(tmp_path / "dim_fail.jsonl"),
            "columns": [
                {"name": "missing_col", "data_type": "integer", "nullable": False},
            ],
            "missing_columns": "fail",
        },
    }
    cfg_file = tmp_path / "bad_pipeline.json"
    cfg_file.write_text(json.dumps(bad_cfg_data), encoding="utf-8")

    custom_tracker = RecordingBatchTracker()

    with pytest.raises(ValueError):
        run_pipeline(cfg_file, tracker=custom_tracker)

    # Injected tracker finish must have been invoked with FAILED status
    assert len(custom_tracker.finished_batches) == 1
    finish_info = custom_tracker.finished_batches[0]
    assert finish_info["status"] == "FAILED"
    assert finish_info["stage"] == "SILVER"
    assert finish_info["error_message"] is not None
    assert "ValueError" in finish_info["error_message"]


def test_run_pipeline_injected_tracker_supports_idempotent_replay(tmp_path: Path) -> None:
    """Verify that an injected tracker supporting find_submission enables idempotent replay."""
    cfg_file = _make_test_config(tmp_path)
    custom_tracker = RecordingBatchTracker()

    # First run
    summary_1 = run_pipeline(cfg_file, submission_key="sub_key_100", tracker=custom_tracker)
    assert summary_1.status == BatchStatus.SUCCESS
    assert summary_1.idempotent_replay is False

    # Second run with same submission key
    summary_2 = run_pipeline(cfg_file, submission_key="sub_key_100", tracker=custom_tracker)
    assert summary_2.status == BatchStatus.SUCCESS
    assert summary_2.idempotent_replay is True
    assert summary_2.batch_id == summary_1.batch_id
