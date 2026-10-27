"""Reliable local raw-file ingestion for the Bronze demonstration.

The reader and landing store are intentionally separate: a Fabric landing-store
adapter can later replace ``LocalBronzeStore`` without changing source configs.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from accelerator.contracts import (
    BatchAuditSummary,
    BatchSourceConfigProtocol,
    BatchStatus,
    ReconciliationStatus,
)


REQUIRED_CONFIG_FIELDS = {"source_name", "format", "input_path", "bronze_path", "metadata_store"}
SUPPORTED_FORMATS = {"csv", "json"}
LEASE_DURATION = timedelta(minutes=15)
_destination_locks: dict[str, threading.Lock] = {}
_destination_locks_guard = threading.Lock()


@contextmanager
def _destination_lock(destination: Path) -> Iterable[None]:
    """Serialize a destination in this process and between Windows processes."""
    lock_path = destination.with_name(f".{destination.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    key = str(lock_path.resolve())
    with _destination_locks_guard:
        thread_lock = _destination_locks.setdefault(key, threading.Lock())
    with thread_lock:
        with lock_path.open("a+b") as lock_file:
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                if os.name == "nt":
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)


@dataclass(frozen=True)
class SourceConfig:
    source_name: str
    format: str
    input_path: Path
    bronze_path: Path
    metadata_store: Path
    delimiter: str = ","
    encoding: str = "utf-8"


def load_config(config_path: str | Path) -> SourceConfig:
    """Load and validate a source configuration, resolving paths from its folder."""
    config_file = Path(config_path).resolve()
    try:
        payload = json.loads(config_file.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"Configuration file does not exist: {config_file}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"Configuration is not valid JSON: {error.msg}") from error

    if not isinstance(payload, dict):
        raise ValueError("Configuration must be a JSON object")
    missing = REQUIRED_CONFIG_FIELDS - payload.keys()
    if missing:
        raise ValueError(f"Configuration is missing required field(s): {', '.join(sorted(missing))}")
    if payload["format"] not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format {payload['format']!r}; expected csv or json")
    if not all(isinstance(payload[field], str) and payload[field].strip() for field in REQUIRED_CONFIG_FIELDS):
        raise ValueError("Required configuration fields must be non-empty strings")

    def resolve(value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else (config_file.parent / path).resolve()

    input_path = resolve(payload["input_path"])
    if not input_path.exists():
        raise ValueError(f"Input file does not exist: {input_path}")

    delimiter = payload.get("delimiter", ",")
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError("delimiter must be one character")
    encoding = payload.get("encoding", "utf-8")
    if not isinstance(encoding, str) or not encoding:
        raise ValueError("encoding must be a non-empty string")

    return SourceConfig(
        source_name=payload["source_name"],
        format=payload["format"],
        input_path=input_path,
        bronze_path=resolve(payload["bronze_path"]),
        metadata_store=resolve(payload["metadata_store"]),
        delimiter=delimiter,
        encoding=encoding,
    )


def read_source(config: SourceConfig) -> Iterable[dict[str, Any]]:
    """Read raw records without coercing CSV field values."""
    if config.format == "csv":
        with config.input_path.open("r", encoding=config.encoding, newline="") as source_file:
            yield from csv.DictReader(source_file, delimiter=config.delimiter)
        return

    # Small JSON extension: accepts a JSON array or newline-delimited JSON objects.
    text = config.input_path.read_text(encoding=config.encoding).strip()
    if not text:
        return
    parsed = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines() if line.strip()]
    if not isinstance(parsed, list) or not all(isinstance(item, dict) for item in parsed):
        raise ValueError("JSON input must be an array or NDJSON stream of objects")
    yield from parsed



class SourceReader:
    """Reader component conforming to SourceReaderProtocol."""

    def read(self, config: Any) -> Iterable[dict[str, Any]]:
        """Read source records preserving raw representation.

        Accepts either a SourceConfig or PipelineConfig instance.
        """
        if not isinstance(config, SourceConfig):
            cfg = SourceConfig(
                source_name=config.source_name,
                format=config.format,
                input_path=Path(config.input_path),
                bronze_path=Path(config.bronze_path),
                metadata_store=Path(config.metadata_store),
                delimiter=getattr(config, 'delimiter', ','),
                encoding=getattr(config, 'encoding', 'utf-8'),
            )
        else:
            cfg = config
        return read_source(cfg)

class BatchTracker:
    """Local equivalent of the original project's metadata.batch_control pattern."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            # Serialize first-use schema migration across concurrent ingestions.
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS batch_control (
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
            existing = {row[1] for row in connection.execute("PRAGMA table_info(batch_control)")}
            for name, definition in {
                "error_message": "TEXT",
                "records_persisted": "INTEGER NOT NULL DEFAULT 0",
                "submission_key": "TEXT",
                "submission_fingerprint": "TEXT",
                "bronze_path": "TEXT",
                "staged_path": "TEXT",
                "commit_state": "TEXT NOT NULL DEFAULT 'NONE'",
                "lease_token": "TEXT",
                "lease_expires_at": "TEXT",
                "records_silver": "INTEGER NOT NULL DEFAULT 0",
                "records_rejected": "INTEGER NOT NULL DEFAULT 0",
                "records_failed": "INTEGER",
                "duration_seconds": "REAL",
                "stage": "TEXT NOT NULL DEFAULT 'BRONZE'",
                "reconciliation_status": "TEXT NOT NULL DEFAULT 'NONE'",
                "reconciliation_discrepancy": "TEXT",
                "silver_path": "TEXT",
                "rejected_path": "TEXT",
            }.items():
                if name not in existing:
                    connection.execute(f"ALTER TABLE batch_control ADD COLUMN {name} {definition}")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS batch_submission_key_unique "
                "ON batch_control(submission_key) WHERE submission_key IS NOT NULL"
            )

    def start(
        self, config: BatchSourceConfigProtocol, ingested_at: str, submission_key: str | None = None,
        submission_fingerprint: str | None = None, lease_token: str | None = None,
        *,
        silver_path: str | Path | None = None,
        rejected_path: str | Path | None = None,
        stage: str = "BRONZE",
    ) -> str:
        batch_id = str(uuid.uuid4())
        with sqlite3.connect(self.database_path) as connection:
            try:
                connection.execute(
                    """INSERT INTO batch_control
                (batch_id, source_name, input_path, format, status, stage, batch_start_time,
                 records_processed, records_persisted, submission_key,
                 submission_fingerprint, bronze_path, silver_path, rejected_path, lease_token, lease_expires_at)
                VALUES (?, ?, ?, ?, 'RUNNING', ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?)""",
                    (batch_id, config.source_name, str(config.input_path), config.format, stage, ingested_at,
                     submission_key, submission_fingerprint, str(config.bronze_path),
                     str(silver_path) if silver_path else None,
                     str(rejected_path) if rejected_path else None,
                     lease_token,
                     (datetime.now(timezone.utc) + LEASE_DURATION).isoformat()),
                )
            except sqlite3.IntegrityError as error:
                if submission_key is not None:
                    raise RuntimeError("Submission key is already being processed") from error
                raise
        return batch_id

    def find_submission(self, submission_key: str) -> sqlite3.Row | None:
        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(
                "SELECT * FROM batch_control WHERE submission_key = ?", (submission_key,)
            ).fetchone()

    def find_batch(self, batch_id: str) -> sqlite3.Row | None:
        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute("SELECT * FROM batch_control WHERE batch_id = ?", (batch_id,)).fetchone()

    def prepare(self, batch_id: str, records_processed: int, staged_path: Path) -> None:
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                "UPDATE batch_control SET records_processed = ?, staged_path = ?, commit_state = 'STAGED' WHERE batch_id = ?",
                (records_processed, str(staged_path), batch_id),
            )

    def mark_committed(self, batch_id: str) -> None:
        with sqlite3.connect(self.database_path) as connection:
            connection.execute("UPDATE batch_control SET commit_state = 'COMMITTED' WHERE batch_id = ?", (batch_id,))

    def set_commit_state(self, batch_id: str, commit_state: str) -> None:
        with sqlite3.connect(self.database_path) as connection:
            connection.execute("UPDATE batch_control SET commit_state = ? WHERE batch_id = ?", (commit_state, batch_id))

    def update_stage(self, batch_id: str, stage: str, commit_state: str | None = None) -> None:
        with sqlite3.connect(self.database_path) as connection:
            if commit_state:
                connection.execute(
                    "UPDATE batch_control SET stage = ?, commit_state = ? WHERE batch_id = ?",
                    (stage, commit_state, batch_id),
                )
            else:
                connection.execute(
                    "UPDATE batch_control SET stage = ? WHERE batch_id = ?",
                    (stage, batch_id),
                )

    def prepare_outputs(
        self,
        batch_id: str,
        records_silver: int,
        records_rejected: int,
        stage: str = "PUBLICATION",
        commit_state: str = "OUTPUTS_STAGED",
    ) -> None:
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                "UPDATE batch_control SET records_silver = ?, records_rejected = ?, stage = ?, commit_state = ? WHERE batch_id = ?",
                (records_silver, records_rejected, stage, commit_state, batch_id),
            )

    def lease_is_active(self, row: sqlite3.Row) -> bool:
        if not row["lease_expires_at"]:
            return False
        try:
            return datetime.fromisoformat(row["lease_expires_at"]) > datetime.now(timezone.utc)
        except ValueError:
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
        now_iso = datetime.now(timezone.utc).isoformat()
        lease_expiry = (datetime.now(timezone.utc) + LEASE_DURATION).isoformat()
        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.execute(
                """UPDATE batch_control SET
                    status = 'RUNNING',
                    stage = ?,
                    batch_start_time = ?,
                    batch_end_time = NULL,
                    duration_seconds = NULL,
                    error_message = NULL,
                    records_processed = 0,
                    records_persisted = 0,
                    records_silver = 0,
                    records_rejected = 0,
                    records_failed = 0,
                    reconciliation_status = 'NONE',
                    reconciliation_discrepancy = NULL,
                    staged_path = NULL,
                    silver_path = COALESCE(?, silver_path),
                    rejected_path = COALESCE(?, rejected_path),
                    commit_state = 'NONE',
                    lease_token = ?,
                    lease_expires_at = ?
                WHERE batch_id = ?
                  AND (status != 'RUNNING' OR lease_expires_at IS NULL OR lease_expires_at < ? OR (? IS NOT NULL AND lease_token = ?))""",
                (
                    stage,
                    now_iso,
                    str(silver_path) if silver_path else None,
                    str(rejected_path) if rejected_path else None,
                    lease_token,
                    lease_expiry,
                    batch_id,
                    now_iso,
                    expected_lease_token,
                    expected_lease_token,
                ),
            )
            return cursor.rowcount > 0

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
        records_rejected: int = 0,
        records_failed: int | None = 0,
        stage: str = "COMPLETED",
        reconciliation_status: str | None = None,
        silver_path: str | Path | None = None,
        rejected_path: str | Path | None = None,
    ) -> None:
        status_val = status.value if hasattr(status, "value") else str(status)
        persisted = records_processed if records_persisted is None else records_persisted
        cleaned_error = str(error_message).replace("\n", " ")[:1000] if error_message is not None else None

        duration_seconds: float | None = None
        db_silver_path = None
        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT batch_start_time, silver_path FROM batch_control WHERE batch_id = ?", (batch_id,)).fetchone()
            if row:
                db_silver_path = row["silver_path"]
                if row["batch_start_time"]:
                    try:
                        start_dt = datetime.fromisoformat(row["batch_start_time"])
                        end_dt = datetime.fromisoformat(finished_at)
                        duration_seconds = max(0.0, (end_dt - start_dt).total_seconds())
                    except (ValueError, TypeError):
                        duration_seconds = None

        is_silver_pipeline = bool(silver_path or db_silver_path or (stage not in ("BRONZE", "COMPLETED") and (records_silver > 0 or records_rejected > 0)))

        if reconciliation_status is not None:
            recon_status = reconciliation_status.value if hasattr(reconciliation_status, "value") else str(reconciliation_status)
            recon_discrepancy = cleaned_error or ""
        elif status_val == BatchStatus.FAILED.value or status_val == "FAILED":
            recon_status = ReconciliationStatus.FAILED.value
            recon_discrepancy = cleaned_error or "Batch failed during execution"
        elif status_val in (BatchStatus.SUCCESS.value, BatchStatus.PARTIAL_SUCCESS.value, "SUCCESS", "PARTIAL_SUCCESS"):
            if not is_silver_pipeline and (stage == "BRONZE" or (records_silver == 0 and records_rejected == 0 and (records_failed or 0) == 0)):
                if records_processed == persisted:
                    recon_status = ReconciliationStatus.RECONCILED.value
                    recon_discrepancy = ""
                else:
                    recon_status = ReconciliationStatus.DISCREPANCY.value
                    recon_discrepancy = f"read={records_processed} != persisted={persisted}"
            else:
                total_accounted = records_silver + records_rejected + (records_failed or 0)
                if records_processed == total_accounted:
                    recon_status = ReconciliationStatus.RECONCILED.value
                    recon_discrepancy = ""
                else:
                    recon_status = ReconciliationStatus.DISCREPANCY.value
                    recon_discrepancy = f"read={records_processed} != silver={records_silver} + rejected={records_rejected} + failed={records_failed or 0}"
        else:
            recon_status = ReconciliationStatus.INCOMPLETE.value
            recon_discrepancy = "Batch incomplete"

        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """UPDATE batch_control SET
                    status = ?,
                    stage = ?,
                    batch_end_time = ?,
                    duration_seconds = ?,
                    records_processed = ?,
                    records_persisted = ?,
                    records_silver = ?,
                    records_rejected = ?,
                    records_failed = ?,
                    reconciliation_status = ?,
                    reconciliation_discrepancy = ?,
                    error_message = ?,
                    staged_path = NULL,
                    silver_path = COALESCE(?, silver_path),
                    rejected_path = COALESCE(?, rejected_path),
                    commit_state = CASE WHEN ? IN ('SUCCESS', 'PARTIAL_SUCCESS') THEN 'COMMITTED' ELSE commit_state END
                WHERE batch_id = ?""",
                (
                    status_val,
                    stage,
                    finished_at,
                    duration_seconds,
                    records_processed,
                    persisted,
                    records_silver,
                    records_rejected,
                    records_failed,
                    recon_status,
                    recon_discrepancy,
                    cleaned_error,
                    str(silver_path) if silver_path is not None else None,
                    str(rejected_path) if rejected_path is not None else None,
                    status_val,
                    batch_id,
                ),
            )

    def get_audit_summary(
        self,
        batch_id: str | None = None,
        submission_key: str | None = None,
    ) -> list[BatchAuditSummary]:
        query = "SELECT * FROM batch_control WHERE 1=1"
        params: list[Any] = []
        if batch_id is not None:
            query += " AND batch_id = ?"
            params.append(batch_id)
        if submission_key is not None:
            query += " AND submission_key = ?"
            params.append(submission_key)
        query += " ORDER BY batch_start_time ASC"

        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(query, params).fetchall()

        summaries: list[BatchAuditSummary] = []
        for row in rows:
            raw_status = row["status"]
            try:
                status_enum = BatchStatus(raw_status)
            except ValueError:
                status_enum = BatchStatus.FAILED

            recon_status = row["reconciliation_status"] or ReconciliationStatus.INCOMPLETE.value
            try:
                recon_status_val = ReconciliationStatus(recon_status).value
            except ValueError:
                recon_status_val = recon_status

            summaries.append(
                BatchAuditSummary(
                    batch_id=row["batch_id"],
                    source_name=row["source_name"],
                    status=status_enum,
                    stage=row["stage"] or "BRONZE",
                    batch_start_time=row["batch_start_time"],
                    batch_end_time=row["batch_end_time"],
                    duration_seconds=row["duration_seconds"],
                    records_read=row["records_processed"] or 0,
                    records_bronze=row["records_persisted"] or 0,
                    records_silver=row["records_silver"] or 0,
                    records_rejected=row["records_rejected"] or 0,
                    records_failed=row["records_failed"],
                    reconciliation_status=recon_status_val,
                    reconciliation_discrepancy=row["reconciliation_discrepancy"] or "",
                    error_message=row["error_message"],
                    submission_key=row["submission_key"],
                    idempotent_replay=False,
                )
            )
        return summaries

    def recover_stale(self, stale_after: timedelta = timedelta(minutes=5)) -> int:
        """Recover abandoned (expired-lease) batches; age alone never takes ownership."""
        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM batch_control WHERE status = 'RUNNING'").fetchall()
        recovered = 0
        for row in rows:
            if self.lease_is_active(row):
                continue
            bronze_path = Path(row["bronze_path"]) if row["bronze_path"] else None
            if not bronze_path:
                continue
            with _destination_lock(bronze_path):
                current = self.find_batch(row["batch_id"])
                if not current or current["status"] != "RUNNING" or self.lease_is_active(current):
                    continue
                # Destination evidence wins over stale metadata only when all expected outputs committed
                if _all_destinations_have_batch(current):
                    silver_path = Path(current["silver_path"]) if current["silver_path"] else None
                    rejected_path = Path(current["rejected_path"]) if current["rejected_path"] else None
                    records_silver = current["records_silver"]
                    if (records_silver is None or records_silver == 0) and silver_path:
                        records_silver = _count_records_for_batch(silver_path, current["batch_id"])
                    records_silver = records_silver or 0

                    records_rejected = current["records_rejected"]
                    if (records_rejected is None or records_rejected == 0) and rejected_path:
                        records_rejected = _count_records_for_batch(rejected_path, current["batch_id"])
                    records_rejected = records_rejected or 0

                    records_failed = current["records_failed"] or 0
                    final_status = "PARTIAL_SUCCESS" if records_rejected > 0 else "SUCCESS"
                    self.finish(
                        current["batch_id"],
                        final_status,
                        current["records_processed"],
                        datetime.now(timezone.utc).isoformat(),
                        records_persisted=current["records_persisted"],
                        records_silver=records_silver,
                        records_rejected=records_rejected,
                        records_failed=records_failed,
                        stage="COMPLETED",
                        silver_path=silver_path,
                        rejected_path=rejected_path,
                    )
                else:
                    staged = Path(current["staged_path"]) if current["staged_path"] else None
                    if staged and staged.exists():
                        staged.unlink()
                    self.finish(
                        current["batch_id"],
                        "FAILED",
                        current["records_processed"],
                        datetime.now(timezone.utc).isoformat(),
                        records_persisted=0,
                        error_message="Abandoned before full commit; retry the submission key.",
                        reconciliation_status=ReconciliationStatus.INCOMPLETE.value,
                    )
            recovered += 1
        return recovered


class LocalBronzeStore:
    """Stage a full JSONL snapshot then atomically publish it with ``os.replace``."""

    def stage(self, destination: Path, batch_id: str, records: Iterable[dict[str, Any]]) -> tuple[Path, int]:
        destination.parent.mkdir(parents=True, exist_ok=True)
        staged = destination.with_name(f".{destination.name}.{batch_id}.staged")
        count = 0
        try:
            with staged.open("w", encoding="utf-8", newline="\n") as bronze_file:
                if destination.exists():
                    bronze_file.write(destination.read_text(encoding="utf-8"))
                for record in records:
                    bronze_file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                    count += 1
                bronze_file.flush()
                os.fsync(bronze_file.fileno())
        except Exception:
            if staged.exists():
                staged.unlink()
            raise
        return staged, count

    def commit(self, staged: Path, destination: Path) -> None:
        os.replace(staged, destination)

    def append(self, destination: Path, records: Iterable[dict[str, Any]]) -> int:
        """Backward-compatible atomic append for callers outside ``ingest``."""
        staged, count = self.stage(destination, str(uuid.uuid4()), records)
        self.commit(staged, destination)
        return count


def _count_records_for_batch(destination: Path, batch_id: str) -> int:
    """Count how many valid records for batch_id exist in destination."""
    if not destination.exists():
        return 0
    count = 0
    try:
        with destination.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    if (
                        record.get("_ingestion_batch_id") == batch_id
                        or record.get("_silver_batch_id") == batch_id
                        or record.get("_rejection_batch_id") == batch_id
                    ):
                        count += 1
                except json.JSONDecodeError:
                    continue
        return count
    except (OSError, UnicodeDecodeError):
        return 0


def _destination_has_batch(destination: Path, batch_id: str) -> bool:
    if not destination.exists():
        return False
    try:
        with destination.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    if (
                        record.get("_ingestion_batch_id") == batch_id
                        or record.get("_silver_batch_id") == batch_id
                        or record.get("_rejection_batch_id") == batch_id
                    ):
                        return True
                except json.JSONDecodeError:
                    continue
        return False
    except (OSError, UnicodeDecodeError):
        return False


def _all_destinations_have_batch(row: sqlite3.Row) -> bool:
    batch_id = row["batch_id"]
    bronze_path = Path(row["bronze_path"]) if row["bronze_path"] else None
    if not bronze_path or not _destination_has_batch(bronze_path, batch_id):
        return False

    silver_path = Path(row["silver_path"]) if row["silver_path"] else None
    if not silver_path:
        return True

    rejected_path = Path(row["rejected_path"]) if row["rejected_path"] else None

    # Check for uncommitted staged files
    staged_silver = silver_path.with_name(f".{silver_path.name}.{batch_id}.staged")
    if staged_silver.exists():
        return False

    if rejected_path:
        staged_quarantine = rejected_path.with_name(f".{rejected_path.name}.{batch_id}.staged")
        if staged_quarantine.exists():
            return False

    records_silver = row["records_silver"]
    records_rejected = row["records_rejected"]
    commit_state = row["commit_state"] if "commit_state" in row.keys() else "NONE"

    silver_has = _destination_has_batch(silver_path, batch_id)
    if records_silver and records_silver > 0 and not silver_has:
        return False

    if rejected_path and records_rejected and records_rejected > 0:
        if not _destination_has_batch(rejected_path, batch_id):
            return False

    # Never mark a batch complete based solely on the presence of a batch ID in one destination
    # if commit_state is not COMMITTED
    if commit_state != "COMMITTED" and rejected_path:
        if not _destination_has_batch(rejected_path, batch_id):
            return False

    rejected_has = _destination_has_batch(rejected_path, batch_id) if rejected_path else False
    if not silver_has and not rejected_has:
        return False

    return True


def _reconcile_committed_batch(tracker: BatchTracker, row: sqlite3.Row, destination: Path | None = None) -> bool:
    """Finalize metadata when publication succeeded before SQLite finalization."""
    if row["silver_path"]:
        if _all_destinations_have_batch(row):
            silver_path = Path(row["silver_path"])
            rejected_path = Path(row["rejected_path"]) if row["rejected_path"] else None

            records_silver = row["records_silver"]
            if (records_silver is None or records_silver == 0) and silver_path:
                records_silver = _count_records_for_batch(silver_path, row["batch_id"])
            records_silver = records_silver or 0

            records_rejected = row["records_rejected"]
            if (records_rejected is None or records_rejected == 0) and rejected_path:
                records_rejected = _count_records_for_batch(rejected_path, row["batch_id"])
            records_rejected = records_rejected or 0

            records_failed = row["records_failed"] or 0
            final_status = "PARTIAL_SUCCESS" if records_rejected > 0 else "SUCCESS"
            tracker.finish(
                row["batch_id"],
                final_status,
                row["records_processed"],
                datetime.now(timezone.utc).isoformat(),
                records_persisted=row["records_persisted"],
                records_silver=records_silver,
                records_rejected=records_rejected,
                records_failed=records_failed,
                stage="COMPLETED",
                silver_path=silver_path,
                rejected_path=rejected_path,
            )
            return True
        return False
    dest = destination or (Path(row["bronze_path"]) if row["bronze_path"] else None)
    if dest and _destination_has_batch(dest, row["batch_id"]):
        tracker.finish(
            row["batch_id"],
            "SUCCESS",
            row["records_processed"],
            datetime.now(timezone.utc).isoformat(),
            row["records_processed"],
        )
        return True
    return False


class BronzeReader:
    """Read committed Bronze JSONL, retaining all stored lineage fields."""

    def read(self, destination: str | Path, *, source_name: str | None = None,
             batch_id: str | None = None) -> Iterable[dict[str, Any]]:
        path = Path(destination)
        if not path.exists():
            raise FileNotFoundError(f"Bronze file does not exist: {path}")
        with path.open("r", encoding="utf-8") as bronze_file:
            for line_number, line in enumerate(bronze_file, 1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f"Malformed Bronze JSON at {path}:{line_number}: {error.msg}") from error
                if not isinstance(record, dict):
                    raise ValueError(f"Bronze record at {path}:{line_number} is not a JSON object")
                if source_name is not None and record.get("_ingestion_source_name") != source_name:
                    continue
                if batch_id is not None and record.get("_ingestion_batch_id") != batch_id:
                    continue
                yield record


def _submission_fingerprint(config: SourceConfig) -> str:
    details = f"{config.source_name}\0{config.format}\0{config.input_path.resolve()}\0{config.bronze_path.resolve()}"
    return hashlib.sha256(details.encode("utf-8")).hexdigest()


def ingest(config_path: str | Path, submission_key: str | None = None) -> dict[str, Any]:
    """Execute a source config; a matching submission key is an idempotent retry.

    Calls without a key retain legacy semantics: each call intentionally creates
    a new batch even if the input is unchanged.
    """
    config = load_config(config_path)
    timestamp = datetime.now(timezone.utc).isoformat()
    tracker = BatchTracker(config.metadata_store)
    fingerprint = _submission_fingerprint(config)
    lease_token = str(uuid.uuid4())
    if submission_key is not None:
        if not isinstance(submission_key, str) or not submission_key.strip():
            raise ValueError("submission_key must be a non-empty string when provided")
        submission_key = submission_key.strip()
        existing = tracker.find_submission(submission_key)
        if existing:
            if existing["submission_fingerprint"] != fingerprint:
                raise ValueError("Submission key is already associated with incompatible source or destination details")
            if existing["status"] == "SUCCESS":
                return {"batch_id": existing["batch_id"], "source_name": existing["source_name"], "records_processed": existing["records_processed"], "bronze_path": existing["bronze_path"], "idempotent_replay": True}
            if existing["status"] == "RUNNING":
                with _destination_lock(config.bronze_path):
                    if _reconcile_committed_batch(tracker, existing, config.bronze_path):
                        return {"batch_id": existing["batch_id"], "source_name": existing["source_name"], "records_processed": existing["records_processed"], "bronze_path": existing["bronze_path"], "idempotent_replay": True}
                if tracker.lease_is_active(existing):
                    raise RuntimeError("Submission key is currently owned by an active ingestion")
                if not tracker.restart(existing["batch_id"], lease_token, expected_lease_token=existing["lease_token"]):
                    raise RuntimeError("Submission key is currently owned by an active ingestion")
                batch_id = existing["batch_id"]
            else:
                batch_id = existing["batch_id"]
                if not tracker.restart(batch_id, lease_token, expected_lease_token=existing["lease_token"]):
                    raise RuntimeError("Submission key is currently owned by an active ingestion")
        else:
            batch_id = tracker.start(config, timestamp, submission_key, fingerprint, lease_token)
    else:
        batch_id = tracker.start(config, timestamp, None, fingerprint, lease_token)
    try:
        records: list[dict[str, Any]] = []
        store = LocalBronzeStore()
        with _destination_lock(config.bronze_path):
            # The lock spans source reading as well as publication, so an
            # expired lease cannot let recovery take over a live local run.
            for record in read_source(config):
                records.append({
                    **record,
                    "_ingestion_source_name": config.source_name,
                    "_ingestion_batch_id": batch_id,
                    "_ingestion_timestamp": timestamp,
                })
            staged, row_count = store.stage(config.bronze_path, batch_id, records)
            tracker.prepare(batch_id, row_count, staged)
            store.commit(staged, config.bronze_path)
            committed = True
            tracker.mark_committed(batch_id)
            tracker.finish(batch_id, "SUCCESS", row_count, datetime.now(timezone.utc).isoformat(), row_count)
    except Exception as error:
        processed = len(records) if "records" in locals() else 0
        persisted = processed if "records" in locals() and _destination_has_batch(config.bronze_path, batch_id) else 0
        message = f"{type(error).__name__}: {error}".replace("\n", " ")[:1000]
        if persisted:
            # Publication is durable; leave/reconcile as SUCCESS rather than allow a retry to append it again.
            try:
                tracker.finish(batch_id, "SUCCESS", processed, datetime.now(timezone.utc).isoformat(), persisted, message)
            except sqlite3.Error:
                pass
        else:
            tracker.finish(batch_id, "FAILED", processed, datetime.now(timezone.utc).isoformat(), 0, message)
            if "staged" in locals() and staged.exists():
                staged.unlink()
        raise
    return {"batch_id": batch_id, "source_name": config.source_name, "records_processed": row_count, "bronze_path": str(config.bronze_path)}
