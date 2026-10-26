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

from accelerator.contracts import BatchSourceConfigProtocol


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
    ) -> str:
        batch_id = str(uuid.uuid4())
        with sqlite3.connect(self.database_path) as connection:
            try:
                connection.execute(
                    """INSERT INTO batch_control
                (batch_id, source_name, input_path, format, status, batch_start_time,
                 records_processed, records_persisted, submission_key,
                 submission_fingerprint, bronze_path, lease_token, lease_expires_at)
                VALUES (?, ?, ?, ?, 'RUNNING', ?, 0, 0, ?, ?, ?, ?, ?)""",
                    (batch_id, config.source_name, str(config.input_path), config.format, ingested_at,
                     submission_key, submission_fingerprint, str(config.bronze_path), lease_token,
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

    def lease_is_active(self, row: sqlite3.Row) -> bool:
        if not row["lease_expires_at"]:
            return False
        try:
            return datetime.fromisoformat(row["lease_expires_at"]) > datetime.now(timezone.utc)
        except ValueError:
            return False

    def restart(self, batch_id: str, lease_token: str) -> None:
        with sqlite3.connect(self.database_path) as connection:
            connection.execute("""UPDATE batch_control SET status = 'RUNNING', batch_start_time = ?, batch_end_time = NULL,
                error_message = NULL, records_processed = 0, records_persisted = 0, staged_path = NULL,
                commit_state = 'NONE', lease_token = ?, lease_expires_at = ? WHERE batch_id = ?""",
                (datetime.now(timezone.utc).isoformat(), lease_token,
                 (datetime.now(timezone.utc) + LEASE_DURATION).isoformat(), batch_id))

    def finish(
        self, batch_id: str, status: str, records_processed: int, finished_at: str,
        records_persisted: int | None = None, error_message: str | None = None,
    ) -> None:
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """UPDATE batch_control SET status = ?, batch_end_time = ?, records_processed = ?,
                   records_persisted = ?, error_message = ?, staged_path = NULL,
                   commit_state = CASE WHEN ? = 'SUCCESS' THEN 'COMMITTED' ELSE commit_state END WHERE batch_id = ?""",
                (status, finished_at, records_processed,
                 records_processed if records_persisted is None else records_persisted,
                 error_message, status, batch_id),
            )

    def recover_stale(self, stale_after: timedelta = timedelta(minutes=5)) -> int:
        """Recover abandoned (expired-lease) batches; age alone never takes ownership."""
        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM batch_control WHERE status = 'RUNNING'").fetchall()
        recovered = 0
        for row in rows:
            if self.lease_is_active(row):
                continue
            staged = Path(row["staged_path"]) if row["staged_path"] else None
            destination = Path(row["bronze_path"]) if row["bronze_path"] else None
            if not destination:
                continue
            with _destination_lock(destination):
                current = self.find_batch(row["batch_id"])
                if not current or current["status"] != "RUNNING" or self.lease_is_active(current):
                    continue
                # Destination evidence wins over stale metadata after publication.
                if _destination_has_batch(destination, current["batch_id"]):
                    self.finish(current["batch_id"], "SUCCESS", current["records_processed"], datetime.now(timezone.utc).isoformat())
                else:
                    # Do not promote an old full-file snapshot: a later batch may
                    # have committed after this process crashed.
                    staged = Path(current["staged_path"]) if current["staged_path"] else None
                    if staged and staged.exists():
                        staged.unlink()
                    self.finish(current["batch_id"], "FAILED", current["records_processed"], datetime.now(timezone.utc).isoformat(), 0, "Abandoned before Bronze commit; retry the submission key.")
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


def _destination_has_batch(destination: Path, batch_id: str) -> bool:
    if not destination.exists():
        return False
    try:
        return any(
            json.loads(line).get("_ingestion_batch_id") == batch_id
            for line in destination.read_text(encoding="utf-8").splitlines() if line.strip()
        )
    except json.JSONDecodeError:
        return False


def _reconcile_committed_batch(tracker: BatchTracker, row: sqlite3.Row, destination: Path) -> bool:
    """Finalize metadata when publication succeeded before SQLite finalization."""
    if _destination_has_batch(destination, row["batch_id"]):
        tracker.finish(row["batch_id"], "SUCCESS", row["records_processed"],
                       datetime.now(timezone.utc).isoformat(), row["records_processed"])
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
                tracker.restart(existing["batch_id"], lease_token)
                batch_id = existing["batch_id"]
            else:
                batch_id = existing["batch_id"]
                tracker.restart(batch_id, lease_token)
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
