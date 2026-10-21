"""Configurable raw-file ingestion for the local Bronze demonstration.

The reader and landing store are intentionally separate: a Fabric landing-store
adapter can later replace ``LocalBronzeStore`` without changing source configs.
"""

from __future__ import annotations

import csv
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


REQUIRED_CONFIG_FIELDS = {"source_name", "format", "input_path", "bronze_path", "metadata_store"}
SUPPORTED_FORMATS = {"csv", "json"}


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


class BatchTracker:
    """Local equivalent of the original project's metadata.batch_control pattern."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
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

    def start(self, config: SourceConfig, ingested_at: str) -> str:
        batch_id = str(uuid.uuid4())
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                "INSERT INTO batch_control VALUES (?, ?, ?, ?, 'RUNNING', ?, NULL, 0)",
                (batch_id, config.source_name, str(config.input_path), config.format, ingested_at),
            )
        return batch_id

    def finish(self, batch_id: str, status: str, records_processed: int, finished_at: str) -> None:
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """UPDATE batch_control SET status = ?, batch_end_time = ?, records_processed = ?
                   WHERE batch_id = ?""",
                (status, finished_at, records_processed, batch_id),
            )


class LocalBronzeStore:
    """Append raw records as JSON Lines; replace this class for cloud/Fabric landing."""

    def append(self, destination: Path, records: Iterable[dict[str, Any]]) -> int:
        destination.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with destination.open("a", encoding="utf-8", newline="\n") as bronze_file:
            for record in records:
                bronze_file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        return count


def ingest(config_path: str | Path) -> dict[str, Any]:
    """Execute one source configuration and return its batch summary."""
    config = load_config(config_path)
    timestamp = datetime.now(timezone.utc).isoformat()
    tracker = BatchTracker(config.metadata_store)
    batch_id = tracker.start(config, timestamp)
    try:
        records = (
            {
                **record,
                "_ingestion_source_name": config.source_name,
                "_ingestion_batch_id": batch_id,
                "_ingestion_timestamp": timestamp,
            }
            for record in read_source(config)
        )
        row_count = LocalBronzeStore().append(config.bronze_path, records)
        tracker.finish(batch_id, "SUCCESS", row_count, datetime.now(timezone.utc).isoformat())
    except Exception:
        tracker.finish(batch_id, "FAILED", 0, datetime.now(timezone.utc).isoformat())
        raise
    return {"batch_id": batch_id, "source_name": config.source_name, "records_processed": row_count, "bronze_path": str(config.bronze_path)}
