"""Local JSONL Slowly Changing Dimension Type 2 processor.

The processor treats the persisted dimension history as the source of truth for
change detection.  It stages a complete replacement file and atomically
publishes it; it deliberately does not claim cross-process writer coordination.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from accelerator.contracts import (
    LINEAGE_SILVER_BATCH_ID,
    SCD_IS_CURRENT,
    SCD_SURROGATE_KEY,
    Scd2Config,
    Scd2ProcessorProtocol,
)


class Scd2Processor:
    """Merge validated Silver records into a local JSONL Type 2 dimension."""

    def apply_scd2(
        self,
        incoming_records: list[dict[str, Any]],
        target_path: Path,
        scd2_config: Scd2Config,
        batch_id: str,
    ) -> int:
        """Apply one batch and return the number of new dimension versions.

        Current rows use ``None`` for effective-to.  On a tracked change, the
        previous row closes at the precise UTC timestamp assigned to its
        replacement.  A batch already represented in persisted history is not
        reapplied, even when a newer version is now current.
        """
        self._validate_config(scd2_config)
        if not isinstance(batch_id, str) or not batch_id.strip():
            raise ValueError("batch_id must be a non-empty string")
        normalized = self._validate_incoming(incoming_records, scd2_config, batch_id)
        existing = self._read_dimension(target_path)
        self._validate_persisted_rows(existing, scd2_config)
        current_by_key = self._current_rows(existing, scd2_config)
        applied_by_batch = self._applied_rows(existing, scd2_config)
        effective_at = datetime.now(timezone.utc).isoformat()
        inserted = 0

        for incoming in normalized:
            key = self._key_for(incoming, scd2_config.business_keys, "incoming record")
            prior_for_batch = applied_by_batch.get((key, batch_id))
            if prior_for_batch is not None:
                if not self._same_tracked_values(prior_for_batch, incoming, scd2_config.tracking_columns):
                    raise ValueError(
                        f"Batch {batch_id!r} was already applied with different tracked values for key {key!r}"
                    )
                continue

            current = current_by_key.get(key)
            if current is not None and self._same_tracked_values(current, incoming, scd2_config.tracking_columns):
                continue

            if current is not None:
                current[scd2_config.effective_to_col] = effective_at
                current[scd2_config.is_current_col] = False

            new_version = dict(incoming)
            new_version[SCD_SURROGATE_KEY] = str(uuid.uuid4())
            new_version[scd2_config.effective_from_col] = effective_at
            new_version[scd2_config.effective_to_col] = None
            new_version[scd2_config.is_current_col] = True
            existing.append(new_version)
            current_by_key[key] = new_version
            applied_by_batch[(key, batch_id)] = new_version
            inserted += 1

        if inserted:
            self._write_dimension_atomically(target_path, existing)
        return inserted

    @staticmethod
    def _validate_config(config: Scd2Config) -> None:
        if not config.enabled:
            raise ValueError("SCD2 processor requires scd2.enabled to be true")
        if not config.business_keys:
            raise ValueError("SCD2 configuration requires at least one business key")
        if not config.tracking_columns:
            raise ValueError("SCD2 configuration requires at least one tracking column")
        if any(not isinstance(name, str) or not name.strip() for name in config.business_keys + config.tracking_columns):
            raise ValueError("SCD2 business keys and tracking columns must be non-empty strings")

    def _validate_incoming(
        self, records: list[dict[str, Any]], config: Scd2Config, batch_id: str
    ) -> list[dict[str, Any]]:
        if not isinstance(records, list):
            raise ValueError("incoming_records must be a list of dictionaries")
        normalized: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                raise ValueError(f"Incoming record {index} is not a dictionary")
            copy = dict(record)
            record_batch_id = copy.get(LINEAGE_SILVER_BATCH_ID)
            if record_batch_id is None:
                copy[LINEAGE_SILVER_BATCH_ID] = batch_id
            elif record_batch_id != batch_id:
                raise ValueError(
                    f"Incoming record {index} has _silver_batch_id {record_batch_id!r}, expected {batch_id!r}"
                )
            self._validate_tracking_columns(copy, config.tracking_columns, f"incoming record {index}")
            key = self._key_for(copy, config.business_keys, f"incoming record {index}")
            if key in seen:
                raise ValueError(f"Duplicate business key {key!r} in incoming batch")
            seen.add(key)
            normalized.append(copy)
        return normalized

    def _validate_persisted_rows(self, rows: list[dict[str, Any]], config: Scd2Config) -> None:
        for index, row in enumerate(rows):
            context = f"persisted dimension record {index}"
            self._key_for(row, config.business_keys, context)
            self._validate_tracking_columns(row, config.tracking_columns, context)

    @staticmethod
    def _validate_tracking_columns(record: dict[str, Any], columns: list[str], context: str) -> None:
        missing = [column for column in columns if column not in record]
        if missing:
            raise ValueError(f"Missing tracking column(s) {', '.join(repr(column) for column in missing)} in {context}")

    @staticmethod
    def _key_for(record: dict[str, Any], columns: list[str], context: str) -> tuple[Any, ...]:
        values: list[Any] = []
        for column in columns:
            value = record.get(column)
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"Invalid empty business key column {column!r} in {context}")
            try:
                hash(value)
            except TypeError as error:
                raise ValueError(f"Business key column {column!r} in {context} must be scalar") from error
            values.append(value)
        return tuple(values)

    def _read_dimension(self, target_path: Path) -> list[dict[str, Any]]:
        if not target_path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with target_path.open("r", encoding="utf-8") as dimension_file:
            for line_number, line in enumerate(dimension_file, 1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f"Malformed SCD2 JSON at {target_path}:{line_number}: {error.msg}") from error
                if not isinstance(row, dict):
                    raise ValueError(f"SCD2 record at {target_path}:{line_number} is not a JSON object")
                rows.append(row)
        return rows

    def _current_rows(self, rows: list[dict[str, Any]], config: Scd2Config) -> dict[tuple[Any, ...], dict[str, Any]]:
        current: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            key = self._key_for(row, config.business_keys, "persisted dimension")
            if row.get(config.is_current_col) is True:
                if key in current:
                    raise ValueError(f"Persisted dimension has multiple current rows for key {key!r}")
                current[key] = row
        return current

    def _applied_rows(
        self, rows: list[dict[str, Any]], config: Scd2Config
    ) -> dict[tuple[tuple[Any, ...], str], dict[str, Any]]:
        applied: dict[tuple[tuple[Any, ...], str], dict[str, Any]] = {}
        for row in rows:
            row_batch_id = row.get(LINEAGE_SILVER_BATCH_ID)
            if row_batch_id is None:
                continue
            key = self._key_for(row, config.business_keys, "persisted dimension")
            applied_key = (key, str(row_batch_id))
            previous = applied.get(applied_key)
            if previous is not None:
                if not self._same_tracked_values(previous, row, config.tracking_columns):
                    raise ValueError(
                        f"Conflicting persisted history for business key {key!r} and batch {row_batch_id!r}"
                    )
                raise ValueError(
                    f"Duplicate persisted history for business key {key!r} and batch {row_batch_id!r}"
                )
            applied[applied_key] = row
        return applied

    @staticmethod
    def _same_tracked_values(existing: dict[str, Any], incoming: dict[str, Any], columns: list[str]) -> bool:
        return all(existing[column] == incoming[column] for column in columns)

    @staticmethod
    def _write_dimension_atomically(target_path: Path, rows: list[dict[str, Any]]) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        staged = target_path.with_name(f".{target_path.name}.{uuid.uuid4()}.staged")
        try:
            with staged.open("w", encoding="utf-8", newline="\n") as dimension_file:
                for row in rows:
                    dimension_file.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                dimension_file.flush()
                os.fsync(dimension_file.fileno())
            os.replace(staged, target_path)
        except Exception:
            if staged.exists():
                staged.unlink()
            raise
