"""Silver transformation module for conformed canonical schema processing.

Implements the SilverTransformer component responsible for:
- Mapping raw source columns to canonical target columns.
- Applying configured string transformations (trim, upper, lower).
- Strict type casting and nullability enforcement according to FieldType.
- Enforcing missing and extra column policies (MissingColumnsPolicy, ExtraColumnsPolicy).
- Enriching conformed records with Silver lineage (_silver_batch_id, _silver_processed_at).
- Calculating deterministic SHA-256 record hashes (_record_hash).
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

from accelerator.contracts import (
    LINEAGE_RECORD_HASH,
    LINEAGE_SILVER_BATCH_ID,
    LINEAGE_SILVER_PROCESSED_AT,
    ColumnMapping,
    ColumnSchema,
    ExtraColumnsPolicy,
    FieldType,
    MissingColumnsPolicy,
    SilverConfig,
    SilverTransformerProtocol,
)

SUPPORTED_TRANSFORMS = {"trim", "upper", "lower"}


def compute_record_hash(canonical_fields: dict[str, Any]) -> str:
    """Generate a deterministic SHA-256 hash for canonical business fields.

    Excludes technical lineage, timestamps, and batch identifiers to ensure
    identical business records produce the exact same hash across repeated runs.
    """
    serialized = json.dumps(
        canonical_fields,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        ensure_ascii=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class SilverTransformer:
    """Transforms raw/Bronze records into conformed Silver records."""

    def transform(
        self,
        records: Iterable[dict[str, Any]],
        silver_config: SilverConfig,
        batch_id: str,
    ) -> list[dict[str, Any]]:
        """Map, cast, and decorate records with Silver lineage.

        Parameters
        ----------
        records:
            Iterable of raw input dictionaries (typically loaded from Bronze JSONL).
        silver_config:
            Silver configuration defining target schema, mappings, policies, and types.
        batch_id:
            Unique identifier of the Silver execution batch.

        Returns
        -------
        list[dict[str, Any]]:
            List of transformed, conformed dictionaries matching the canonical schema.
        """
        # Validate transformations in mappings up front
        for mapping in silver_config.mappings:
            if mapping.transform and mapping.transform.lower() not in SUPPORTED_TRANSFORMS:
                valid = ", ".join(sorted(SUPPORTED_TRANSFORMS))
                raise ValueError(
                    f"Unsupported transform '{mapping.transform}' for column mapping "
                    f"'{mapping.source_column}' -> '{mapping.target_column}'. Supported: {valid}"
                )

        target_to_mapping: dict[str, ColumnMapping] = {
            m.target_column: m for m in silver_config.mappings
        }

        # Determine mapped source columns for extra column detection
        if silver_config.mappings:
            mapped_source_cols = {m.source_column for m in silver_config.mappings}
        else:
            mapped_source_cols = {col.name for col in silver_config.columns}

        # Deterministic batch timestamp (UTC ISO-8601)
        batch_timestamp = datetime.now(timezone.utc).isoformat()

        transformed_records: list[dict[str, Any]] = []

        for idx, raw_record in enumerate(records):
            if not isinstance(raw_record, dict):
                raise ValueError(
                    f"Record {idx}: Expected dictionary record, got {type(raw_record).__name__}"
                )

            # Check for extra columns in source record
            # Ingestion technical lineage columns starting with '_' are excluded from business extra check
            extra_cols = [
                k
                for k in raw_record
                if k not in mapped_source_cols and not k.startswith("_")
            ]

            if extra_cols and silver_config.extra_columns == ExtraColumnsPolicy.FAIL:
                raise ValueError(
                    f"Record {idx}: Unexpected extra column(s) found: {', '.join(sorted(extra_cols))}"
                )

            transformed: dict[str, Any] = {}
            canonical_for_hash: dict[str, Any] = {}

            # Process each target column defined in canonical schema
            for col in silver_config.columns:
                mapping = target_to_mapping.get(col.name)
                source_col = mapping.source_column if mapping else col.name

                if source_col in raw_record:
                    val = raw_record[source_col]
                else:
                    if mapping and mapping.default_value is not None:
                        val = mapping.default_value
                    elif silver_config.missing_columns == MissingColumnsPolicy.FAIL:
                        raise ValueError(
                            f"Record {idx}: Missing required source column '{source_col}' "
                            f"for target column '{col.name}'"
                        )
                    else:
                        val = None

                # Apply configured string transform if applicable
                if mapping and mapping.transform and val is not None and isinstance(val, str):
                    tf = mapping.transform.lower()
                    if tf == "trim":
                        val = val.strip()
                    elif tf == "upper":
                        val = val.upper()
                    elif tf == "lower":
                        val = val.lower()

                # Cast value to canonical type and enforce nullability
                cast_val = self._cast_value(val, col, idx)
                transformed[col.name] = cast_val
                canonical_for_hash[col.name] = cast_val

            # Handle preserve extra columns policy safely without overwriting canonical fields
            if extra_cols and silver_config.extra_columns == ExtraColumnsPolicy.PRESERVE:
                transformed["_unmapped_attributes"] = {k: raw_record[k] for k in sorted(extra_cols)}

            # Populate Silver lineage fields
            transformed[LINEAGE_SILVER_BATCH_ID] = batch_id
            transformed[LINEAGE_SILVER_PROCESSED_AT] = batch_timestamp
            transformed[LINEAGE_RECORD_HASH] = compute_record_hash(canonical_for_hash)

            transformed_records.append(transformed)

        return transformed_records

    def _cast_value(self, val: Any, col: ColumnSchema, idx: int) -> Any:
        """Cast raw value to declared FieldType with nullability and precision checks."""
        # Handle None
        if val is None:
            if not col.nullable:
                raise ValueError(
                    f"Record {idx}, column '{col.name}': non-nullable column received None"
                )
            return None

        # Handle empty string
        if isinstance(val, str) and val == "":
            if col.data_type == FieldType.STRING:
                if col.nullable:
                    return None
                raise ValueError(
                    f"Record {idx}, column '{col.name}': non-nullable string cannot be empty"
                )
            # For non-string types, empty string indicates null/absent value
            if not col.nullable:
                raise ValueError(
                    f"Record {idx}, column '{col.name}': non-nullable {col.data_type.value} "
                    f"cannot be populated from empty string"
                )
            return None

        # Type casting by FieldType
        if col.data_type == FieldType.STRING:
            return str(val)

        if col.data_type == FieldType.INTEGER:
            if isinstance(val, bool):
                raise ValueError(
                    f"Record {idx}, column '{col.name}': boolean value '{val}' cannot be cast to integer"
                )
            if isinstance(val, float):
                if not val.is_integer():
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': non-integral float '{val}' "
                        f"cannot be cast to integer without silent truncation"
                    )
                return int(val)
            if isinstance(val, Decimal):
                if val % 1 != 0:
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': Decimal '{val}' has non-zero fractional part"
                    )
                return int(val)
            if isinstance(val, int):
                return val
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    if col.nullable:
                        return None
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': non-nullable integer received empty string"
                    )
                if "." in s:
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': string '{val}' with decimal point "
                        f"cannot be cast to integer"
                    )
                try:
                    return int(s)
                except ValueError as err:
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': cannot cast string '{val}' to integer"
                    ) from err
            raise ValueError(
                f"Record {idx}, column '{col.name}': unsupported type {type(val).__name__} for integer"
            )

        if col.data_type == FieldType.FLOAT:
            if isinstance(val, bool):
                raise ValueError(
                    f"Record {idx}, column '{col.name}': boolean value '{val}' cannot be cast to float"
                )
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    if col.nullable:
                        return None
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': non-nullable float received empty string"
                    )
            try:
                return float(val)
            except (ValueError, TypeError) as err:
                raise ValueError(
                    f"Record {idx}, column '{col.name}': cannot cast '{val}' to float"
                ) from err

        if col.data_type == FieldType.DECIMAL:
            if isinstance(val, bool):
                raise ValueError(
                    f"Record {idx}, column '{col.name}': boolean value '{val}' cannot be cast to decimal"
                )
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    if col.nullable:
                        return None
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': non-nullable decimal received empty string"
                    )
                try:
                    return Decimal(s)
                except Exception as err:
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': cannot cast string '{val}' to decimal"
                    ) from err
            if isinstance(val, Decimal):
                return val
            try:
                return Decimal(str(val))
            except Exception as err:
                raise ValueError(
                    f"Record {idx}, column '{col.name}': cannot cast '{val}' to decimal"
                ) from err

        if col.data_type == FieldType.BOOLEAN:
            if isinstance(val, bool):
                return val
            if isinstance(val, (int, float)):
                if val == 1:
                    return True
                if val == 0:
                    return False
                raise ValueError(
                    f"Record {idx}, column '{col.name}': numeric '{val}' cannot be cast to boolean "
                    f"(expected 0 or 1)"
                )
            if isinstance(val, str):
                s = val.strip().lower()
                if not s:
                    if col.nullable:
                        return None
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': non-nullable boolean received empty string"
                    )
                if s in ("true", "1", "t", "yes"):
                    return True
                if s in ("false", "0", "f", "no"):
                    return False
                raise ValueError(
                    f"Record {idx}, column '{col.name}': cannot cast string '{val}' to boolean "
                    f"(expected true/false, 1/0, t/f, yes/no)"
                )
            raise ValueError(
                f"Record {idx}, column '{col.name}': unsupported type {type(val).__name__} for boolean"
            )

        if col.data_type == FieldType.DATE:
            if isinstance(val, datetime):
                return val.date().isoformat()
            if isinstance(val, date):
                return val.isoformat()
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    if col.nullable:
                        return None
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': non-nullable date received empty string"
                    )
                try:
                    if len(s) == 10 and s[4] == "-" and s[7] == "-":
                        return date.fromisoformat(s).isoformat()
                    return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
                except Exception as err:
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': cannot cast '{val}' to date "
                        f"(expected YYYY-MM-DD)"
                    ) from err
            raise ValueError(
                f"Record {idx}, column '{col.name}': unsupported type {type(val).__name__} for date"
            )

        if col.data_type == FieldType.TIMESTAMP:
            if isinstance(val, datetime):
                dt = val
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt.isoformat()
            if isinstance(val, date):
                dt = datetime(val.year, val.month, val.day, tzinfo=timezone.utc)
                return dt.isoformat()
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    if col.nullable:
                        return None
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': non-nullable timestamp received empty string"
                    )
                try:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                    return dt.isoformat()
                except Exception as err:
                    raise ValueError(
                        f"Record {idx}, column '{col.name}': cannot cast '{val}' to timestamp "
                        f"(expected ISO-8601 format)"
                    ) from err
            raise ValueError(
                f"Record {idx}, column '{col.name}': unsupported type {type(val).__name__} for timestamp"
            )

        raise ValueError(
            f"Record {idx}, column '{col.name}': unhandled field type '{col.data_type}'"
        )
