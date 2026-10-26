"""Component contracts, canonical schema definitions, and metadata specifications.

This module provides the architectural foundation for the Accelerator:
- Strongly-typed data models for datasets, canonical schemas, column mappings,
  data quality rules, and SCD2 configurations.
- Abstract Protocols defining stable boundaries between ingestion, storage,
  transformation, validation, SCD2, batch metadata, and orchestration.
- Standard lineage constants and execution status definitions.
- Configuration loading and semantic validation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable


# =============================================================================
# Lineage & Technical Metadata Field Constants
# =============================================================================

# Bronze Ingestion Lineage
LINEAGE_SOURCE_NAME = "_ingestion_source_name"
LINEAGE_BATCH_ID = "_ingestion_batch_id"
LINEAGE_INGESTION_TIMESTAMP = "_ingestion_timestamp"

# Silver Standardized Lineage
LINEAGE_SILVER_BATCH_ID = "_silver_batch_id"
LINEAGE_SILVER_PROCESSED_AT = "_silver_processed_at"
LINEAGE_RECORD_HASH = "_record_hash"

# SCD Type 2 Technical Attributes
SCD_SURROGATE_KEY = "_scd_key"
SCD_EFFECTIVE_FROM = "_effective_from"
SCD_EFFECTIVE_TO = "_effective_to"
SCD_IS_CURRENT = "_is_current"

# Data Quality & Quarantine Technical Attributes
REJECTION_TIMESTAMP = "_rejected_at"
REJECTION_REASON = "_rejection_reason"
REJECTION_RULE = "_failed_rule"
REJECTION_BATCH_ID = "_rejection_batch_id"
REJECTION_RAW_RECORD = "_raw_record"


# =============================================================================
# Standard Enums
# =============================================================================

class BatchStatus(str, Enum):
    """Lifecycle status values for batch control tracking."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"


class FieldType(str, Enum):
    """Canonical data types supported by the accelerator schema engine."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"


class ExtraColumnsPolicy(str, Enum):
    """Policy for handling source columns not defined in the canonical schema."""
    IGNORE = "ignore"      # Drop extra columns during canonicalization
    FAIL = "fail"          # Reject the record/batch if extra columns appear
    PRESERVE = "preserve"  # Keep extra columns as unmapped raw fields


class MissingColumnsPolicy(str, Enum):
    """Policy for handling target columns absent from the source dataset."""
    NULL = "null"  # Populate with None/null if column is nullable
    FAIL = "fail"  # Reject the record/batch if a required column is missing


class ValidationSeverity(str, Enum):
    """Severity of a data quality rule failure."""
    WARN = "warn"        # Log warning; allow record through to Silver
    REJECT = "reject"    # Divert record to quarantine/rejected store; do not land in Silver
    CRITICAL = "critical"# Abort the entire batch immediately


# =============================================================================
# Configuration & Schema Dataclasses
# =============================================================================

@dataclass(frozen=True)
class ColumnSchema:
    """Definition of a single canonical field."""
    name: str
    data_type: FieldType
    nullable: bool = True
    description: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ColumnSchema:
        if not isinstance(data, dict):
            raise ValueError("Column schema must be a dictionary")
        if "name" not in data or not isinstance(data["name"], str) or not data["name"].strip():
            raise ValueError("Column schema 'name' is required and must be a non-empty string")
        if "data_type" not in data:
            raise ValueError(f"Column schema '{data.get('name')}' is missing 'data_type'")
        try:
            field_type = FieldType(str(data["data_type"]).lower())
        except ValueError as err:
            valid_types = [t.value for t in FieldType]
            raise ValueError(
                f"Unsupported data_type '{data['data_type']}' for column '{data['name']}'. "
                f"Valid types are: {', '.join(valid_types)}"
            ) from err

        return cls(
            name=data["name"].strip(),
            data_type=field_type,
            nullable=bool(data.get("nullable", True)),
            description=str(data.get("description", "")),
        )


@dataclass(frozen=True)
class ColumnMapping:
    """Mapping from a raw source column name to a target canonical column name."""
    source_column: str
    target_column: str
    transform: str | None = None  # e.g., "trim", "upper", "lower"
    default_value: Any = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ColumnMapping:
        if not isinstance(data, dict):
            raise ValueError("Column mapping must be a dictionary")
        if "source_column" not in data or not data["source_column"]:
            raise ValueError("Column mapping requires non-empty 'source_column'")
        if "target_column" not in data or not data["target_column"]:
            raise ValueError("Column mapping requires non-empty 'target_column'")
        return cls(
            source_column=str(data["source_column"]).strip(),
            target_column=str(data["target_column"]).strip(),
            transform=str(data["transform"]).strip().lower() if data.get("transform") else None,
            default_value=data.get("default_value"),
        )


@dataclass(frozen=True)
class QualityRule:
    """A data quality check rule applied before landing in Silver."""
    rule_name: str
    columns: list[str]
    severity: ValidationSeverity = ValidationSeverity.REJECT
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QualityRule:
        if not isinstance(data, dict):
            raise ValueError("Quality rule must be a dictionary")
        if "rule_name" not in data or not data["rule_name"]:
            raise ValueError("Quality rule requires non-empty 'rule_name'")
        cols = data.get("columns", [])
        if not isinstance(cols, list) or not cols:
            raise ValueError(f"Quality rule '{data.get('rule_name')}' requires a non-empty 'columns' list")
        severity_val = str(data.get("severity", "reject")).lower()
        try:
            severity = ValidationSeverity(severity_val)
        except ValueError as err:
            valid_sevs = [s.value for s in ValidationSeverity]
            raise ValueError(
                f"Unknown severity '{severity_val}' in rule '{data['rule_name']}'. "
                f"Valid severities are: {', '.join(valid_sevs)}"
            ) from err

        return cls(
            rule_name=str(data["rule_name"]).strip(),
            columns=[str(c).strip() for c in cols],
            severity=severity,
            params=dict(data.get("params", {})),
        )


@dataclass(frozen=True)
class Scd2Config:
    """Configuration for Slowly Changing Dimension Type 2 processing."""
    enabled: bool = False
    business_keys: list[str] = field(default_factory=list)
    tracking_columns: list[str] = field(default_factory=list)
    effective_from_col: str = SCD_EFFECTIVE_FROM
    effective_to_col: str = SCD_EFFECTIVE_TO
    is_current_col: str = SCD_IS_CURRENT

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> Scd2Config:
        if not data:
            return cls(enabled=False)
        enabled = bool(data.get("enabled", False))
        business_keys = [str(k).strip() for k in data.get("business_keys", [])]
        tracking_cols = [str(c).strip() for c in data.get("tracking_columns", [])]
        if enabled and not business_keys:
            raise ValueError("SCD2 is enabled but 'business_keys' list is empty")
        return cls(
            enabled=enabled,
            business_keys=business_keys,
            tracking_columns=tracking_cols,
            effective_from_col=str(data.get("effective_from_col", SCD_EFFECTIVE_FROM)),
            effective_to_col=str(data.get("effective_to_col", SCD_EFFECTIVE_TO)),
            is_current_col=str(data.get("is_current_col", SCD_IS_CURRENT)),
        )


@dataclass(frozen=True)
class SilverConfig:
    """Configuration for Silver standardization, validation, and SCD2."""
    target_name: str
    silver_path: Path
    rejected_path: Path | None = None
    primary_keys: list[str] = field(default_factory=list)
    extra_columns: ExtraColumnsPolicy = ExtraColumnsPolicy.IGNORE
    missing_columns: MissingColumnsPolicy = MissingColumnsPolicy.NULL
    columns: list[ColumnSchema] = field(default_factory=list)
    mappings: list[ColumnMapping] = field(default_factory=list)
    quality_rules: list[QualityRule] = field(default_factory=list)
    scd2: Scd2Config = field(default_factory=Scd2Config)


@dataclass(frozen=True)
class PipelineConfig:
    """Complete end-to-end configuration for Bronze and Silver processing."""
    source_name: str
    format: str
    input_path: Path
    bronze_path: Path
    metadata_store: Path
    delimiter: str = ","
    encoding: str = "utf-8"
    silver: SilverConfig | None = None


# =============================================================================
# Execution & Result Dataclasses
# =============================================================================

@dataclass(frozen=True)
class IngestionResult:
    """Summary of a Bronze ingestion operation."""
    batch_id: str
    source_name: str
    records_processed: int
    bronze_path: str
    status: BatchStatus = BatchStatus.SUCCESS


@dataclass(frozen=True)
class QualityCheckResult:
    """Result of evaluating a quality rule against a batch of records."""
    rule_name: str
    passed: bool
    failed_record_count: int
    severity: ValidationSeverity
    details: str = ""


@dataclass(frozen=True)
class TransformationResult:
    """Summary of Silver transformation and quarantine processing."""
    batch_id: str
    records_read: int
    records_transformed: int
    records_rejected: int
    silver_path: str
    rejected_path: str | None = None
    status: BatchStatus = BatchStatus.SUCCESS
    quality_checks: list[QualityCheckResult] = field(default_factory=list)


# =============================================================================
# Component Protocols (Component Interfaces)
# =============================================================================

@runtime_checkable
class SourceReaderProtocol(Protocol):
    """Protocol for reading raw source data into record dictionaries."""

    def read(self, config: PipelineConfig) -> Iterable[dict[str, Any]]:
        """Read source records preserving raw representation."""
        ...


@runtime_checkable
class BronzeStoreProtocol(Protocol):
    """Protocol for landing raw records into Bronze storage."""

    def append(self, destination: Path, records: Iterable[dict[str, Any]]) -> int:
        """Append records to the destination store and return count."""
        ...


@runtime_checkable
class QualityValidatorProtocol(Protocol):
    """Protocol for evaluating quality rules and splitting valid/rejected records."""

    def validate(
        self,
        records: list[dict[str, Any]],
        rules: list[QualityRule],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[QualityCheckResult]]:
        """Validate records. Returns (valid_records, rejected_records, check_summaries)."""
        ...


@runtime_checkable
class SilverTransformerProtocol(Protocol):
    """Protocol for transforming Bronze raw records to canonical Silver schema."""

    def transform(
        self,
        records: Iterable[dict[str, Any]],
        silver_config: SilverConfig,
        batch_id: str,
    ) -> list[dict[str, Any]]:
        """Map, cast, and decorate records with Silver lineage."""
        ...


@runtime_checkable
class Scd2ProcessorProtocol(Protocol):
    """Protocol for applying Slowly Changing Dimension Type 2 logic."""

    def apply_scd2(
        self,
        incoming_records: list[dict[str, Any]],
        target_path: Path,
        scd2_config: Scd2Config,
        batch_id: str,
    ) -> int:
        """Merge incoming records into the existing SCD2 dimension."""
        ...


@runtime_checkable
class BatchSourceConfigProtocol(Protocol):
    """Structural protocol representing minimal configuration attributes for batch tracking."""
    source_name: str
    format: str
    input_path: Path
    bronze_path: Path


@runtime_checkable
class BatchTrackerProtocol(Protocol):
    """Protocol for batch control and execution auditing."""

    def start(
        self,
        config: BatchSourceConfigProtocol,
        ingested_at: str,
        submission_key: str | None = None,
        submission_fingerprint: str | None = None,
        lease_token: str | None = None,
    ) -> str:
        """Start a batch record and return unique batch_id."""
        ...

    def finish(
        self,
        batch_id: str,
        status: BatchStatus | str,
        records_processed: int,
        finished_at: str,
        records_persisted: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """Finalize batch status and record processing metrics."""
        ...


# =============================================================================
# Configuration Loading & Validation
# =============================================================================

REQUIRED_BASE_FIELDS = {"source_name", "format", "input_path", "bronze_path", "metadata_store"}
SUPPORTED_FORMATS = {"csv", "json"}


def load_pipeline_config(config_path: str | Path) -> PipelineConfig:
    """Load and validate a pipeline configuration file.

    Supports both legacy Bronze-only configurations and full Bronze+Silver
    specifications. Resolves relative paths against the config file directory.
    """
    config_file = Path(config_path).resolve()
    try:
        payload = json.loads(config_file.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"Configuration file does not exist: {config_file}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"Configuration is not valid JSON: {error.msg}") from error

    if not isinstance(payload, dict):
        raise ValueError("Configuration must be a JSON object")

    missing = REQUIRED_BASE_FIELDS - payload.keys()
    if missing:
        raise ValueError(f"Configuration is missing required field(s): {', '.join(sorted(missing))}")

    if payload["format"] not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format {payload['format']!r}; expected csv or json")

    for field_name in REQUIRED_BASE_FIELDS:
        if not isinstance(payload[field_name], str) or not payload[field_name].strip():
            raise ValueError(f"Configuration field '{field_name}' must be a non-empty string")

    def resolve(val: str) -> Path:
        p = Path(val)
        return p if p.is_absolute() else (config_file.parent / p).resolve()

    input_path = resolve(payload["input_path"])
    if not input_path.exists():
        raise ValueError(f"Input file does not exist: {input_path}")

    delimiter = payload.get("delimiter", ",")
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError("delimiter must be one character")

    encoding = payload.get("encoding", "utf-8")
    if not isinstance(encoding, str) or not encoding:
        raise ValueError("encoding must be a non-empty string")

    silver_config: SilverConfig | None = None
    if "silver" in payload and payload["silver"] is not None:
        silver_data = payload["silver"]
        if not isinstance(silver_data, dict):
            raise ValueError("'silver' section must be a JSON object")

        if "target_name" not in silver_data or not silver_data["target_name"]:
            raise ValueError("Silver configuration requires 'target_name'")
        if "silver_path" not in silver_data or not silver_data["silver_path"]:
            raise ValueError("Silver configuration requires 'silver_path'")

        target_name = str(silver_data["target_name"]).strip()
        silver_path = resolve(silver_data["silver_path"])
        rejected_path = resolve(silver_data["rejected_path"]) if silver_data.get("rejected_path") else None

        primary_keys = [str(k).strip() for k in silver_data.get("primary_keys", [])]

        extra_cols_str = str(silver_data.get("extra_columns", "ignore")).lower()
        try:
            extra_columns = ExtraColumnsPolicy(extra_cols_str)
        except ValueError as err:
            valid_policies = [p.value for p in ExtraColumnsPolicy]
            raise ValueError(
                f"Unknown extra_columns policy '{extra_cols_str}'. Valid: {', '.join(valid_policies)}"
            ) from err

        miss_cols_str = str(silver_data.get("missing_columns", "null")).lower()
        try:
            missing_columns = MissingColumnsPolicy(miss_cols_str)
        except ValueError as err:
            valid_policies = [p.value for p in MissingColumnsPolicy]
            raise ValueError(
                f"Unknown missing_columns policy '{miss_cols_str}'. Valid: {', '.join(valid_policies)}"
            ) from err

        columns = [ColumnSchema.from_dict(c) for c in silver_data.get("columns", [])]
        mappings = [ColumnMapping.from_dict(m) for m in silver_data.get("mappings", [])]
        quality_rules = [QualityRule.from_dict(r) for r in silver_data.get("quality_rules", [])]
        scd2 = Scd2Config.from_dict(silver_data.get("scd2"))

        # Semantic verification: mapping target columns must exist in columns definition if columns are specified
        defined_col_names = {col.name for col in columns}
        if defined_col_names:
            for m in mappings:
                if m.target_column not in defined_col_names:
                    raise ValueError(
                        f"ColumnMapping target '{m.target_column}' is not defined in silver columns: {defined_col_names}"
                    )
            # Verify primary keys exist in columns
            for pk in primary_keys:
                if pk not in defined_col_names:
                    raise ValueError(f"primary_key '{pk}' is not defined in silver columns: {defined_col_names}")

            # Verify SCD2 business keys exist in columns
            if scd2.enabled:
                for bk in scd2.business_keys:
                    if bk not in defined_col_names:
                        raise ValueError(f"SCD2 business_key '{bk}' is not defined in silver columns: {defined_col_names}")

        silver_config = SilverConfig(
            target_name=target_name,
            silver_path=silver_path,
            rejected_path=rejected_path,
            primary_keys=primary_keys,
            extra_columns=extra_columns,
            missing_columns=missing_columns,
            columns=columns,
            mappings=mappings,
            quality_rules=quality_rules,
            scd2=scd2,
        )

    return PipelineConfig(
        source_name=payload["source_name"],
        format=payload["format"],
        input_path=input_path,
        bronze_path=resolve(payload["bronze_path"]),
        metadata_store=resolve(payload["metadata_store"]),
        delimiter=delimiter,
        encoding=encoding,
        silver=silver_config,
    )
