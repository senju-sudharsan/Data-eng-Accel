"""Metadata-Driven Bronze-Silver Data Engineering Accelerator."""

from accelerator.config_loader import (
    CentralConfig,
    GlobalDefaults,
    PathDefaults,
    load_central_config,
)
from accelerator.bronze_ingestion import (
    BatchTracker,
    LocalBronzeStore,
    SourceConfig,
    ingest,
    load_config,
    read_source,
    SourceReader,
)
from accelerator.contracts import (
    LINEAGE_BATCH_ID,
    LINEAGE_INGESTION_TIMESTAMP,
    LINEAGE_RECORD_HASH,
    LINEAGE_SILVER_BATCH_ID,
    LINEAGE_SILVER_PROCESSED_AT,
    LINEAGE_SOURCE_NAME,
    REJECTION_BATCH_ID,
    REJECTION_RAW_RECORD,
    REJECTION_REASON,
    REJECTION_RULE,
    REJECTION_TIMESTAMP,
    SCD_EFFECTIVE_FROM,
    SCD_EFFECTIVE_TO,
    SCD_IS_CURRENT,
    SCD_SURROGATE_KEY,
    BatchStatus,
    BatchAuditSummary,
    ReconciliationStatus,
    BatchSourceConfigProtocol,
    BatchTrackerProtocol,
    BronzeStoreProtocol,
    ColumnMapping,
    ColumnSchema,
    ExtraColumnsPolicy,
    FieldType,
    IngestionResult,
    MissingColumnsPolicy,
    PipelineConfig,
    QualityCheckResult,
    QualityRule,
    QualityValidatorProtocol,
    Scd2Config,
    Scd2ProcessorProtocol,
    SilverConfig,
    SilverTransformerProtocol,
    SourceReaderProtocol,
    TransformationResult,
    ValidationSeverity,
    load_pipeline_config,
)
from accelerator.pipeline_runner import (
    run_pipeline,
)
from accelerator.quality_validator import (
    QualityValidator,
)
from accelerator.scd2_processor import (
    Scd2Processor,
)
from accelerator.silver_transformations import (
    SilverTransformer,
    compute_record_hash,
)

__all__ = [
    # Central Configuration Loader
    "CentralConfig",
    "GlobalDefaults",
    "PathDefaults",
    "load_central_config",
    # Pipeline Runner
    "run_pipeline",
    # Bronze Ingestion
    "SourceConfig",
    "load_config",
    "read_source",
    "SourceReader",
    "BatchTracker",
    "LocalBronzeStore",
    "ingest",
    # Contracts & Schema Models
    "PipelineConfig",
    "SilverConfig",
    "ColumnSchema",
    "ColumnMapping",
    "QualityRule",
    "Scd2Config",
    "load_pipeline_config",
    # Enums
    "BatchStatus",
    "ReconciliationStatus",
    "FieldType",
    "ExtraColumnsPolicy",
    "MissingColumnsPolicy",
    "ValidationSeverity",
    # Results & Summaries
    "IngestionResult",
    "TransformationResult",
    "QualityCheckResult",
    "BatchAuditSummary",
    # Protocols
    "SourceReaderProtocol",
    "BronzeStoreProtocol",
    "SilverTransformerProtocol",
    "QualityValidatorProtocol",
    "Scd2ProcessorProtocol",
    "BatchSourceConfigProtocol",
    "BatchTrackerProtocol",
    # Quality Validation
    "QualityValidator",
    # Silver Transformation
    "SilverTransformer",
    "compute_record_hash",
    # SCD2 Processor
    "Scd2Processor",
    # Lineage Constants
    "LINEAGE_SOURCE_NAME",
    "LINEAGE_BATCH_ID",
    "LINEAGE_INGESTION_TIMESTAMP",
    "LINEAGE_SILVER_BATCH_ID",
    "LINEAGE_SILVER_PROCESSED_AT",
    "LINEAGE_RECORD_HASH",
    "SCD_SURROGATE_KEY",
    "SCD_EFFECTIVE_FROM",
    "SCD_EFFECTIVE_TO",
    "SCD_IS_CURRENT",
    "REJECTION_TIMESTAMP",
    "REJECTION_REASON",
    "REJECTION_RULE",
    "REJECTION_BATCH_ID",
    "REJECTION_RAW_RECORD",
]
