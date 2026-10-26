"""Tests for component contracts, schema validation, and pipeline configurations."""

import json
from pathlib import Path

import pytest

from accelerator.contracts import (
    LINEAGE_BATCH_ID,
    LINEAGE_INGESTION_TIMESTAMP,
    LINEAGE_SOURCE_NAME,
    SCD_EFFECTIVE_FROM,
    SCD_EFFECTIVE_TO,
    SCD_IS_CURRENT,
    SCD_SURROGATE_KEY,
    BatchStatus,
    BatchTrackerProtocol,
    BronzeStoreProtocol,
    ColumnMapping,
    ColumnSchema,
    ExtraColumnsPolicy,
    FieldType,
    MissingColumnsPolicy,
    QualityRule,
    QualityValidatorProtocol,
    Scd2Config,
    Scd2ProcessorProtocol,
    SilverConfig,
    SilverTransformerProtocol,
    SourceReaderProtocol,
    ValidationSeverity,
    load_pipeline_config,
)


def test_load_legacy_bronze_configs_are_backwards_compatible() -> None:
    """Ensure existing Week 1 Bronze configs load seamlessly without Silver section."""
    cfg1 = load_pipeline_config("config/source_customers.json")
    assert cfg1.source_name == "assessment_customers"
    assert cfg1.format == "csv"
    assert cfg1.silver is None

    cfg2 = load_pipeline_config("config/source_customers_alternate.json")
    assert cfg2.source_name == "assessment_customers_alternate"
    assert cfg2.silver is None


def test_load_full_pipeline_config() -> None:
    """Ensure full pipeline config parses all canonical, quality, and SCD2 definitions."""
    cfg = load_pipeline_config("config/dataset_customers_pipeline.json")
    assert cfg.source_name == "assessment_customers"
    assert cfg.silver is not None

    silver = cfg.silver
    assert silver.target_name == "dim_customers"
    assert silver.extra_columns == ExtraColumnsPolicy.IGNORE
    assert silver.missing_columns == MissingColumnsPolicy.NULL
    assert len(silver.columns) == 3
    assert len(silver.mappings) == 3
    assert len(silver.quality_rules) == 3
    assert silver.scd2.enabled is True
    assert silver.scd2.business_keys == ["customer_id"]
    assert silver.scd2.tracking_columns == ["customer_name", "postal_code"]


def test_config_validation_rejects_missing_required_base_fields(tmp_path: Path) -> None:
    bad_config = tmp_path / "missing_fields.json"
    bad_config.write_text(json.dumps({"source_name": "test"}), encoding="utf-8")

    with pytest.raises(ValueError, match="missing required field"):
        load_pipeline_config(bad_config)


def test_config_validation_rejects_unsupported_data_type(tmp_path: Path) -> None:
    (tmp_path / "in.csv").write_text("id\n1\n", encoding="utf-8")
    cfg_data = {
        "source_name": "test",
        "format": "csv",
        "input_path": "in.csv",
        "bronze_path": "bronze.jsonl",
        "metadata_store": "meta.db",
        "silver": {
            "target_name": "test_silver",
            "silver_path": "silver.jsonl",
            "columns": [{"name": "id", "data_type": "invalid_type"}],
        },
    }
    cfg_file = tmp_path / "bad_type.json"
    cfg_file.write_text(json.dumps(cfg_data), encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported data_type 'invalid_type'"):
        load_pipeline_config(cfg_file)


def test_config_validation_rejects_mapping_to_undefined_column(tmp_path: Path) -> None:
    (tmp_path / "in.csv").write_text("raw_id\n1\n", encoding="utf-8")
    cfg_data = {
        "source_name": "test",
        "format": "csv",
        "input_path": "in.csv",
        "bronze_path": "bronze.jsonl",
        "metadata_store": "meta.db",
        "silver": {
            "target_name": "test_silver",
            "silver_path": "silver.jsonl",
            "columns": [{"name": "customer_id", "data_type": "string"}],
            "mappings": [{"source_column": "raw_id", "target_column": "non_existent_column"}],
        },
    }
    cfg_file = tmp_path / "bad_mapping.json"
    cfg_file.write_text(json.dumps(cfg_data), encoding="utf-8")

    with pytest.raises(ValueError, match="not defined in silver columns"):
        load_pipeline_config(cfg_file)


def test_config_validation_rejects_scd2_without_business_keys(tmp_path: Path) -> None:
    (tmp_path / "in.csv").write_text("id\n1\n", encoding="utf-8")
    cfg_data = {
        "source_name": "test",
        "format": "csv",
        "input_path": "in.csv",
        "bronze_path": "bronze.jsonl",
        "metadata_store": "meta.db",
        "silver": {
            "target_name": "test_silver",
            "silver_path": "silver.jsonl",
            "columns": [{"name": "id", "data_type": "string"}],
            "scd2": {"enabled": True, "business_keys": []},
        },
    }
    cfg_file = tmp_path / "bad_scd2.json"
    cfg_file.write_text(json.dumps(cfg_data), encoding="utf-8")

    with pytest.raises(ValueError, match="SCD2 is enabled but 'business_keys' list is empty"):
        load_pipeline_config(cfg_file)


def test_config_validation_rejects_scd2_business_key_missing_from_columns(tmp_path: Path) -> None:
    (tmp_path / "in.csv").write_text("id\n1\n", encoding="utf-8")
    cfg_data = {
        "source_name": "test",
        "format": "csv",
        "input_path": "in.csv",
        "bronze_path": "bronze.jsonl",
        "metadata_store": "meta.db",
        "silver": {
            "target_name": "test_silver",
            "silver_path": "silver.jsonl",
            "columns": [{"name": "id", "data_type": "string"}],
            "scd2": {"enabled": True, "business_keys": ["ghost_key"]},
        },
    }
    cfg_file = tmp_path / "bad_scd2_key.json"
    cfg_file.write_text(json.dumps(cfg_data), encoding="utf-8")

    with pytest.raises(ValueError, match="SCD2 business_key 'ghost_key' is not defined in silver columns"):
        load_pipeline_config(cfg_file)


def test_runtime_protocols_are_checkable() -> None:
    """Verify that runtime_checkable Protocols can validate concrete classes."""
    class DummyReader:
        def read(self, config):
            return []

    class DummyStore:
        def append(self, destination, records):
            return 0

    assert isinstance(DummyReader(), SourceReaderProtocol)
    assert isinstance(DummyStore(), BronzeStoreProtocol)
    assert not isinstance(DummyReader(), BronzeStoreProtocol)


def test_lineage_constants_values() -> None:
    """Ensure standard metadata column names are immutable across framework releases."""
    assert LINEAGE_SOURCE_NAME == "_ingestion_source_name"
    assert LINEAGE_BATCH_ID == "_ingestion_batch_id"
    assert LINEAGE_INGESTION_TIMESTAMP == "_ingestion_timestamp"
    assert SCD_SURROGATE_KEY == "_scd_key"
    assert SCD_EFFECTIVE_FROM == "_effective_from"
    assert SCD_EFFECTIVE_TO == "_effective_to"
    assert SCD_IS_CURRENT == "_is_current"

def test_concrete_protocol_conformance_and_signatures(tmp_path: Path) -> None:
    """Verify concrete SourceReader and BatchTracker adhere to Protocols and signatures."""
    from accelerator.bronze_ingestion import BatchTracker, SourceReader

    reader = SourceReader()
    assert isinstance(reader, SourceReaderProtocol)

    doc_test_db = tmp_path / "contract_check.db"
    tracker = BatchTracker(doc_test_db)
    assert isinstance(tracker, BatchTrackerProtocol)

    cfg = load_pipeline_config("config/source_customers.json")
    records = list(reader.read(cfg))
    assert len(records) == 2
    assert records[0]["customer_id"] == "C001"

    # Verify signature compatibility through BatchTrackerProtocol type annotation
    tracker_proto: BatchTrackerProtocol = tracker
    batch_id = tracker_proto.start(
        config=cfg,
        ingested_at="2026-10-26T12:00:00Z",
        submission_key="sub_test_001",
    )
    assert batch_id
    tracker_proto.finish(
        batch_id=batch_id,
        status=BatchStatus.SUCCESS,
        records_processed=2,
        finished_at="2026-10-26T12:01:00Z",
        records_persisted=2,
    )

def test_batch_tracker_protocol_status_and_config_types(tmp_path: Path) -> None:
    """Verify BatchTrackerProtocol start and finish work with SourceConfig, PipelineConfig, BatchStatus and str."""
    from accelerator.bronze_ingestion import BatchTracker, load_config

    db_path = tmp_path / "strong_types_check.db"
    tracker = BatchTracker(db_path)
    tracker_proto: BatchTrackerProtocol = tracker

    # Test 1: Start with SourceConfig, Finish with raw str status
    src_cfg = load_config("config/source_customers.json")
    batch_1 = tracker_proto.start(src_cfg, '2026-10-26T10:00:00Z')
    assert batch_1
    tracker_proto.finish(batch_1, "SUCCESS", 2, '2026-10-26T10:01:00Z', 2)

    # Test 2: Start with PipelineConfig, Finish with BatchStatus enum
    pipe_cfg = load_pipeline_config("config/dataset_customers_pipeline.json")
    batch_2 = tracker_proto.start(pipe_cfg, '2026-10-26T11:00:00Z', submission_key='sub_002')
    assert batch_2
    tracker_proto.finish(batch_2, BatchStatus.SUCCESS, 2, '2026-10-26T11:01:00Z', 2)

def test_batch_tracker_protocol_type_hints_resolution() -> None:
    """Regression test: verify get_type_hints resolves cleanly without NameError."""
    import typing
    from accelerator.contracts import BatchTrackerProtocol, BatchSourceConfigProtocol

    hints_start = typing.get_type_hints(BatchTrackerProtocol.start)
    assert "config" in hints_start
    assert hints_start["config"] is BatchSourceConfigProtocol

    hints_finish = typing.get_type_hints(BatchTrackerProtocol.finish)
    assert "status" in hints_finish
