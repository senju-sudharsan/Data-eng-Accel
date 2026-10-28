"""Unit and integration tests for CentralConfig loader and config.yml entry point."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from accelerator.bronze_ingestion import SourceConfig, ingest
from accelerator.config_loader import CentralConfig, load_central_config
from accelerator.contracts import (
    ExtraColumnsPolicy,
    FieldType,
    MissingColumnsPolicy,
    PipelineConfig,
    load_pipeline_config,
)
from accelerator.pipeline_runner import run_pipeline

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_load_root_config_yml_success() -> None:
    """Ensure the repository's root config.yml loads and validates all declared sources/pipelines."""
    root_cfg_file = PROJECT_ROOT / "config.yml"
    assert root_cfg_file.exists()

    cfg = load_central_config(root_cfg_file)
    assert cfg.version == 1
    assert "assessment_customers" in cfg.sources
    assert "olist_products" in cfg.sources
    assert "product_category_translation" in cfg.sources
    assert "customers" in cfg.pipelines
    assert "olist_products" in cfg.pipelines

    # Check path defaults
    assert cfg.defaults.paths.raw == (PROJECT_ROOT / "data" / "fixtures").resolve()
    assert cfg.defaults.paths.bronze == (PROJECT_ROOT / "data" / "bronze").resolve()
    assert cfg.defaults.paths.silver == (PROJECT_ROOT / "data" / "silver").resolve()

    # Check source-specific encoding override (BOM preservation)
    trans_src = cfg.get_source("product_category_translation")
    assert trans_src.encoding == "utf-8-sig"

    # Check pipeline resolution
    pipe = cfg.get_pipeline("customers")
    assert isinstance(pipe, PipelineConfig)
    assert pipe.source_name == "assessment_customers"
    assert pipe.silver is not None
    assert pipe.silver.target_name == "dim_customers"
    assert len(pipe.silver.columns) == 3
    assert pipe.silver.scd2.enabled is True


def test_reject_missing_file() -> None:
    """Ensure loading a nonexistent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Configuration file not found"):
        load_central_config("nonexistent_config.yml")


def test_reject_malformed_yaml(tmp_path: Path) -> None:
    """Ensure invalid YAML syntax raises ValueError with helpful message."""
    bad_yaml = tmp_path / "bad.yml"
    bad_yaml.write_text("version: 1\nsources: [unclosed", encoding="utf-8")

    with pytest.raises(ValueError, match="Malformed YAML"):
        load_central_config(bad_yaml)


def test_reject_non_mapping_root(tmp_path: Path) -> None:
    """Ensure YAML containing a list at root is rejected."""
    bad_yaml = tmp_path / "bad.yml"
    bad_yaml.write_text("- item1\n- item2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a mapping"):
        load_central_config(bad_yaml)


def test_reject_missing_version(tmp_path: Path) -> None:
    """Ensure missing version raises ValueError."""
    bad_yaml = tmp_path / "bad.yml"
    bad_yaml.write_text("defaults: {}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required 'version' key"):
        load_central_config(bad_yaml)


def test_reject_unsupported_version(tmp_path: Path) -> None:
    """Ensure unsupported version raises ValueError."""
    bad_yaml = tmp_path / "bad.yml"
    bad_yaml.write_text("version: 999\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported configuration version: 999"):
        load_central_config(bad_yaml)


def test_reject_unknown_root_key(tmp_path: Path) -> None:
    """Ensure unrecognized root keys (typos) fail validation."""
    bad_yaml = tmp_path / "bad.yml"
    bad_yaml.write_text("version: 1\nextra_bogus_key: true\n", encoding="utf-8")


    with pytest.raises(ValueError, match="Unknown or unrecognized key"):
        load_central_config(bad_yaml)


def test_reject_unknown_key_in_sources_or_pipelines(tmp_path: Path) -> None:
    """Ensure typos in source or pipeline definitions fail immediately."""
    bad_yaml = tmp_path / "bad.yml"
    data = {
        "version": 1,
        "sources": {
            "s1": {
                "input_path": "in.csv",
                "typo_format": "crv",
            }
        }
    }
    bad_yaml.write_text(yaml.safe_dump(data), encoding="utf-8")

    with pytest.raises(ValueError, match="Unknown or unrecognized key"):
        load_central_config(bad_yaml)


def test_defaults_and_overrides_handling(tmp_path: Path) -> None:
    """Ensure global defaults apply when omitted, and explicit overrides take precedence."""
    yaml_file = tmp_path / "test_config.yml"
    data = {
        "version": 1,
        "defaults": {
            "paths": {
                "bronze": "custom_bronze_dir",
            },
            "delimiter": ";",
            "encoding": "latin-1",
        },
        "sources": {
            "defaulted_source": {
                "input_path": "in1.crv",
            },
            "overridden_source": {
                "input_path": "in2.csv",
                "bronze_path": "explicit_bronze/out.jsonl",
                "delimiter": "|",
                "encoding": "utf-8",
            },
        },
    }
    yaml_file.write_text(yaml.safe_dump(data), encoding="utf-8")

    cfg = load_central_config(yaml_file)

    s1 = cfg.get_source("defaulted_source")
    assert s1.delimiter == ";"
    assert s1.encoding == "latin-1"
    assert s1.bronze_path == (tmp_path / "custom_bronze_dir" / "defaulted_source_raw.jsonl").resolve()

    s2 = cfg.get_source("overridden_source")
    assert s2.delimiter == "p|"[1]
    assert s2.encoding == "utf-8"
    assert s2.bronze_path == (tmp_path / "explicit_bronze" / "out.jsonl").resolve()


def test_path_resolution_relative_to_config_dir(tmp_path: Path) -> None:
    sub_dir = tmp_path / "nested" / "configs"
    sub_dir.mkdir(parents=True)
    cfg_file = sub_dir / "app_config.yml"

    data = {
        "version": 1,
        "sources": {
            "s1": {
                "input_path": "../../data/raw/data.csv",
                "bronze_path": "../../data/bronze/s1.jsonl",
            }
        }
    }
    cfg_file.write_text(yaml.safe_dump(data), encoding="utf-8")

    cfg = load_central_config(cfg_file)
    s1 = cfg.get_source("s1")
    assert s1.input_path == (tmp_path / "data" / "raw" / "data.csv").resolve()
    assert s1.bronze_path == (tmp_path / "data" / "bronze" / "s1.jsonl").resolve()


def test_select_unknown_source_or_pipeline_raises_key_error(tmp_path: Path) -> None:
    cfg_file = tmp_path / "cfg.yml"
    cfg_file.write_text(
        "version: 1\nsources:\n  src1:\n    input_path: in.csv\npipelines:\n  p1:\n    source: src1\n",
        encoding="utf-8",
    )
    cfg = load_central_config(cfg_file)

    with pytest.raises(KeyError, match="Available sources: src1"):
        cfg.get_source("unknown_src")

    with pytest.raises(KeyError, match="Available pipelines: p1"):
        cfg.get_pipeline("unknown_pipe")


def test_pipeline_references_undeclared_source_fails(tmp_path: Path) -> None:
    cfg_file = tmp_path / "cfg.yml"
    cfg_file.write_text(
        "version: 1\nsources:\n  src1:\n    input_path: in.csv\npipelines:\n  p1:\n    source: ghost_source\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="references source 'ghost_source'"):
        load_central_config(cfg_file)


def test_no_side_effects_on_load() -> None:
    cfg_file = PROJECT_ROOT / "config.yml"
    before_bronze = list((PROJECT_ROOT / "data" / "bronze").glob("*"))
    before_silver = list((PROJECT_ROOT / "data" / "silver").glob("*"))

    cfg = load_central_config(cfg_file)
    assert cfg.version == 1

    after_bronze = list((PROJECT_ROOT / "data" / "bronze").glob("*"))
    after_silver = list((PROJECT_ROOT / "data" / "silver").glob("*"))

    assert [p.name for p in before_bronze] == [p.name for p in after_bronze]
    assert [p.name for p in before_silver] == [p.name for p in after_silver]


def test_backward_compatibility_with_existing_json_configs() -> None:
    json_path = PROJECT_ROOT / "config" / "dataset_customers_pipeline.json"
    cfg = load_pipeline_config(json_path)
    assert cfg.source_name == "assessment_customers"
    assert cfg.silver is not None
    assert cfg.silver.target_name == "dim_customers"


def test_synthetic_end_to_end_pipeline_execution_via_central_config(tmp_path: Path) -> None:
    in_csv = tmp_path / "customers.csv"
    in_csv.write_text(
        "customer_id,customer_name,postal_code\n"
        "C001,Alice,12345\n"
        "C002,Bob,67890\n",
        encoding="utf-8",
    )

    bronze_out = tmp_path / "bronze.jsonl"
    silver_out = tmp_path / "silver.jsonl"
    quarantine_out = tmp_path / "quarantine.jsonl"
    meta_db = tmp_path / "meta.db"

    cfg_file = tmp_path / "test_pipeline_config.yml"
    data = {
        "version": 1,
        "sources": {
            "cust_source": {
                "input_path": str(in_csv),
                "format": "csv",
                "bronze_path": str(bronze_out),
                "metadata_store": str(meta_db),
            }
        },
        "pipelines": {
            "cust_pipeline": {
                "source": "cust_source",
                "silver": {
                    "target_name": "dim_customers",
                    "silver_path": str(silver_out),
                    "rejected_path": str(quarantine_out),
                    "primary_keys": ["customer_id"],
                    "columns": [
                        {"name": "customer_id", "data_type": "string", "nullable": False},
                        {"name": "customer_name", "data_type": "string", "nullable": False},
                        {"name": "postal_code", "data_type": "string", "nullable": True},
                    ],
                    "mappings": [
                        {"source_column": "customer_id", "target_column": "customer_id", "transform": "trim"},
                        {"source_column": "customer_name", "target_column": "customer_name", "transform": "trim"},
                        {"source_column": "postal_code", "target_column": "postal_code", "transform": "trim"},
                    ],
                    "quality_rules": [
                        {"rule_name": "not_null", "columns": ["customer_id"], "severity": "reject"},
                    ],
                    "scd2": {
                        "enabled": True,
                        "business_keys": ["customer_id"],
                        "tracking_columns": ["customer_name"],
                    },
                },
            }
        },
    }
    cfg_file.write_text(yaml.safe_dump(path=None, data=data) if False else yaml.safe_dump(data), encoding="utf-8")

    central = load_central_config(cfg_file)
    pipeline_cfg = central.get_pipeline("cust_pipeline")

    # 1. First execution
    summary_1 = run_pipeline(pipeline_cfg, submission_key="sub_cust_001")
    assert summary_1.records_read == 2
    assert summary_1.records_silver == 2
    assert summary_1.records_scd2_changed == 2
    assert summary_1.records_rejected == 0
    assert summary_1.status.value == "SUCCESS"
    assert summary_1.reconciliation_status == "RECONCILED"

    # 2. Replay idempotency
    summary_replay = run_pipeline(pipeline_cfg, submission_key="sub_cust_001")
    assert summary_replay.idempotent_replay is True
    assert summary_replay.batch_id == summary_1.batch_id


@pytest.mark.skipif(
    not (PROJECT_ROOT / "data" / "fixtures" / "archive" / "olist_products_dataset.csv").exists(),
    reason="Olist products archive dataset not present locally",
)
def test_olist_products_end_to_end_from_central_config(tmp_path: Path) -> None:
    central = load_central_config(PROJECT_ROOT / "config.yml")
    pipe_cfg = central.get_pipeline("olist_products")

    iso_bronze = tmp_path / "olist_products_bronze.jsonl"
    iso_silver = tmp_path / "dim_products.jsonl"
    iso_quarantine = tmp_path / "quarantine_products.jsonl"
    iso_meta = tmp_path / "olist_meta.db"

    iso_silver_cfg = replace(
        pipe_cfg.silver,
        silver_path=iso_silver,
        rejected_path=iso_quarantine,
    )
    iso_pipe_cfg = replace(
        pipe_cfg,
        bronze_path=iso_bronze,
        metadata_store=iso_meta,
        silver=iso_silver_cfg,
    )

    summary = run_pipeline(iso_pipe_cfg, submission_key="olist_prod_test_001")
    assert summary.records_read == 32951
    assert summary.records_silver == 32951
    assert summary.records_scd2_changed == 32951
    assert summary.records_rejected == 0
    assert summary.status.value == "SUCCESS"
    assert summary.reconciliation_status == "RECONCILED"

    replay = run_pipeline(iso_pipe_cfg, submission_key="olist_prod_test_001")
    assert replay.idempotent_replay is True
    assert replay.batch_id == summary.batch_id
