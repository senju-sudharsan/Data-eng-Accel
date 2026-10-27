"""Focused tests for local JSONL SCD Type 2 history management."""

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from accelerator import scd2_processor
from accelerator.contracts import (
    LINEAGE_BATCH_ID,
    LINEAGE_SILVER_BATCH_ID,
    SCD_EFFECTIVE_FROM,
    SCD_EFFECTIVE_TO,
    SCD_IS_CURRENT,
    SCD_SURROGATE_KEY,
    Scd2Config,
    Scd2ProcessorProtocol,
    load_pipeline_config,
)
from accelerator.scd2_processor import Scd2Processor


def scd2_config() -> Scd2Config:
    return Scd2Config(
        enabled=True,
        business_keys=["customer_id"],
        tracking_columns=["customer_name", "postal_code"],
    )


def customer(customer_id: str, name: str, postal_code: str, *, bronze_batch: str = "bronze-1") -> dict[str, str]:
    return {
        "customer_id": customer_id,
        "customer_name": name,
        "postal_code": postal_code,
        LINEAGE_BATCH_ID: bronze_batch,
    }


def read_rows(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_processor_conforms_to_protocol() -> None:
    assert isinstance(Scd2Processor(), Scd2ProcessorProtocol)


def test_initial_load_creates_current_versions_with_lineage(tmp_path: Path) -> None:
    target = tmp_path / "dim_customers.jsonl"
    rows = [customer("C001", "Ada", "00123"), customer("C002", "Ben", "04567")]

    assert Scd2Processor().apply_scd2(rows, target, scd2_config(), "silver-1") == 2

    persisted = read_rows(target)
    assert len(persisted) == 2
    assert all(row[SCD_IS_CURRENT] is True and row[SCD_EFFECTIVE_TO] is None for row in persisted)
    assert all(row[SCD_SURROGATE_KEY] and row[LINEAGE_SILVER_BATCH_ID] == "silver-1" for row in persisted)
    assert persisted[0][LINEAGE_BATCH_ID] == "bronze-1"
    assert all(datetime.fromisoformat(str(row[SCD_EFFECTIVE_FROM])).tzinfo is not None for row in persisted)


def test_tracked_change_closes_previous_at_replacement_start(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    processor = Scd2Processor()
    processor.apply_scd2([customer("C001", "Ada", "00123")], target, scd2_config(), "silver-1")

    assert processor.apply_scd2([customer("C001", "Ada Lovelace", "00999")], target, scd2_config(), "silver-2") == 1
    old, new = read_rows(target)
    assert old[SCD_IS_CURRENT] is False
    assert new[SCD_IS_CURRENT] is True
    assert old[SCD_EFFECTIVE_TO] == new[SCD_EFFECTIVE_FROM]
    assert old[SCD_SURROGATE_KEY] != new[SCD_SURROGATE_KEY]


def test_unchanged_record_creates_no_new_version(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    processor = Scd2Processor()
    processor.apply_scd2([customer("C001", "Ada", "00123")], target, scd2_config(), "silver-1")
    before = target.read_text(encoding="utf-8")

    assert processor.apply_scd2([customer("C001", "Ada", "00123")], target, scd2_config(), "silver-2") == 0
    assert target.read_text(encoding="utf-8") == before


def test_new_key_and_multiple_tracked_changes_each_create_one_version(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    processor = Scd2Processor()
    processor.apply_scd2([customer("C001", "Ada", "00123")], target, scd2_config(), "silver-1")

    assert processor.apply_scd2(
        [customer("C001", "Ada Lovelace", "00999"), customer("C002", "Ben", "04567")],
        target, scd2_config(), "silver-2",
    ) == 2
    rows = read_rows(target)
    assert len(rows) == 3
    assert len([row for row in rows if row["customer_id"] == "C001" and row[SCD_IS_CURRENT]]) == 1
    assert len([row for row in rows if row["customer_id"] == "C002" and row[SCD_IS_CURRENT]]) == 1


def test_rerunning_an_already_applied_batch_does_not_reopen_history(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    processor = Scd2Processor()
    original = customer("C001", "Ada", "00123")
    changed = customer("C001", "Ada Lovelace", "00999")
    processor.apply_scd2([original], target, scd2_config(), "silver-1")
    processor.apply_scd2([changed], target, scd2_config(), "silver-2")

    assert processor.apply_scd2([original], target, scd2_config(), "silver-1") == 0
    rows = read_rows(target)
    assert len(rows) == 2
    assert [row["customer_name"] for row in rows if row[SCD_IS_CURRENT]] == ["Ada Lovelace"]


@pytest.mark.parametrize("bad_key", [None, "", "   "])
def test_invalid_business_key_is_rejected(tmp_path: Path, bad_key: object) -> None:
    record = customer("C001", "Ada", "00123")
    record["customer_id"] = bad_key  # type: ignore[assignment]
    with pytest.raises(ValueError, match="Invalid empty business key"):
        Scd2Processor().apply_scd2([record], tmp_path / "dimension.jsonl", scd2_config(), "silver-1")


def test_duplicate_business_key_in_one_batch_is_rejected_without_output(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    with pytest.raises(ValueError, match="Duplicate business key"):
        Scd2Processor().apply_scd2(
            [customer("C001", "Ada", "00123"), customer("C001", "Ada 2", "00123")],
            target, scd2_config(), "silver-1",
        )
    assert not target.exists()


def test_missing_incoming_tracking_column_is_rejected_without_output(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    record = customer("C001", "Ada", "00123")
    del record["postal_code"]
    with pytest.raises(ValueError, match="Missing tracking column.*'postal_code'.*incoming record 0"):
        Scd2Processor().apply_scd2([record], target, scd2_config(), "silver-1")
    assert not target.exists()


def test_missing_persisted_tracking_column_is_rejected_without_modifying_history(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    target.write_text(
        json.dumps({"customer_id": "C001", "customer_name": "Ada", LINEAGE_SILVER_BATCH_ID: "silver-1", SCD_IS_CURRENT: True}) + "\n",
        encoding="utf-8",
    )
    before = target.read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="Missing tracking column.*'postal_code'.*persisted dimension record 0"):
        Scd2Processor().apply_scd2([customer("C002", "Ben", "04567")], target, scd2_config(), "silver-2")
    assert target.read_text(encoding="utf-8") == before


def test_conflicting_persisted_batch_history_is_rejected_without_modifying_history(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    conflicting_rows = [
        {"customer_id": "C001", "customer_name": "Ada", "postal_code": "00123", LINEAGE_SILVER_BATCH_ID: "silver-1", SCD_IS_CURRENT: False},
        {"customer_id": "C001", "customer_name": "Ada", "postal_code": "00999", LINEAGE_SILVER_BATCH_ID: "silver-1", SCD_IS_CURRENT: True},
    ]
    target.write_text("".join(json.dumps(row) + "\n" for row in conflicting_rows), encoding="utf-8")
    before = target.read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="Conflicting persisted history"):
        Scd2Processor().apply_scd2([customer("C002", "Ben", "04567")], target, scd2_config(), "silver-2")
    assert target.read_text(encoding="utf-8") == before


def test_malformed_persisted_dimension_is_diagnostic_and_unchanged(tmp_path: Path) -> None:
    target = tmp_path / "dimension.jsonl"
    target.write_text('{"customer_id":"C001"}\nnot-json\n', encoding="utf-8")
    before = target.read_text(encoding="utf-8")
    with pytest.raises(ValueError, match=r"dimension.jsonl:2"):
        Scd2Processor().apply_scd2([customer("C002", "Ben", "04567")], target, scd2_config(), "silver-1")
    assert target.read_text(encoding="utf-8") == before


def test_atomic_write_failure_preserves_existing_dimension(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "dimension.jsonl"
    processor = Scd2Processor()
    processor.apply_scd2([customer("C001", "Ada", "00123")], target, scd2_config(), "silver-1")
    before = target.read_text(encoding="utf-8")
    original_replace = scd2_processor.os.replace
    def fail_replace(staged: Path, destination: Path) -> None:
        raise OSError("simulated replacement failure")
    monkeypatch.setattr(scd2_processor.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated replacement"):
        processor.apply_scd2([customer("C002", "Ben", "04567")], target, scd2_config(), "silver-2")
    assert target.read_text(encoding="utf-8") == before
    assert not list(tmp_path.glob("*.staged"))
    monkeypatch.setattr(scd2_processor.os, "replace", original_replace)


def test_processor_rejects_disabled_or_inconsistent_batch_lineage(tmp_path: Path) -> None:
    disabled = Scd2Config(enabled=False, business_keys=["customer_id"], tracking_columns=["customer_name"])
    with pytest.raises(ValueError, match="enabled"):
        Scd2Processor().apply_scd2([customer("C001", "Ada", "00123")], tmp_path / "dimension.jsonl", disabled, "silver-1")
    mismatched = customer("C001", "Ada", "00123")
    mismatched[LINEAGE_SILVER_BATCH_ID] = "other-batch"
    with pytest.raises(ValueError, match="expected 'silver-1'"):
        Scd2Processor().apply_scd2([mismatched], tmp_path / "dimension.jsonl", scd2_config(), "silver-1")


def test_pipeline_config_rejects_tracking_column_outside_schema(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("customer_id\nC001\n", encoding="utf-8")
    config = {
        "source_name": "customers", "format": "csv", "input_path": "source.csv",
        "bronze_path": "bronze.jsonl", "metadata_store": "metadata.db",
        "silver": {
            "target_name": "dim_customers", "silver_path": "dimension.jsonl",
            "columns": [{"name": "customer_id", "data_type": "string"}],
            "scd2": {"enabled": True, "business_keys": ["customer_id"], "tracking_columns": ["missing"]},
        },
    }
    config_path = tmp_path / "bad_tracking.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError, match="SCD2 tracking_column 'missing'"):
        load_pipeline_config(config_path)
