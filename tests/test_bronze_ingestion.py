import json
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from accelerator.bronze_ingestion import ingest, load_config


def write_config(tmp_path: Path, source_name: str, input_name: str) -> Path:
    (tmp_path / input_name).write_text("customer_id,postal_code\nC001,00123\n", encoding="utf-8")
    config_path = tmp_path / f"{source_name}.json"
    config_path.write_text(
        json.dumps(
            {
                "source_name": source_name,
                "format": "csv",
                "input_path": input_name,
                "bronze_path": "bronze/customers.jsonl",
                "metadata_store": "metadata/batches.db",
            }
        ),
        encoding="utf-8",
    )
    return config_path


def test_csv_ingestion_preserves_strings_and_records_lineage(tmp_path: Path) -> None:
    result = ingest(write_config(tmp_path, "customers_one", "one.csv"))
    rows = [json.loads(line) for line in (tmp_path / "bronze/customers.jsonl").read_text(encoding="utf-8").splitlines()]

    assert result["records_processed"] == 1
    assert rows[0]["postal_code"] == "00123"
    assert rows[0]["_ingestion_source_name"] == "customers_one"
    assert rows[0]["_ingestion_batch_id"] == result["batch_id"]
    assert rows[0]["_ingestion_timestamp"]

    with sqlite3.connect(tmp_path / "metadata/batches.db") as connection:
        batch = connection.execute("SELECT status, records_processed FROM batch_control").fetchone()
    assert batch == ("SUCCESS", 1)


def test_changing_only_configuration_ingests_second_compatible_source(tmp_path: Path) -> None:
    first = ingest(write_config(tmp_path, "customers_one", "one.csv"))
    second = ingest(write_config(tmp_path, "customers_two", "two.csv"))
    rows = [json.loads(line) for line in (tmp_path / "bronze/customers.jsonl").read_text(encoding="utf-8").splitlines()]

    assert [row["_ingestion_source_name"] for row in rows] == ["customers_one", "customers_two"]
    assert {row["_ingestion_batch_id"] for row in rows} == {first["batch_id"], second["batch_id"]}


def test_configuration_validation_rejects_unsupported_format(tmp_path: Path) -> None:
    config_path = tmp_path / "bad.json"
    config_path.write_text(json.dumps({"source_name": "bad", "format": "xml", "input_path": "x", "bronze_path": "out", "metadata_store": "meta"}), encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported format"):
        load_config(config_path)


def test_json_array_reader_is_available_as_a_small_extension(tmp_path: Path) -> None:
    (tmp_path / "customers.json").write_text('[{"customer_id": "C004", "postal_code": "00999"}]', encoding="utf-8")
    config_path = tmp_path / "json.json"
    config_path.write_text(
        json.dumps(
            {
                "source_name": "customers_json",
                "format": "json",
                "input_path": "customers.json",
                "bronze_path": "bronze/customers.jsonl",
                "metadata_store": "metadata/batches.db",
            }
        ),
        encoding="utf-8",
    )

    ingest(config_path)
    row = json.loads((tmp_path / "bronze/customers.jsonl").read_text(encoding="utf-8"))
    assert row["customer_id"] == "C004"
    assert row["_ingestion_source_name"] == "customers_json"
