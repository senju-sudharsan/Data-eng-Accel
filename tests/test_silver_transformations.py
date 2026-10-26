"""Tests for the Silver transformation module and conformed schema engine."""

from __future__ import annotations

import copy
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

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
    load_pipeline_config,
)
from accelerator.silver_transformations import SilverTransformer, compute_record_hash


def make_simple_silver_config(
    columns: list[ColumnSchema] | None = None,
    mappings: list[ColumnMapping] | None = None,
    extra_columns: ExtraColumnsPolicy = ExtraColumnsPolicy.IGNORE,
    missing_columns: MissingColumnsPolicy = MissingColumnsPolicy.NULL,
) -> SilverConfig:
    """Helper to construct a SilverConfig for testing."""
    return SilverConfig(
        target_name="test_target",
        silver_path=Path("dummy_silver.jsonl"),
        columns=columns or [],
        mappings=mappings or [],
        extra_columns=extra_columns,
        missing_columns=missing_columns,
    )


def test_protocol_conformance() -> None:
    """Verify that SilverTransformer adheres to SilverTransformerProtocol."""
    transformer = SilverTransformer()
    assert isinstance(transformer, SilverTransformerProtocol)


def test_basic_mapping_source_to_target() -> None:
    """Test 1: Basic mapping from source fields to canonical target fields."""
    config = make_simple_silver_config(
        columns=[
            ColumnSchema("cust_id", FieldType.STRING, nullable=False),
            ColumnSchema("cust_name", FieldType.STRING, nullable=False),
        ],
        mappings=[
            ColumnMapping(source_column="raw_id", target_column="cust_id"),
            ColumnMapping(source_column="raw_name", target_column="cust_name"),
        ],
    )
    raw_records = [{"raw_id": "C100", "raw_name": "Alice"}]
    transformer = SilverTransformer()
    results = transformer.transform(raw_records, config, batch_id="batch-001")

    assert len(results) == 1
    assert results[0]["cust_id"] == "C100"
    assert results[0]["cust_name"] == "Alice"
    assert results[0][LINEAGE_SILVER_BATCH_ID] == "batch-001"


def test_mapping_transforms_trim_upper_lower() -> None:
    """Test 2: Configured trim, upper, and lower transformations."""
    config = make_simple_silver_config(
        columns=[
            ColumnSchema("trimmed_col", FieldType.STRING),
            ColumnSchema("upper_col", FieldType.STRING),
            ColumnSchema("lower_col", FieldType.STRING),
        ],
        mappings=[
            ColumnMapping(source_column="s1", target_column="trimmed_col", transform="trim"),
            ColumnMapping(source_column="s2", target_column="upper_col", transform="upper"),
            ColumnMapping(source_column="s3", target_column="lower_col", transform="lower"),
        ],
    )
    raw = [{"s1": "  hello world  ", "s2": "mixed Case", "s3": "MIXED CASE"}]
    res = SilverTransformer().transform(raw, config, batch_id="b1")[0]

    assert res["trimmed_col"] == "hello world"
    assert res["upper_col"] == "MIXED CASE"
    assert res["lower_col"] == "mixed case"


def test_type_conversions_all_supported_types() -> None:
    """Test 3: Conversion of all supported types (string, int, float, decimal, bool, date, timestamp)."""
    config = make_simple_silver_config(
        columns=[
            ColumnSchema("str_val", FieldType.STRING),
            ColumnSchema("int_val", FieldType.INTEGER),
            ColumnSchema("float_val", FieldType.FLOAT),
            ColumnSchema("dec_val", FieldType.DECIMAL),
            ColumnSchema("bool_val", FieldType.BOOLEAN),
            ColumnSchema("date_val", FieldType.DATE),
            ColumnSchema("ts_val", FieldType.TIMESTAMP),
        ]
    )
    raw = [
        {
            "str_val": "text",
            "int_val": "42",
            "float_val": "3.1415",
            "dec_val": "99.99",
            "bool_val": "true",
            "date_val": "2026-10-26",
            "ts_val": "2026-10-26T14:30:00Z",
        }
    ]
    res = SilverTransformer().transform(raw, config, batch_id="b1")[0]

    assert res["str_val"] == "text"
    assert res["int_val"] == 42
    assert isinstance(res["int_val"], int)
    assert res["float_val"] == pytest.approx(3.1415)
    assert res["dec_val"] == Decimal("99.99")
    assert res["bool_val"] is True
    assert res["date_val"] == "2026-10-26"
    assert res["ts_val"] == "2026-10-26T14:30:00+00:00"


def test_invalid_values_raise_useful_errors() -> None:
    """Test 4: Non-convertible values and unsupported transforms raise descriptive errors."""
    transformer = SilverTransformer()

    # Invalid transform in mapping
    bad_tf_cfg = make_simple_silver_config(
        columns=[ColumnSchema("col", FieldType.STRING)],
        mappings=[ColumnMapping(source_column="col", target_column="col", transform="reverse")],
    )
    with pytest.raises(ValueError, match="Unsupported transform 'reverse'"):
        transformer.transform([{"col": "val"}], bad_tf_cfg, batch_id="b1")

    # Invalid integer (floating string to integer)
    int_cfg = make_simple_silver_config(columns=[ColumnSchema("num", FieldType.INTEGER)])
    with pytest.raises(ValueError, match="cannot be cast to integer"):
        transformer.transform([{"num": "12.34"}], int_cfg, batch_id="b1")

    # Non-integral float to integer
    with pytest.raises(ValueError, match="cannot be cast to integer without silent truncation"):
        transformer.transform([{"num": 12.5}], int_cfg, batch_id="b1")

    # Boolean to integer rejected
    with pytest.raises(ValueError, match="boolean value 'True' cannot be cast to integer"):
        transformer.transform([{"num": True}], int_cfg, batch_id="b1")

    # Invalid date
    date_cfg = make_simple_silver_config(columns=[ColumnSchema("dt", FieldType.DATE)])
    with pytest.raises(ValueError, match="cannot cast 'not-a-date' to date"):
        transformer.transform([{"dt": "not-a-date"}], date_cfg, batch_id="b1")

    # Invalid boolean
    bool_cfg = make_simple_silver_config(columns=[ColumnSchema("b", FieldType.BOOLEAN)])
    with pytest.raises(ValueError, match="cannot cast string 'maybe' to boolean"):
        transformer.transform([{"b": "maybe"}], bool_cfg, batch_id="b1")


def test_null_handling_and_non_nullable_enforcement() -> None:
    """Test 5: Null handling respects column nullability for all types."""
    transformer = SilverTransformer()

    nullable_cfg = make_simple_silver_config(
        columns=[
            ColumnSchema("c_str", FieldType.STRING, nullable=True),
            ColumnSchema("c_int", FieldType.INTEGER, nullable=True),
        ]
    )
    res = transformer.transform([{"c_str": None, "c_int": ""}], nullable_cfg, batch_id="b1")[0]
    assert res["c_str"] is None
    assert res["c_int"] is None

    non_null_cfg = make_simple_silver_config(
        columns=[ColumnSchema("req_col", FieldType.STRING, nullable=False)]
    )
    with pytest.raises(ValueError, match="non-nullable column received None"):
        transformer.transform([{"req_col": None}], non_null_cfg, batch_id="b1")

    with pytest.raises(ValueError, match="non-nullable string cannot be empty"):
        transformer.transform([{"req_col": ""}], non_null_cfg, batch_id="b1")


def test_missing_columns_null_policy() -> None:
    """Test 6a: Missing columns under 'null' policy are populated with None or default."""
    config = make_simple_silver_config(
        columns=[
            ColumnSchema("existing_col", FieldType.STRING),
            ColumnSchema("missing_col", FieldType.STRING, nullable=True),
            ColumnSchema("defaulted_col", FieldType.STRING, nullable=True),
        ],
        mappings=[
            ColumnMapping(source_column="existing_col", target_column="existing_col"),
            ColumnMapping(source_column="missing_raw", target_column="missing_col"),
            ColumnMapping(source_column="missing_def", target_column="defaulted_col", default_value="DEF"),
        ],
        missing_columns=MissingColumnsPolicy.NULL,
    )
    raw = [{"existing_col": "present"}]
    res = SilverTransformer().transform(raw, config, batch_id="b1")[0]

    assert res["existing_col"] == "present"
    assert res["missing_col"] is None
    assert res["defaulted_col"] == "DEF"


def test_missing_columns_fail_policy() -> None:
    """Test 6b: Missing columns under 'fail' policy raise an informative error."""
    config = make_simple_silver_config(
        columns=[
            ColumnSchema("c1", FieldType.STRING),
            ColumnSchema("c2", FieldType.STRING),
        ],
        mappings=[
            ColumnMapping(source_column="s1", target_column="c1"),
            ColumnMapping(source_column="s2", target_column="c2"),
        ],
        missing_columns=MissingColumnsPolicy.FAIL,
    )
    raw = [{"s1": "only_one"}]
    with pytest.raises(ValueError, match="Missing required source column 's2' for target column 'c2'"):
        SilverTransformer().transform(raw, config, batch_id="b1")


def test_extra_columns_ignore_policy() -> None:
    """Test 7a: Extra columns under 'ignore' policy are dropped from output."""
    config = make_simple_silver_config(
        columns=[ColumnSchema("c1", FieldType.STRING)],
        extra_columns=ExtraColumnsPolicy.IGNORE,
    )
    raw = [{"c1": "value", "unexpected_col": "drop_me"}]
    res = SilverTransformer().transform(raw, config, batch_id="b1")[0]

    assert "c1" in res
    assert "unexpected_col" not in res


def test_extra_columns_fail_policy() -> None:
    """Test 7b: Extra columns under 'fail' policy raise an informative error."""
    config = make_simple_silver_config(
        columns=[ColumnSchema("c1", FieldType.STRING)],
        extra_columns=ExtraColumnsPolicy.FAIL,
    )
    raw = [{"c1": "value", "unexpected_col": "fail_me"}]
    with pytest.raises(ValueError, match="Unexpected extra column\\(s\\) found: unexpected_col"):
        SilverTransformer().transform(raw, config, batch_id="b1")


def test_extra_columns_preserve_policy() -> None:
    """Test 7c: Extra columns under 'preserve' policy are safely saved in _unmapped_attributes."""
    config = make_simple_silver_config(
        columns=[ColumnSchema("c1", FieldType.STRING)],
        extra_columns=ExtraColumnsPolicy.PRESERVE,
    )
    raw = [{"c1": "val", "ext1": "extra1", "ext2": 123}]
    res = SilverTransformer().transform(raw, config, batch_id="b1")[0]

    assert res["c1"] == "val"
    assert "_unmapped_attributes" in res
    assert res["_unmapped_attributes"] == {"ext1": "extra1", "ext2": 123}
    # Ensure canonical fields were not overwritten
    assert "ext1" not in {k for k in res if not k.startswith("_")}


def test_mapped_source_columns_not_misclassified_as_extra() -> None:
    """Test 8: Renamed source columns are recognized and not misclassified as extra."""
    config = make_simple_silver_config(
        columns=[ColumnSchema("customer_id", FieldType.STRING)],
        mappings=[ColumnMapping(source_column="legacy_cust_id", target_column="customer_id")],
        extra_columns=ExtraColumnsPolicy.FAIL,
    )
    # legacy_cust_id is mapped, and _ingestion_* are bronze lineage fields; neither should fail as extra
    raw = [
        {
            "legacy_cust_id": "C001",
            "_ingestion_batch_id": "bronze-batch-1",
            "_ingestion_timestamp": "2026-10-26T12:00:00Z",
        }
    ]
    res = SilverTransformer().transform(raw, config, batch_id="silver-b1")[0]
    assert res["customer_id"] == "C001"


def test_silver_lineage_fields_populated() -> None:
    """Test 9: Silver lineage fields (_silver_batch_id, _silver_processed_at) are present."""
    config = make_simple_silver_config(columns=[ColumnSchema("id", FieldType.STRING)])
    raw = [{"id": "1"}]
    res = SilverTransformer().transform(raw, config, batch_id="batch-xyz")[0]

    assert res[LINEAGE_SILVER_BATCH_ID] == "batch-xyz"
    assert res[LINEAGE_SILVER_PROCESSED_AT]
    # Verify ISO-8601 UTC format
    dt = datetime.fromisoformat(res[LINEAGE_SILVER_PROCESSED_AT])
    assert dt.tzinfo is not None


def test_deterministic_record_hash_stability() -> None:
    """Test 10 & 11: Hash is deterministic and remains stable across different batches/timestamps."""
    config = make_simple_silver_config(
        columns=[
            ColumnSchema("name", FieldType.STRING),
            ColumnSchema("age", FieldType.INTEGER),
        ]
    )
    raw = [{"name": "Ada", "age": 30}]
    transformer = SilverTransformer()

    res1 = transformer.transform(raw, config, batch_id="batch-A")[0]
    res2 = transformer.transform(raw, config, batch_id="batch-B")[0]

    assert res1[LINEAGE_SILVER_BATCH_ID] != res2[LINEAGE_SILVER_BATCH_ID]
    # The record hash must be identical because business values are identical
    assert res1[LINEAGE_RECORD_HASH] == res2[LINEAGE_RECORD_HASH]


def test_record_hash_changes_when_data_changes() -> None:
    """Test 12: Hash changes whenever any canonical business value changes."""
    config = make_simple_silver_config(
        columns=[
            ColumnSchema("name", FieldType.STRING),
            ColumnSchema("age", FieldType.INTEGER),
        ]
    )
    transformer = SilverTransformer()

    res1 = transformer.transform([{"name": "Ada", "age": 30}], config, batch_id="b1")[0]
    res2 = transformer.transform([{"name": "Ada", "age": 31}], config, batch_id="b1")[0]

    assert res1[LINEAGE_RECORD_HASH] != res2[LINEAGE_RECORD_HASH]


def test_no_mutation_of_input_records() -> None:
    """Test 13: Input records are never mutated during transformation."""
    config = make_simple_silver_config(
        columns=[ColumnSchema("cust_id", FieldType.STRING)],
        mappings=[ColumnMapping(source_column="id", target_column="cust_id", transform="trim")],
    )
    original_input = [{"id": "   C123   ", "other": "val"}]
    deep_copy_input = copy.deepcopy(original_input)

    SilverTransformer().transform(original_input, config, batch_id="b1")

    assert original_input == deep_copy_input


def test_empty_input_batches() -> None:
    """Test 14: Empty input records stream returns an empty list without error."""
    config = make_simple_silver_config(columns=[ColumnSchema("col", FieldType.STRING)])
    results = SilverTransformer().transform([], config, batch_id="b1")
    assert results == []


def test_multiple_records_in_one_batch() -> None:
    """Test 15: Multiple records are processed in order within a single batch."""
    config = make_simple_silver_config(
        columns=[
            ColumnSchema("id", FieldType.STRING),
            ColumnSchema("seq", FieldType.INTEGER),
        ]
    )
    raw = [
        {"id": "A", "seq": "1"},
        {"id": "B", "seq": "2"},
        {"id": "C", "seq": "3"},
    ]
    results = SilverTransformer().transform(raw, config, batch_id="b-multi")

    assert len(results) == 3
    assert [r["id"] for r in results] == ["A", "B", "C"]
    assert [r["seq"] for r in results] == [1, 2, 3]
    # Batch timestamp should be uniform across records in the same batch
    assert len({r[LINEAGE_SILVER_PROCESSED_AT] for r in results}) == 1


def test_malformed_records_error_messages() -> None:
    """Test 16: Non-dict records or invalid structures raise contextual errors."""
    config = make_simple_silver_config(columns=[ColumnSchema("id", FieldType.STRING)])
    with pytest.raises(ValueError, match="Record 0: Expected dictionary record, got str"):
        SilverTransformer().transform(["not a dict"], config, batch_id="b1")  # type: ignore


def test_postal_codes_retain_leading_zeros() -> None:
    """Test 17: Leading zeros in postal codes are strictly preserved as strings."""
    config = make_simple_silver_config(
        columns=[ColumnSchema("postal_code", FieldType.STRING)],
        mappings=[ColumnMapping(source_column="postal_code", target_column="postal_code", transform="trim")],
    )
    raw = [
        {"postal_code": "00123"},
        {"postal_code": "04567"},
        {"postal_code": "07890"},
    ]
    results = SilverTransformer().transform(raw, config, batch_id="b-zeros")

    assert [r["postal_code"] for r in results] == ["00123", "04567", "07890"]


def test_real_dataset_customers_pipeline_config() -> None:
    """Test 18: Full validation using real config/dataset_customers_pipeline.json."""
    pipeline_cfg = load_pipeline_config("config/dataset_customers_pipeline.json")
    assert pipeline_cfg.silver is not None
    silver_cfg = pipeline_cfg.silver

    raw_fixture_records = [
        {"customer_id": "C001", "customer_name": "  Ada  ", "postal_code": "00123"},
        {"customer_id": "C002", "customer_name": "Ben", "postal_code": "04567"},
        {"customer_id": "C003", "customer_name": " Chandra ", "postal_code": "07890"},
    ]

    transformer = SilverTransformer()
    silver_records = transformer.transform(raw_fixture_records, silver_cfg, batch_id="silver-batch-001")

    assert len(silver_records) == 3

    # Check Ada
    assert silver_records[0]["customer_id"] == "C001"
    assert silver_records[0]["customer_name"] == "Ada"
    assert silver_records[0]["postal_code"] == "00123"

    # Check Ben
    assert silver_records[1]["customer_id"] == "C002"
    assert silver_records[1]["customer_name"] == "Ben"
    assert silver_records[1]["postal_code"] == "04567"

    # Check Chandra
    assert silver_records[2]["customer_id"] == "C003"
    assert silver_records[2]["customer_name"] == "Chandra"
    assert silver_records[2]["postal_code"] == "07890"

    # Lineage and Hash verification
    for rec in silver_records:
        assert rec[LINEAGE_SILVER_BATCH_ID] == "silver-batch-001"
        assert rec[LINEAGE_SILVER_PROCESSED_AT]
        assert rec[LINEAGE_RECORD_HASH]
        assert len(rec[LINEAGE_RECORD_HASH]) == 64  # SHA-256 hex string length


def test_real_csv_fixtures_end_to_end_transformation() -> None:
    """Validate transformations on records read directly from data/fixtures/ CSV files."""
    import csv

    pipeline_cfg = load_pipeline_config("config/dataset_customers_pipeline.json")
    silver_cfg = pipeline_cfg.silver
    assert silver_cfg is not None

    fixtures_dir = PROJECT_ROOT / "data" / "fixtures"

    # Ingest customers.csv
    with (fixtures_dir / "customers.csv").open("r", encoding="utf-8") as f:
        records_1 = list(csv.DictReader(f))

    # Ingest customers_alternate.csv
    with (fixtures_dir / "customers_alternate.csv").open("r", encoding="utf-8") as f:
        records_2 = list(csv.DictReader(f))

    transformer = SilverTransformer()

    transformed_1 = transformer.transform(records_1, silver_cfg, batch_id="batch-f1")
    assert len(transformed_1) == 2
    assert transformed_1[0]["customer_id"] == "C001"
    assert transformed_1[0]["customer_name"] == "Ada"
    assert transformed_1[0]["postal_code"] == "00123"
    assert transformed_1[1]["customer_id"] == "C002"
    assert transformed_1[1]["customer_name"] == "Ben"
    assert transformed_1[1]["postal_code"] == "04567"

    transformed_2 = transformer.transform(records_2, silver_cfg, batch_id="batch-f2")
    assert len(transformed_2) == 1
    assert transformed_2[0]["customer_id"] == "C003"
    assert transformed_2[0]["customer_name"] == "Chandra"
    assert transformed_2[0]["postal_code"] == "07890"

def test_bronze_to_silver_integration_handoff(tmp_path: Path) -> None:
    """Test complete handoff from BronzeReader output to SilverTransformer."""
    from accelerator.bronze_ingestion import BronzeReader

    bronze_file = PROJECT_ROOT / "data" / "bronze" / "customers_raw.jsonl"
    if not bronze_file.exists():
        pytest.skip("Bronze customers_raw.jsonl not found")

    reader = BronzeReader()
    bronze_records = list(reader.read(bronze_file))
    assert len(bronze_records) > 0

    pipeline_cfg = load_pipeline_config("config/dataset_customers_pipeline.json")
    assert pipeline_cfg.silver is not None

    transformer = SilverTransformer()
    silver_records = transformer.transform(
        records=bronze_records,
        silver_config=pipeline_cfg.silver,
        batch_id="test-bronze-silver-handoff",
    )

    assert len(silver_records) == len(bronze_records)
    for rec in silver_records:
        assert "customer_id" in rec
        assert "customer_name" in rec
        assert "postal_code" in rec
        assert rec[LINEAGE_SILVER_BATCH_ID] == "test-bronze-silver-handoff"
        assert rec[LINEAGE_RECORD_HASH]
        # Ensure leading zeros on postal_code were not coerced or truncated
        if rec["customer_id"] == "C001":
            assert rec["postal_code"] == "00123"
