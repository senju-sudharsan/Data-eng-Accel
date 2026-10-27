"""Tests for Phase 4: Data Quality Validation and Rejected-Record Handling."""

from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

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
    ColumnSchema,
    FieldType,
    QualityCheckResult,
    QualityRule,
    QualityValidatorProtocol,
    SilverConfig,
    ValidationSeverity,
    load_pipeline_config,
)
from accelerator.quality_validator import QualityValidator
from accelerator.silver_transformations import SilverTransformer


def make_test_rules() -> list[QualityRule]:
    """Standard 3 quality rules matching config/dataset_customers_pipeline.json."""
    return [
        QualityRule(
            rule_name="not_null",
            columns=["customer_id"],
            severity=ValidationSeverity.REJECT,
        ),
        QualityRule(
            rule_name="unique",
            columns=["customer_id"],
            severity=ValidationSeverity.REJECT,
        ),
        QualityRule(
            rule_name="regex",
            columns=["postal_code"],
            severity=ValidationSeverity.WARN,
            params={"pattern": r"^\d{5}$"},
        ),
    ]


def test_validator_protocol_conformance() -> None:
    """Verify QualityValidator conforms to runtime-checkable QualityValidatorProtocol."""
    validator = QualityValidator()
    assert isinstance(validator, QualityValidatorProtocol)


def test_valid_records_pass_all_rules() -> None:
    """Valid records pass all rejecting rules and are accepted cleanly into valid_records."""
    records = [
        {
            "customer_id": "C001",
            "customer_name": "Ada",
            "postal_code": "00123",
            LINEAGE_SILVER_BATCH_ID: "batch-1",
            LINEAGE_SILVER_PROCESSED_AT: "2026-10-27T10:00:00+00:00",
            LINEAGE_RECORD_HASH: "hash-001",
        },
        {
            "customer_id": "C002",
            "customer_name": "Ben",
            "postal_code": "04567",
            LINEAGE_SILVER_BATCH_ID: "batch-1",
            LINEAGE_SILVER_PROCESSED_AT: "2026-10-27T10:00:00+00:00",
            LINEAGE_RECORD_HASH: "hash-002",
        },
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(records, make_test_rules())

    assert len(valid) == 2
    assert len(rejected) == 0
    assert all(s.passed for s in summaries)
    assert all(s.failed_record_count == 0 for s in summaries)

    # Clean Silver output: no quarantine metadata
    for rec in valid:
        assert REJECTION_REASON not in rec
        assert REJECTION_RULE not in rec
        assert REJECTION_TIMESTAMP not in rec
        assert REJECTION_BATCH_ID not in rec
        assert REJECTION_RAW_RECORD not in rec
        assert rec[LINEAGE_SILVER_BATCH_ID] == "batch-1"
        assert rec[LINEAGE_RECORD_HASH]


def test_missing_customer_id_fails_not_null() -> None:
    """Record with missing/None customer_id fails not_null rule with REJECT severity."""
    records = [
        {"customer_id": None, "customer_name": "No ID", "postal_code": "12345"},
        {"customer_name": "Missing Key", "postal_code": "12345"},
    ]
    raw = [
        {"customer_id": "", "customer_name": "No ID", "_ingestion_source_name": "test"},
        {"customer_name": "Missing Key", "_ingestion_source_name": "test"},
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(
        records, make_test_rules(), raw_records=raw, batch_id="b-null"
    )

    assert len(valid) == 0
    assert len(rejected) == 2

    # Check summaries
    not_null_summary = next(s for s in summaries if s.rule_name == "not_null")
    assert not not_null_summary.passed
    assert not_null_summary.failed_record_count == 2
    assert not_null_summary.severity == ValidationSeverity.REJECT

    # Check quarantine decoration
    for q_rec in rejected:
        assert q_rec[REJECTION_RULE] == "not_null"
        assert "Column 'customer_id' is null or empty" in q_rec[REJECTION_REASON]
        assert q_rec[REJECTION_BATCH_ID] == "b-null"
        assert q_rec[REJECTION_TIMESTAMP]
        assert isinstance(q_rec[REJECTION_RAW_RECORD], dict)
        assert q_rec[REJECTION_RAW_RECORD]["_ingestion_source_name"] == "test"


def test_empty_customer_id_fails_not_null() -> None:
    """Record with empty string or whitespace customer_id fails not_null."""
    records = [
        {"customer_id": "", "customer_name": "Empty"},
        {"customer_id": "   ", "customer_name": "Whitespace"},
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(records, make_test_rules())

    assert len(valid) == 0
    assert len(rejected) == 2
    for q_rec in rejected:
        assert q_rec[REJECTION_RULE] == "not_null"
        assert "Column 'customer_id' is null or empty" in q_rec[REJECTION_REASON]


def test_first_occurrence_wins_and_subsequent_duplicate_quarantined() -> None:
    """Decision A: First occurrence of each customer_id is preserved; subsequent duplicates quarantined."""
    records = [
        {"customer_id": "C001", "customer_name": "Ada", "postal_code": "00123"},
        {"customer_id": "C002", "customer_name": "Ben", "postal_code": "04567"},
        {"customer_id": "C001", "customer_name": "Ada Duplicate", "postal_code": "00123"},
        {"customer_id": "C001", "customer_name": "Ada Triplicate", "postal_code": "00123"},
    ]
    raw = [
        {"customer_id": "C001", "version": 1},
        {"customer_id": "C002", "version": 1},
        {"customer_id": "C001", "version": 2},
        {"customer_id": "C001", "version": 3},
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(
        records, make_test_rules(), raw_records=raw, batch_id="batch-dup"
    )

    # First occurrence of C001 and C002 accepted
    assert len(valid) == 2
    assert [r["customer_id"] for r in valid] == ["C001", "C002"]
    assert valid[0]["customer_name"] == "Ada"

    # Subsequent duplicates quarantined in source order
    assert len(rejected) == 2
    assert rejected[0]["customer_name"] == "Ada Duplicate"
    assert rejected[0][REJECTION_RULE] == "unique"
    assert "Duplicate value 'C001' in column(s) customer_id" in rejected[0][REJECTION_REASON]
    assert rejected[0][REJECTION_RAW_RECORD]["version"] == 2

    assert rejected[1]["customer_name"] == "Ada Triplicate"
    assert rejected[1][REJECTION_RULE] == "unique"
    assert rejected[1][REJECTION_RAW_RECORD]["version"] == 3

    unique_summary = next(s for s in summaries if s.rule_name == "unique")
    assert not unique_summary.passed
    assert unique_summary.failed_record_count == 2


def test_missing_ids_handled_alongside_uniqueness_checks() -> None:
    """Missing customer IDs fail not_null without falsely being grouped as duplicate keys in unique check."""
    records = [
        {"customer_id": None, "customer_name": "Missing 1", "postal_code": "00123"},
        {"customer_id": None, "customer_name": "Missing 2", "postal_code": "00123"},
        {"customer_id": "C001", "customer_name": "Valid 1", "postal_code": "00123"},
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(records, make_test_rules())

    # C001 accepted
    assert len(valid) == 1
    assert valid[0]["customer_id"] == "C001"

    # Both missing records rejected by not_null, NOT unique
    assert len(rejected) == 2
    for r in rejected:
        assert r[REJECTION_RULE] == "not_null"
        assert "Column 'customer_id' is null or empty" in r[REJECTION_REASON]

    not_null_sum = next(s for s in summaries if s.rule_name == "not_null")
    assert not_null_sum.failed_record_count == 2

    unique_sum = next(s for s in summaries if s.rule_name == "unique")
    # unique rule did not fail because missing IDs were not grouped as duplicate keys
    assert unique_sum.passed
    assert unique_sum.failed_record_count == 0


def test_regex_postal_code_warning_allows_record_through() -> None:
    """Non-matching postal code logs WARN severity in summaries but allows record into valid_records."""
    records = [
        {"customer_id": "C001", "customer_name": "Ada", "postal_code": "INVALID_ZIP"},
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(records, make_test_rules())

    # Record passes because regex severity is WARN
    assert len(valid) == 1
    assert len(rejected) == 0
    assert valid[0]["customer_id"] == "C001"
    assert REJECTION_REASON not in valid[0]

    regex_sum = next(s for s in summaries if s.rule_name == "regex")
    assert not regex_sum.passed
    assert regex_sum.failed_record_count == 1
    assert regex_sum.severity == ValidationSeverity.WARN
    assert "Rule 'regex' [warn] failed on 1 of 1 record(s)" in regex_sum.details


def test_regex_postal_code_rejection_quarantines_record() -> None:
    """Configuring regex rule with severity REJECT diverts non-matching records to quarantine."""
    rules = [
        QualityRule(
            rule_name="regex",
            columns=["postal_code"],
            severity=ValidationSeverity.REJECT,
            params={"pattern": r"^\d{5}$"},
        )
    ]
    records = [
        {"customer_id": "C001", "postal_code": "12345"},  # Valid
        {"customer_id": "C002", "postal_code": "BADZIP"}, # Invalid
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(records, rules)

    assert len(valid) == 1
    assert valid[0]["customer_id"] == "C001"

    assert len(rejected) == 1
    assert rejected[0]["customer_id"] == "C002"
    assert rejected[0][REJECTION_RULE] == "regex"
    assert "does not match pattern" in rejected[0][REJECTION_REASON]


def test_record_failing_multiple_rules_captures_all_failures_in_one_quarantine_record() -> None:
    """Decision B: Record failing multiple rejecting rules produces ONE quarantine record with all failures."""
    rules = [
        QualityRule(rule_name="not_null", columns=["customer_id"], severity=ValidationSeverity.REJECT),
        QualityRule(rule_name="regex", columns=["postal_code"], severity=ValidationSeverity.REJECT, params={"pattern": r"^\d{5}$"}),
    ]
    records = [
        {"customer_id": "", "customer_name": "Empty ID", "postal_code": "BADZIP"},
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(records, rules)

    assert len(valid) == 0
    # Exactly ONE quarantined record produced
    assert len(rejected) == 1

    quarantined = rejected[0]
    # Contains all failed rule identifiers in deterministic order
    assert quarantined[REJECTION_RULE] == "not_null, regex"
    # Contains all rejection reasons
    assert "Column 'customer_id' is null or empty" in quarantined[REJECTION_REASON]
    assert "does not match pattern" in quarantined[REJECTION_REASON]
    assert "; " in quarantined[REJECTION_REASON]

    # Both rule summaries show the failure
    assert all(not s.passed for s in summaries)
    assert all(s.failed_record_count == 1 for s in summaries)


def test_quarantined_record_captures_warn_and_reject_failures() -> None:
    """When quarantined due to a REJECT rule, any co-occurring WARN rule failure is also captured."""
    records = [
        {"customer_id": "", "customer_name": "Empty ID", "postal_code": "BADZIP"},
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(records, make_test_rules())

    assert len(valid) == 0
    assert len(rejected) == 1

    quarantined = rejected[0]
    assert "not_null" in quarantined[REJECTION_RULE]
    assert "regex" in quarantined[REJECTION_RULE]
    assert "Column 'customer_id' is null or empty" in quarantined[REJECTION_REASON]
    assert "does not match pattern" in quarantined[REJECTION_REASON]


def test_quarantine_records_preserve_raw_record_and_lineage() -> None:
    """Quarantine records preserve original raw record with Bronze lineage attributes."""
    records = [
        {"customer_id": None, "customer_name": "Ada", "postal_code": "00123"},
    ]
    raw_bronze = [
        {
            "customer_id": "",
            "customer_name": "  Ada  ",
            "postal_code": "00123",
            LINEAGE_SOURCE_NAME: "crm_customers",
            LINEAGE_BATCH_ID: "bronze-batch-101",
            LINEAGE_INGESTION_TIMESTAMP: "2026-10-27T09:00:00Z",
        }
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(
        records, make_test_rules(), raw_records=raw_bronze, batch_id="silver-batch-202"
    )

    assert len(rejected) == 1
    q = rejected[0]
    assert q[REJECTION_BATCH_ID] == "silver-batch-202"
    assert q[REJECTION_TIMESTAMP]
    # Verify timestamp is ISO-8601 UTC
    dt = datetime.fromisoformat(q[REJECTION_TIMESTAMP])
    assert dt.tzinfo is not None

    raw_rec = q[REJECTION_RAW_RECORD]
    assert raw_rec["customer_name"] == "  Ada  "
    assert raw_rec[LINEAGE_SOURCE_NAME] == "crm_customers"
    assert raw_rec[LINEAGE_BATCH_ID] == "bronze-batch-101"
    assert raw_rec[LINEAGE_INGESTION_TIMESTAMP] == "2026-10-27T09:00:00Z"


def test_quarantine_preserves_source_values_that_change_during_transformation() -> None:
    """Regression test: Quarantined record preserves original Bronze values that change during Silver transformation."""
    from accelerator.contracts import ColumnMapping, ExtraColumnsPolicy

    silver_cfg = SilverConfig(
        target_name="dim_customers",
        silver_path=Path("dummy_silver.jsonl"),
        columns=[
            ColumnSchema("customer_id", FieldType.STRING, nullable=False),
            ColumnSchema("customer_name", FieldType.STRING, nullable=False),
            ColumnSchema("postal_code", FieldType.STRING, nullable=True),
        ],
        mappings=[
            ColumnMapping(source_column="legacy_id", target_column="customer_id", transform="trim"),
            ColumnMapping(source_column="raw_name", target_column="customer_name", transform="trim"),
            ColumnMapping(source_column="raw_postal", target_column="postal_code", transform="trim"),
        ],
        extra_columns=ExtraColumnsPolicy.IGNORE,
        quality_rules=make_test_rules(),
    )

    raw_bronze = [
        {
            "legacy_id": "  C001  ",
            "raw_name": "   Ada Lovelace   ",
            "raw_postal": "  00123  ",
            "unmapped_extra_field": "confidential_source_metadata",
            LINEAGE_SOURCE_NAME: "crm_v1_legacy",
            LINEAGE_BATCH_ID: "bronze-batch-777",
            LINEAGE_INGESTION_TIMESTAMP: "2026-10-27T06:30:00Z",
        },
        {
            "legacy_id": "  C001  ",  # Duplicate key that gets transformed to 'C001'
            "raw_name": "   Ada Duplicate With Space   ",
            "raw_postal": "  00123  ",
            "unmapped_extra_field": "another_extra_payload",
            LINEAGE_SOURCE_NAME: "crm_v1_legacy",
            LINEAGE_BATCH_ID: "bronze-batch-777",
            LINEAGE_INGESTION_TIMESTAMP: "2026-10-27T06:30:00Z",
        },
    ]

    # Transform raw Bronze -> Silver candidate records
    transformer = SilverTransformer()
    candidate_records = transformer.transform(
        records=raw_bronze,
        silver_config=silver_cfg,
        batch_id="silver-run-888",
    )

    # Verify transformation modified column names and trimmed values
    assert candidate_records[0]["customer_id"] == "C001"
    assert candidate_records[0]["customer_name"] == "Ada Lovelace"
    assert "unmapped_extra_field" not in candidate_records[0]

    assert candidate_records[1]["customer_id"] == "C001"
    assert candidate_records[1]["customer_name"] == "Ada Duplicate With Space"
    assert "unmapped_extra_field" not in candidate_records[1]

    # Validate candidates with raw_records supplied
    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(
        records=candidate_records,
        rules=silver_cfg.quality_rules,
        raw_records=raw_bronze,
        batch_id="silver-run-888",
    )

    assert len(valid) == 1
    assert len(rejected) == 1

    quarantined = rejected[0]
    # In the candidate portion: transformed canonical name and trimmed string
    assert quarantined["customer_id"] == "C001"
    assert quarantined["customer_name"] == "Ada Duplicate With Space"
    assert quarantined[REJECTION_RULE] == "unique"

    # In the _raw_record portion: original source column names and untrimmed raw values
    raw_saved = quarantined[REJECTION_RAW_RECORD]
    assert raw_saved["legacy_id"] == "  C001  "
    assert raw_saved["raw_name"] == "   Ada Duplicate With Space   "
    assert raw_saved["raw_postal"] == "  00123  "
    assert raw_saved["unmapped_extra_field"] == "another_extra_payload"
    assert raw_saved[LINEAGE_SOURCE_NAME] == "crm_v1_legacy"
    assert raw_saved[LINEAGE_BATCH_ID] == "bronze-batch-777"
    assert raw_saved[LINEAGE_INGESTION_TIMESTAMP] == "2026-10-27T06:30:00Z"

    # Strict inequality proving raw values were preserved and not reconstructed from transformed values
    assert raw_saved["raw_name"] != quarantined["customer_name"]
    assert "legacy_id" in raw_saved and "legacy_id" not in quarantined


def test_critical_severity_raises_runtime_error() -> None:
    """Quality rule with CRITICAL severity aborts execution immediately with RuntimeError."""
    rules = [
        QualityRule(
            rule_name="not_null",
            columns=["customer_id"],
            severity=ValidationSeverity.CRITICAL,
        )
    ]
    records = [
        {"customer_id": "C001"},
        {"customer_id": None},
    ]

    validator = QualityValidator()
    with pytest.raises(RuntimeError, match="CRITICAL quality rule 'not_null' failed"):
        validator.validate(records, rules)


def test_empty_input_batch() -> None:
    """Empty record list returns empty valid/rejected lists and clean summaries."""
    validator = QualityValidator()
    valid, rejected, summaries = validator.validate([], make_test_rules())

    assert valid == []
    assert rejected == []
    assert len(summaries) == 3
    assert all(s.passed for s in summaries)
    assert all(s.failed_record_count == 0 for s in summaries)


def test_count_reconciliation_exact() -> None:
    """Count reconciliation: records_validated == len(valid_records) + len(rejected_records)."""
    validator = QualityValidator()
    rules = make_test_rules()

    # Case 1: All valid
    batch_1 = [
        {"customer_id": "C1", "postal_code": "00001"},
        {"customer_id": "C2", "postal_code": "00002"},
    ]
    v1, r1, _ = validator.validate(batch_1, rules)
    assert len(batch_1) == len(v1) + len(r1)
    assert len(v1) == 2 and len(r1) == 0

    # Case 2: All rejected (missing IDs)
    batch_2 = [
        {"customer_id": "", "postal_code": "00001"},
        {"customer_id": None, "postal_code": "00002"},
    ]
    v2, r2, _ = validator.validate(batch_2, rules)
    assert len(batch_2) == len(v2) + len(r2)
    assert len(v2) == 0 and len(r2) == 2

    # Case 3: Mixed batch with duplicates, missing fields, and regex warnings
    batch_3 = [
        {"customer_id": "C1", "postal_code": "00001"},  # Valid
        {"customer_id": "C2", "postal_code": "BADZIP"}, # Valid (warn only)
        {"customer_id": "C1", "postal_code": "00001"},  # Rejected (duplicate)
        {"customer_id": None, "postal_code": "00003"},  # Rejected (not_null)
        {"customer_id": "C3", "postal_code": "00004"},  # Valid
    ]
    v3, r3, _ = validator.validate(batch_3, rules)
    assert len(batch_3) == len(v3) + len(r3)
    assert len(v3) == 3
    assert len(r3) == 2

    # Case 4: Empty batch
    v4, r4, _ = validator.validate([], rules)
    assert len([]) == len(v4) + len(r4)


def test_end_to_end_with_silver_transformer_and_real_config() -> None:
    """End-to-end integration: SilverTransformer transforms Bronze -> QualityValidator validates candidate."""
    pipeline_cfg = load_pipeline_config("config/dataset_customers_pipeline.json")
    silver_cfg = pipeline_cfg.silver
    assert silver_cfg is not None

    raw_bronze = [
        {
            "customer_id": "C001",
            "customer_name": "  Ada Lovelace  ",
            "postal_code": "00123",
            LINEAGE_SOURCE_NAME: "crm_customers",
            LINEAGE_BATCH_ID: "b-001",
            LINEAGE_INGESTION_TIMESTAMP: "2026-10-27T08:00:00Z",
        },
        {
            "customer_id": "C002",
            "customer_name": "  Grace Hopper  ",
            "postal_code": "99999",
            LINEAGE_SOURCE_NAME: "crm_customers",
            LINEAGE_BATCH_ID: "b-001",
            LINEAGE_INGESTION_TIMESTAMP: "2026-10-27T08:00:00Z",
        },
        {
            "customer_id": "C001",  # Duplicate customer_id
            "customer_name": "  Ada Lovelace II ",
            "postal_code": "00123",
            LINEAGE_SOURCE_NAME: "crm_customers",
            LINEAGE_BATCH_ID: "b-001",
            LINEAGE_INGESTION_TIMESTAMP: "2026-10-27T08:00:00Z",
        },
    ]

    transformer = SilverTransformer()
    candidate_records = transformer.transform(
        records=raw_bronze,
        silver_config=silver_cfg,
        batch_id="silver-batch-999",
    )
    assert len(candidate_records) == 3

    validator = QualityValidator()
    valid_records, rejected_records, summaries = validator.validate(
        records=candidate_records,
        rules=silver_cfg.quality_rules,
        raw_records=raw_bronze,
        batch_id="silver-batch-999",
    )

    # Reconciliation check
    assert len(candidate_records) == len(valid_records) + len(rejected_records)
    assert len(valid_records) == 2
    assert len(rejected_records) == 1

    # Accepted records check
    assert valid_records[0]["customer_id"] == "C001"
    assert valid_records[0]["customer_name"] == "Ada Lovelace"
    assert valid_records[0]["postal_code"] == "00123"
    assert valid_records[0][LINEAGE_SILVER_BATCH_ID] == "silver-batch-999"
    assert valid_records[0][LINEAGE_RECORD_HASH]
    assert REJECTION_REASON not in valid_records[0]

    assert valid_records[1]["customer_id"] == "C002"
    assert valid_records[1]["customer_name"] == "Grace Hopper"

    # Quarantined duplicate check
    rej = rejected_records[0]
    assert rej["customer_id"] == "C001"
    assert rej[REJECTION_RULE] == "unique"
    assert "Duplicate value 'C001' in column(s) customer_id" in rej[REJECTION_REASON]
    assert rej[REJECTION_BATCH_ID] == "silver-batch-999"
    assert rej[REJECTION_RAW_RECORD]["customer_name"] == "  Ada Lovelace II "
    assert rej[REJECTION_RAW_RECORD][LINEAGE_SOURCE_NAME] == "crm_customers"


def test_phase3_strict_transformer_behavior_preserved() -> None:
    """Decision C: SilverTransformer continues to raise ValueError on non-nullable nulls or bad casts."""
    transformer = SilverTransformer()
    config = SilverConfig(
        target_name="test_target",
        silver_path=Path("dummy_silver.jsonl"),
        columns=[ColumnSchema("req_col", FieldType.STRING, nullable=False)],
    )

    # Must raise ValueError, preserving Phase 3 contract
    with pytest.raises(ValueError, match="non-nullable column received None"):
        transformer.transform([{"req_col": None}], config, batch_id="b1")

    # Invalid int cast must raise ValueError
    int_cfg = SilverConfig(
        target_name="test_target",
        silver_path=Path("dummy_silver.jsonl"),
        columns=[ColumnSchema("num", FieldType.INTEGER, nullable=True)],
    )
    with pytest.raises(ValueError, match="cannot cast string 'not_an_int' to integer"):
        transformer.transform([{"num": "not_an_int"}], int_cfg, batch_id="b1")


def test_range_rule_supported() -> None:
    """Verify extensible 'range' rule evaluates min and max constraints."""
    rules = [
        QualityRule(
            rule_name="range",
            columns=["score"],
            severity=ValidationSeverity.REJECT,
            params={"min": 0, "max": 100},
        )
    ]
    records = [
        {"score": 50},
        {"score": -5},
        {"score": 105},
    ]

    validator = QualityValidator()
    valid, rejected, summaries = validator.validate(records, rules)

    assert len(valid) == 1
    assert valid[0]["score"] == 50
    assert len(rejected) == 2
    assert "min 0" in rejected[0][REJECTION_REASON]
    assert "max 100" in rejected[1][REJECTION_REASON]
