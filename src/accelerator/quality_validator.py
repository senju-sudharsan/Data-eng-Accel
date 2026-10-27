"""Quality validation module for Silver conformed records and quarantine handling.

Implements the QualityValidator component conforming to QualityValidatorProtocol:
- Evaluates configured QualityRule checks (not_null, unique, regex, range).
- Handles validation severities: WARN (audit warning), REJECT (quarantine), CRITICAL (abort batch).
- Decision A: First occurrence wins for unique rules; preserves source order.
- Decision B: Evaluates all applicable rules and captures all failures per record.
- Decision C: Preserves Phase 3 SilverTransformer behavior; operates on transformed candidates.
- Enriches quarantined records with _rejection_reason, _failed_rule, _rejected_at, _raw_record, _rejection_batch_id.
- Produces clean Silver records without quarantine metadata.
- Reconciles input, accepted, and rejected record counts.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from accelerator.contracts import (
    LINEAGE_BATCH_ID,
    LINEAGE_SILVER_BATCH_ID,
    REJECTION_BATCH_ID,
    REJECTION_RAW_RECORD,
    REJECTION_REASON,
    REJECTION_RULE,
    REJECTION_TIMESTAMP,
    QualityCheckResult,
    QualityRule,
    QualityValidatorProtocol,
    ValidationSeverity,
)

QUARANTINE_FIELDS = {
    REJECTION_TIMESTAMP,
    REJECTION_REASON,
    REJECTION_RULE,
    REJECTION_BATCH_ID,
    REJECTION_RAW_RECORD,
}


class QualityValidator:
    """Evaluates configurable data-quality rules against conformed records."""

    def validate(
        self,
        records: list[dict[str, Any]],
        rules: list[QualityRule],
        raw_records: list[dict[str, Any]] | None = None,
        batch_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[QualityCheckResult]]:
        """Validate candidate records against configured quality rules.

        Parameters
        ----------
        records:
            List of transformed candidate record dictionaries.
        rules:
            List of QualityRule objects to evaluate against the records.
        raw_records:
            Optional parallel list of original Bronze records for quarantine preservation.
        batch_id:
            Optional batch identifier to populate _rejection_batch_id on quarantined records.

        Returns
        -------
        tuple[list[dict[str, Any]], list[dict[str, Any]], list[QualityCheckResult]]:
            - valid_records: Clean conformed records passing all rejecting rules.
            - rejected_records: Quarantined records decorated with failure metadata and _raw_record.
            - check_summaries: List of QualityCheckResult summaries for all evaluated rules.
        """
        rejection_timestamp = datetime.now(timezone.utc).isoformat()
        if raw_records is not None and not isinstance(raw_records, list):
            raw_records = list(raw_records)

        if not records:
            summaries = [
                QualityCheckResult(
                    rule_name=rule.rule_name,
                    passed=True,
                    failed_record_count=0,
                    severity=self._normalize_severity(rule.severity),
                    details="No records to validate",
                )
                for rule in rules
            ]
            return [], [], summaries

        # Per-record failure tracking: record_idx -> list of (QualityRule, failure_message)
        record_failures: dict[int, list[tuple[QualityRule, str]]] = {
            i: [] for i in range(len(records))
        }

        # Per-rule failure counts: rule_index -> count of distinct failing records
        rule_failure_counts: dict[int, int] = {i: 0 for i in range(len(rules))}

        for rule_idx, rule in enumerate(rules):
            norm_severity = self._normalize_severity(rule.severity)
            failed_in_this_rule: list[tuple[int, str]] = self._evaluate_rule(rule, records)

            rule_failure_counts[rule_idx] = len(failed_in_this_rule)

            # Record failure messages against each failing record
            for rec_idx, msg in failed_in_this_rule:
                record_failures[rec_idx].append((rule, msg))

            # CRITICAL severity aborts execution immediately
            if norm_severity == ValidationSeverity.CRITICAL and failed_in_this_rule:
                first_rec_idx, first_msg = failed_in_this_rule[0]
                raise RuntimeError(
                    f"CRITICAL quality rule '{rule.rule_name}' failed on "
                    f"{len(failed_in_this_rule)} record(s). "
                    f"First breach (record {first_rec_idx}): {first_msg}"
                )

        # Build check summaries
        check_summaries: list[QualityCheckResult] = []
        for rule_idx, rule in enumerate(rules):
            failed_count = rule_failure_counts[rule_idx]
            norm_severity = self._normalize_severity(rule.severity)
            passed = failed_count == 0
            if passed:
                details = f"Rule '{rule.rule_name}' passed on all {len(records)} record(s)."
            else:
                details = (
                    f"Rule '{rule.rule_name}' [{norm_severity.value}] failed on "
                    f"{failed_count} of {len(records)} record(s)."
                )

            check_summaries.append(
                QualityCheckResult(
                    rule_name=rule.rule_name,
                    passed=passed,
                    failed_record_count=failed_count,
                    severity=norm_severity,
                    details=details,
                )
            )

        # Partition into accepted (valid) and rejected (quarantined)
        valid_records: list[dict[str, Any]] = []
        rejected_records: list[dict[str, Any]] = []

        for idx, record in enumerate(records):
            failures = record_failures[idx]
            rejecting_failures = [
                (r, msg)
                for r, msg in failures
                if self._normalize_severity(r.severity) == ValidationSeverity.REJECT
            ]

            if rejecting_failures:
                # Quarantined record: collect all applicable failure details
                # Deduplicate failed rule names preserving evaluation order
                failed_rules_ordered = list(
                    dict.fromkeys(r.rule_name for r, _ in failures)
                )
                failed_rule_str = ", ".join(failed_rules_ordered)
                rejection_reason_str = "; ".join(msg for _, msg in failures)

                # Determine rejection batch ID
                resolved_batch_id = (
                    batch_id
                    or record.get(LINEAGE_SILVER_BATCH_ID)
                    or record.get(LINEAGE_BATCH_ID)
                    or ""
                )

                # Preserve the original raw record from Bronze
                raw_record = self._extract_raw_record(idx, record, raw_records)

                quarantined_record = dict(record)
                quarantined_record[REJECTION_REASON] = rejection_reason_str
                quarantined_record[REJECTION_RULE] = failed_rule_str
                quarantined_record[REJECTION_TIMESTAMP] = rejection_timestamp
                quarantined_record[REJECTION_BATCH_ID] = str(resolved_batch_id)
                quarantined_record[REJECTION_RAW_RECORD] = raw_record

                rejected_records.append(quarantined_record)
            else:
                # Accepted record: clean Silver output without quarantine metadata
                clean_record = {
                    k: v for k, v in record.items() if k not in QUARANTINE_FIELDS
                }
                valid_records.append(clean_record)

        return valid_records, rejected_records, check_summaries

    def _evaluate_rule(
        self,
        rule: QualityRule,
        records: list[dict[str, Any]],
    ) -> list[tuple[int, str]]:
        """Evaluate a single rule across all records, returning (record_idx, reason) pairs."""
        rule_name = rule.rule_name.strip().lower()

        if rule_name == "not_null":
            return self._evaluate_not_null(rule, records)
        if rule_name == "unique":
            return self._evaluate_unique(rule, records)
        if rule_name == "regex":
            return self._evaluate_regex(rule, records)
        if rule_name == "range":
            return self._evaluate_range(rule, records)

        raise ValueError(f"Unsupported quality rule: '{rule.rule_name}'")

    def _evaluate_not_null(
        self,
        rule: QualityRule,
        records: list[dict[str, Any]],
    ) -> list[tuple[int, str]]:
        """Verify specified columns are non-null and non-empty."""
        failures: list[tuple[int, str]] = []
        for idx, rec in enumerate(records):
            col_failures: list[str] = []
            for col in rule.columns:
                val = rec.get(col)
                if val is None or (isinstance(val, str) and not val.strip()):
                    col_failures.append(f"Column '{col}' is null or empty")
            if col_failures:
                failures.append((idx, "; ".join(col_failures)))
        return failures

    def _evaluate_unique(
        self,
        rule: QualityRule,
        records: list[dict[str, Any]],
    ) -> list[tuple[int, str]]:
        """Verify uniqueness across the batch using first-occurrence-wins semantics.

        Preserves source order: the first occurrence of each distinct key is kept
        eligible for acceptance, while subsequent occurrences are quarantined.
        Missing or empty keys are bypassed so they are handled by not_null rather
        than falsely treated as valid duplicate keys.
        """
        failures: list[tuple[int, str]] = []
        seen_keys: set[tuple[Any, ...]] = set()

        for idx, rec in enumerate(records):
            key_values: list[Any] = []
            has_empty_key = False
            for col in rule.columns:
                val = rec.get(col)
                if val is None or (isinstance(val, str) and not val.strip()):
                    has_empty_key = True
                    break
                key_values.append(val)

            if has_empty_key:
                # Missing or empty keys are not considered valid keys for duplicate grouping
                continue

            key_tuple = tuple(key_values)
            if key_tuple in seen_keys:
                cols_str = ", ".join(rule.columns)
                val_str = ", ".join(str(v) for v in key_values)
                failures.append(
                    (idx, f"Duplicate value '{val_str}' in column(s) {cols_str}")
                )
            else:
                seen_keys.add(key_tuple)

        return failures

    def _evaluate_regex(
        self,
        rule: QualityRule,
        records: list[dict[str, Any]],
    ) -> list[tuple[int, str]]:
        """Verify column strings conform to configured regular expression pattern."""
        pattern_str = rule.params.get("pattern")
        if not pattern_str:
            raise ValueError(f"Quality rule '{rule.rule_name}' requires 'pattern' parameter")

        pattern = re.compile(pattern_str)
        failures: list[tuple[int, str]] = []

        for idx, rec in enumerate(records):
            col_failures: list[str] = []
            for col in rule.columns:
                val = rec.get(col)
                # Null or empty values in optional fields pass regex; not_null governs presence
                if val is not None and (not isinstance(val, str) or val.strip()):
                    str_val = str(val)
                    if not pattern.search(str_val):
                        col_failures.append(
                            f"Column '{col}' value '{str_val}' does not match pattern '{pattern_str}'"
                        )
            if col_failures:
                failures.append((idx, "; ".join(col_failures)))

        return failures

    def _evaluate_range(
        self,
        rule: QualityRule,
        records: list[dict[str, Any]],
    ) -> list[tuple[int, str]]:
        """Verify column values fall within optional min and max bounds."""
        min_val = rule.params.get("min")
        max_val = rule.params.get("max")
        failures: list[tuple[int, str]] = []

        for idx, rec in enumerate(records):
            col_failures: list[str] = []
            for col in rule.columns:
                val = rec.get(col)
                if val is not None:
                    try:
                        if min_val is not None and val < min_val:
                            col_failures.append(f"Column '{col}' value {val} < min {min_val}")
                        if max_val is not None and val > max_val:
                            col_failures.append(f"Column '{col}' value {val} > max {max_val}")
                    except TypeError:
                        col_failures.append(f"Column '{col}' value '{val}' cannot be compared for range")
            if col_failures:
                failures.append((idx, "; ".join(col_failures)))

        return failures

    @staticmethod
    def _normalize_severity(severity: ValidationSeverity | str) -> ValidationSeverity:
        """Ensure case-safe and type-safe ValidationSeverity representation."""
        if isinstance(severity, ValidationSeverity):
            return severity
        return ValidationSeverity(str(severity).strip().lower())

    @staticmethod
    def _extract_raw_record(
        idx: int,
        record: dict[str, Any],
        raw_records: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        """Extract the original Bronze raw record, preserving source values and lineage."""
        if raw_records is not None and idx < len(raw_records):
            return dict(raw_records[idx])
        if REJECTION_RAW_RECORD in record and isinstance(record[REJECTION_RAW_RECORD], dict):
            return dict(record[REJECTION_RAW_RECORD])
        # Fallback: retain non-quarantine fields from the candidate record
        return {k: v for k, v in record.items() if k not in QUARANTINE_FIELDS}
