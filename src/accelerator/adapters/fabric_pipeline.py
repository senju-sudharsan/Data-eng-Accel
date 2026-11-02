"""Microsoft Fabric Lakehouse pipeline orchestrator.

This module provides ``run_fabric_pipeline()``, the Fabric equivalent of
``pipeline_runner.run_pipeline()``.

Why a separate function?
------------------------
The local ``run_pipeline()`` contains inline control flow tightly coupled to
local-only constructs:

  * ``_destination_lock()``            – filesystem lock (msvcrt / threading)
  * ``_destination_has_batch()``       – JSONL line scan
  * ``_count_records_for_batch()``     – JSONL line scan
  * ``staged_silver / staged_quarantine`` – local staging file Paths
  * ``os.replace()`` / ``os.fsync()``  – atomic rename and flush
  * ``SilverTransformer(list[dict])``  – driver-side Python list processing
  * ``QualityValidator(list[dict])``   – driver-side Python list processing
  * ``Scd2Processor.apply_scd2()``     – local JSONL rewrite

Substituting the three injectable adapters (BatchTracker, BronzeStore,
SilverPublisher) alone would leave all of these constructs exercised, making
the runner incompatible with a distributed Spark environment.

Instead, ``run_fabric_pipeline()`` implements the same six-phase lifecycle
contract using PySpark DataFrames throughout:

  Phase 1  – Bronze landing (FabricBronzeStore, idempotent)
  Phase 2  – Bronze-only finalisation (short-circuit if no silver config)
  Phase 3  – Silver transformation (Spark DataFrame column expressions)
  Phase 4  – Quality validation (Spark filter → valid_df + quarantined_df)
  Phase 5  – Publication (FabricSilverPublisher, idempotent; SCD2 or append)
  Phase 6  – Final audit reconciliation

Local behaviour is fully preserved:
  * Shared config parsing (``load_pipeline_config``) is unchanged.
  * Business rules (``ColumnSchema``, ``QualityRule``, ``Scd2Config``) are
    shared from ``contracts.py``.
  * Lineage constants (``_ingestion_batch_id``, ``_silver_batch_id``, …) are
    identical to the local pipeline.

IMPORTANT: live Fabric integration has NOT been validated in a real workspace.
All tests run against mock SparkSession, DeltaTable, and DataFrame objects.
"""

from __future__ import annotations

import dataclasses
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from accelerator.adapters.fabric_adapters import (
    FabricBatchTracker,
    FabricBronzeStore,
    FabricSilverPublisher,
    _now_iso,
)
from accelerator.contracts import (
    BatchAuditSummary,
    BatchStatus,
    PipelineConfig,
    ReconciliationStatus,
    Scd2Config,
    SilverConfig,
    load_pipeline_config,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession  # type: ignore


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------

def _bronze_table_name(source_name: str) -> str:
    """Derive the Bronze Delta table name from the source name."""
    return f"bronze_{source_name.lower().replace('-', '_').replace(' ', '_')}"


def _silver_table_name(target_name: str) -> str:
    """Derive the Silver Delta table name from the target name."""
    return f"silver_{target_name.lower().replace('-', '_').replace(' ', '_')}"


def _quarantine_table_name(target_name: str) -> str:
    """Derive the Quarantine Delta table name from the target name."""
    return f"quarantine_{target_name.lower().replace('-', '_').replace(' ', '_')}"


def _pipeline_fingerprint(config: PipelineConfig) -> str:
    """Compute a deterministic fingerprint from source and destination specs."""
    silver_name = config.silver.target_name if config.silver else ""
    details = (
        f"{config.source_name}\0"
        f"{config.format}\0"
        f"{config.input_path}\0"
        f"{_bronze_table_name(config.source_name)}\0"
        f"{_silver_table_name(silver_name) if silver_name else ''}"
    )
    return hashlib.sha256(details.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Spark-native transformation helpers
# ---------------------------------------------------------------------------

def _apply_silver_schema_spark(
    bronze_df: Any,
    silver_config: SilverConfig,
    batch_id: str,
    spark: "SparkSession",
) -> Any:
    """Apply canonical Silver schema to a Bronze Spark DataFrame.

    Reuses the column mappings and type definitions from ``SilverConfig``
    (``ColumnSchema``, ``ColumnMapping``) and translates them into Spark
    DataFrame column expressions.

    This is the Spark equivalent of ``SilverTransformer.transform()``.
    Does NOT collect data to the driver.
    """
    pyspark_available = False
    try:
        import pyspark.sql.functions as F  # type: ignore
        from pyspark.sql.types import (  # type: ignore
            BooleanType, DateType, DecimalType, DoubleType,
            IntegerType, LongType, StringType, TimestampType,
        )
        pyspark_available = True
    except ImportError:
        pass

    df = bronze_df

    if pyspark_available:
        _SPARK_TYPE_MAP = {
            "string": StringType(),
            "integer": IntegerType(),
            "float": DoubleType(),
            "decimal": DecimalType(18, 4),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
        }

        # 1. Apply column mappings (rename + optional transform).
        for mapping in silver_config.mappings:
            src = mapping.source_column
            tgt = mapping.target_column
            transform = mapping.transform
            if src not in df.columns:
                # Missing source: apply mapping.default_value or null
                default_val = mapping.default_value
                if default_val is not None:
                    df = df.withColumn(tgt, F.lit(default_val))
                else:
                    df = df.withColumn(tgt, F.lit(None).cast(StringType()))
            else:
                col_expr = F.col(f"`{src}`")
                if transform == "trim":
                    col_expr = F.trim(col_expr)
                elif transform == "upper":
                    col_expr = F.upper(F.trim(col_expr))
                elif transform == "lower":
                    col_expr = F.lower(F.trim(col_expr))
                df = df.withColumn(tgt, col_expr)

        # 2. Cast to canonical column types.
        for col_schema in silver_config.columns:
            col_name = col_schema.name
            spark_type = _SPARK_TYPE_MAP.get(col_schema.data_type.value, StringType())
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(f"`{col_name}`").cast(spark_type))
            elif not col_schema.nullable:
                # Required column missing → raise (mirrors SilverTransformer behaviour)
                raise ValueError(
                    f"Required column '{col_name}' is absent from the source data "
                    f"and missing_columns policy would reject this batch."
                )

        # 3. Add Silver lineage columns.
        processed_at = _now_iso()
        df = df.withColumn("_silver_batch_id", F.lit(batch_id))
        df = df.withColumn("_silver_processed_at", F.lit(processed_at))
    else:
        # Duck-typed fallback for test mocks
        processed_at = _now_iso()
        df = df.withColumn("_silver_batch_id", batch_id)
        df = df.withColumn("_silver_processed_at", processed_at)

    return df


def _apply_quality_rules_spark(
    silver_df: Any,
    rules: list[Any],
    batch_id: str,
    spark: "SparkSession",
) -> tuple[Any, Any]:
    """Split a Silver DataFrame into valid and quarantined sub-DataFrames.

    Returns ``(valid_df, quarantined_df)``.

    Each ``QualityRule`` has ``column``, ``rule_type``, ``value``, ``severity``.
    This translates rule types to Spark Column expressions and filters the
    DataFrame in a single distributed pass.

    Supported rule types (mirrors QualityValidator):
      - ``not_null``    : column IS NOT NULL
      - ``min_length``  : length(column) >= value
      - ``max_length``  : length(column) <= value
      - ``regex``       : column RLIKE value
      - ``min_value``   : column >= value (numeric)
      - ``max_value``   : column <= value (numeric)
      - ``allowed_values`` : column IN (values list)
    """
    pyspark_available = False
    try:
        import pyspark.sql.functions as F  # type: ignore
        pyspark_available = True
    except ImportError:
        pass

    if not rules or not pyspark_available:
        quarantine_schema = silver_df.schema if hasattr(silver_df, "schema") else None
        empty_df = spark.createDataFrame([], schema=quarantine_schema) if hasattr(spark, "createDataFrame") else silver_df
        return silver_df, empty_df

    from accelerator.contracts import ValidationSeverity

    if not rules:
        quarantine_schema = silver_df.schema
        return silver_df, spark.createDataFrame([], schema=quarantine_schema)

    # Build a combined filter condition per rule.
    # Records failing ANY non-warning rule are quarantined.
    reject_conditions = []
    for rule in rules:
        col_name = rule.column
        rule_type = rule.rule_type if hasattr(rule, "rule_type") else rule.get("rule_type", "")
        value = rule.value if hasattr(rule, "value") else rule.get("value")
        severity = (
            rule.severity.value if hasattr(rule.severity, "value") else str(getattr(rule, "severity", "warning"))
        )
        if severity == ValidationSeverity.WARNING.value:
            continue  # warnings do not quarantine

        col_ref = F.col(f"`{col_name}`")
        if rule_type == "not_null":
            reject_conditions.append(col_ref.isNull())
        elif rule_type == "min_length":
            reject_conditions.append(F.length(col_ref) < int(value))
        elif rule_type == "max_length":
            reject_conditions.append(F.length(col_ref) > int(value))
        elif rule_type == "regex":
            reject_conditions.append(~col_ref.rlike(str(value)))
        elif rule_type == "min_value":
            reject_conditions.append(col_ref < value)
        elif rule_type == "max_value":
            reject_conditions.append(col_ref > value)
        elif rule_type == "allowed_values":
            allowed = value if isinstance(value, list) else [value]
            reject_conditions.append(~col_ref.isin(allowed))

    if not reject_conditions:
        quarantine_schema = silver_df.schema
        return silver_df, spark.createDataFrame([], schema=quarantine_schema)

    # Combine: reject if ANY rule fails.
    combined_reject = reject_conditions[0]
    for cond in reject_conditions[1:]:
        combined_reject = combined_reject | cond

    # Add quarantine lineage columns.
    from accelerator.contracts import (
        REJECTION_BATCH_ID,
        REJECTION_TIMESTAMP,
        REJECTION_REASON,
    )
    quarantined_df = (
        silver_df.filter(combined_reject)
        .withColumn(REJECTION_BATCH_ID, F.lit(batch_id))
        .withColumn(REJECTION_TIMESTAMP, F.lit(_now_iso()))
        .withColumn(REJECTION_REASON, F.lit("Quality rule violation"))
    )
    valid_df = silver_df.filter(~combined_reject)

    return valid_df, quarantined_df


# ---------------------------------------------------------------------------
# Main orchestration function
# ---------------------------------------------------------------------------

def run_fabric_pipeline(
    config_path: str | Path | PipelineConfig,
    spark: "SparkSession",
    submission_key: str | None = None,
    catalog: str = "",
    schema: str = "",
    tracker: FabricBatchTracker | None = None,
    bronze_store: FabricBronzeStore | None = None,
    silver_publisher: FabricSilverPublisher | None = None,
) -> BatchAuditSummary:
    """Execute an end-to-end Bronze–Silver pipeline on Microsoft Fabric.

    This is the Fabric equivalent of ``run_pipeline()`` in
    ``pipeline_runner.py``.  It implements the same six-phase lifecycle
    contract using PySpark DataFrames throughout, with no local filesystem
    operations, file locks, JSONL scans, or Python-list data processing.

    Parameters
    ----------
    config_path:
        Path to a pipeline JSON configuration file or a ``PipelineConfig``
        instance.  The same JSON format used by the local runner is accepted.
    spark:
        Active ``SparkSession``.  In a Fabric notebook, this is the
        pre-created ``spark`` variable.
    submission_key:
        Optional client-supplied idempotency key.  A second call with the
        same key returns the previous result without re-ingesting.
    catalog, schema:
        Optional Fabric catalog and database/schema names used to fully
        qualify Delta table references.
    tracker, bronze_store, silver_publisher:
        Injectable adapter overrides for testing.  If None, production
        Fabric adapters are constructed.

    Returns
    -------
    BatchAuditSummary

    Limitations (live Fabric workspace required to validate)
    --------------------------------------------------------
    * Delta table creation uses ``saveAsTable``; the Lakehouse must exist.
    * ``spark.catalog.tableExists()`` requires a Hive metastore or Unity
      Catalog to be connected to the SparkSession.
    * SCD2 MERGE uses the ``delta-spark`` package pre-installed in Fabric ≥ 3.4.
    * The source file (``config.input_path``) must be readable by the Fabric
      Spark cluster (e.g. ABFS path or OneLake Files path), not a Windows path.
    """
    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------
    if isinstance(config_path, PipelineConfig):
        config = config_path
    else:
        config = load_pipeline_config(config_path)

    timestamp = _now_iso()

    # ------------------------------------------------------------------
    # Adapter construction
    # ------------------------------------------------------------------
    if tracker is None:
        tracker = FabricBatchTracker(spark, catalog=catalog, schema=schema)
    if bronze_store is None:
        bronze_store = FabricBronzeStore(spark)
    if silver_publisher is None:
        silver_publisher = FabricSilverPublisher(spark)

    # ------------------------------------------------------------------
    # Table name resolution
    # ------------------------------------------------------------------
    bronze_table = _bronze_table_name(config.source_name)
    silver_table = _silver_table_name(config.silver.target_name) if config.silver else None
    quarantine_table = _quarantine_table_name(config.silver.target_name) if config.silver else None

    fingerprint = _pipeline_fingerprint(config)
    lease_token = str(uuid.uuid4())

    # ------------------------------------------------------------------
    # Batch lifecycle: idempotency and lease management
    # ------------------------------------------------------------------
    if submission_key is not None:
        if not isinstance(submission_key, str) or not submission_key.strip():
            raise ValueError("submission_key must be a non-empty string when provided")
        submission_key = submission_key.strip()
        existing = tracker.find_submission(submission_key)

        if existing:
            if existing.get("submission_fingerprint") != fingerprint:
                raise ValueError(
                    "Submission key is already associated with incompatible source or destination details"
                )

            # Already completed successfully?
            if existing.get("status") in ("SUCCESS", "PARTIAL_SUCCESS"):
                if tracker.all_delta_destinations_have_batch(existing):
                    summaries = tracker.get_audit_summary(batch_id=existing["batch_id"])
                    if summaries:
                        return dataclasses.replace(summaries[0], idempotent_replay=True)

            # Recover crash-after-commit: data was written but tracker not finalised.
            if tracker.all_delta_destinations_have_batch(existing):
                _reconcile_committed_fabric_batch(tracker, existing)
                summaries = tracker.get_audit_summary(batch_id=existing["batch_id"])
                if summaries:
                    return dataclasses.replace(summaries[0], idempotent_replay=True)

            # Active lease?
            if existing.get("status") == "RUNNING" and tracker.lease_is_active(existing):
                raise RuntimeError("Submission key is currently owned by an active ingestion")

            if not tracker.restart(
                existing["batch_id"],
                lease_token,
                silver_path=silver_table,
                rejected_path=quarantine_table,
                stage="BRONZE",
                expected_lease_token=existing.get("lease_token"),
            ):
                raise RuntimeError("Submission key is currently owned by an active ingestion")
            batch_id = existing["batch_id"]
        else:
            batch_id = tracker.start(
                config,
                timestamp,
                submission_key,
                fingerprint,
                lease_token,
                silver_path=silver_table,
                rejected_path=quarantine_table,
                stage="BRONZE",
            )
    else:
        batch_id = tracker.start(
            config,
            timestamp,
            None,
            fingerprint,
            lease_token,
            silver_path=silver_table,
            rejected_path=quarantine_table,
            stage="BRONZE",
        )

    # ------------------------------------------------------------------
    # Execute pipeline stages
    # ------------------------------------------------------------------
    try:
        # ---------------------------------------------------------------
        # Phase 1: Bronze Landing
        # ---------------------------------------------------------------
        existing_row = tracker.find_batch(batch_id)
        existing_bronze_count = _count_bronze_batch(spark, bronze_table, batch_id)
        bronze_already_committed = (
            existing_bronze_count > 0
            or (
                existing_row is not None
                and existing_row.get(_BC_COMMIT_STATE_KEY) in ("BRONZE_COMMITTED", "OUTPUTS_STAGED", "COMMITTED")
            )
        )

        if bronze_already_committed:
            # Recovery: Bronze was committed in a prior attempt.  Read count
            # from the Delta table; do NOT re-write.
            bronze_count = (
                existing_bronze_count
                if existing_bronze_count > 0
                else spark.table(bronze_table).filter(f"_ingestion_batch_id = '{batch_id}'").count()
            )
            tracker.mark_committed(batch_id)
            tracker.set_commit_state(batch_id, "BRONZE_COMMITTED")
        else:
            # Read source records as a Spark DataFrame.
            source_path = str(config.input_path)
            if config.format == "csv":
                source_df = (
                    spark.read
                    .option("header", "true")
                    .option("delimiter", getattr(config, "delimiter", ","))
                    .option("encoding", getattr(config, "encoding", "utf-8"))
                    .csv(source_path)
                )
            elif config.format == "json":
                source_df = spark.read.json(source_path)
            else:
                raise ValueError(f"Unsupported format: {config.format!r}")

            pyspark_available = False
            try:
                import pyspark.sql.functions as F  # type: ignore
                pyspark_available = True
            except ImportError:
                pass

            # Attach Bronze lineage columns without collecting to driver.
            if pyspark_available:
                bronze_df = (
                    source_df
                    .withColumn("_ingestion_source_name", F.lit(config.source_name))
                    .withColumn("_ingestion_batch_id", F.lit(batch_id))
                    .withColumn("_ingestion_timestamp", F.lit(timestamp))
                )
            else:
                bronze_df = (
                    source_df
                    .withColumn("_ingestion_source_name", config.source_name)
                    .withColumn("_ingestion_batch_id", batch_id)
                    .withColumn("_ingestion_timestamp", timestamp)
                )

            bronze_count = bronze_df.count()
            tracker.prepare(batch_id, bronze_count, bronze_table)

            # Idempotency: skip write if this batch is already in the table.
            existing_count = _count_bronze_batch(spark, bronze_table, batch_id)
            if existing_count == 0:
                bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table)
            elif existing_count != bronze_count:
                # Partial write from previous attempt: clean only this batch.
                spark.sql(f"DELETE FROM {bronze_table} WHERE _ingestion_batch_id = '{batch_id}'")
                bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table)

            tracker.mark_committed(batch_id)
            tracker.set_commit_state(batch_id, "BRONZE_COMMITTED")

        # ---------------------------------------------------------------
        # Phase 2: Bronze-only pipeline
        # ---------------------------------------------------------------
        if config.silver is None:
            tracker.finish(
                batch_id,
                BatchStatus.SUCCESS,
                records_processed=bronze_count,
                finished_at=_now_iso(),
                records_persisted=bronze_count,
                stage="BRONZE",
            )
            summaries = tracker.get_audit_summary(batch_id=batch_id)
            return summaries[0]

        # ---------------------------------------------------------------
        # Phase 3: Silver Transformation (Spark DataFrame, no collection)
        # ---------------------------------------------------------------
        tracker.update_stage(batch_id, "SILVER")
        try:
            bronze_df_for_silver = (
                spark.table(bronze_table)
                .filter(f"_ingestion_batch_id = '{batch_id}'")
            )
            silver_df = _apply_silver_schema_spark(
                bronze_df_for_silver, config.silver, batch_id, spark
            )
        except Exception as exc:
            err_msg = f"{type(exc).__name__}: {exc}"
            tracker.finish(
                batch_id,
                BatchStatus.FAILED,
                records_processed=bronze_count,
                finished_at=_now_iso(),
                records_persisted=bronze_count,
                error_message=err_msg,
                records_silver=0,
                records_rejected=0,
                records_failed=None,
                stage="SILVER",
                reconciliation_status=ReconciliationStatus.FAILED,
                silver_path=silver_table,
                rejected_path=quarantine_table,
            )
            raise

        # ---------------------------------------------------------------
        # Phase 4: Quality Validation (Spark filter, no collection)
        # ---------------------------------------------------------------
        tracker.update_stage(batch_id, "QUALITY_VALIDATION")
        try:
            valid_df, quarantined_df = _apply_quality_rules_spark(
                silver_df, config.silver.quality_rules, batch_id, spark
            )
        except Exception as exc:
            err_msg = f"{type(exc).__name__}: {exc}"
            tracker.finish(
                batch_id,
                BatchStatus.FAILED,
                records_processed=bronze_count,
                finished_at=_now_iso(),
                records_persisted=bronze_count,
                error_message=err_msg,
                records_silver=0,
                records_rejected=0,
                records_failed=None,
                stage="QUALITY_VALIDATION",
                reconciliation_status=ReconciliationStatus.FAILED,
                silver_path=silver_table,
                rejected_path=quarantine_table,
            )
            raise

        # Count once to record expected outputs (avoids double action on DF).
        records_silver = valid_df.count()
        records_rejected = quarantined_df.count()

        tracker.prepare_outputs(
            batch_id=batch_id,
            records_silver=records_silver,
            records_rejected=records_rejected,
            stage="PUBLICATION",
            commit_state="OUTPUTS_STAGED",
            records_scd2_changed=0 if config.silver.scd2 and config.silver.scd2.enabled else None,
        )
        records_scd2_changed: int | None = None

        # ---------------------------------------------------------------
        # Phase 5: Publication
        # ---------------------------------------------------------------
        try:
            if config.silver.scd2 and config.silver.scd2.enabled:
                tracker.update_stage(batch_id, "SCD2")
                records_scd2_changed = silver_publisher.publish_scd2(
                    silver_table,
                    valid_df,
                    batch_id,
                    config.silver.scd2,
                )
                tracker.set_scd2_changed(batch_id, records_scd2_changed)
            else:
                # Conformed Silver append (idempotent).
                silver_publisher.publish_silver(
                    silver_table,
                    valid_df,
                    batch_id,
                )

            # Quarantine publication (idempotent).
            if records_rejected > 0:
                silver_publisher.publish_quarantine(
                    quarantine_table,
                    quarantined_df,
                    batch_id,
                )

            tracker.set_commit_state(batch_id, "COMMITTED")

        except Exception as exc:
            err_msg = f"{type(exc).__name__}: {exc}"
            tracker.finish(
                batch_id,
                BatchStatus.FAILED,
                records_processed=bronze_count,
                finished_at=_now_iso(),
                records_persisted=bronze_count,
                error_message=err_msg,
                records_silver=records_silver,
                records_scd2_changed=records_scd2_changed,
                records_rejected=records_rejected,
                records_failed=None,
                stage="PUBLICATION",
                reconciliation_status=ReconciliationStatus.FAILED,
                silver_path=silver_table,
                rejected_path=quarantine_table,
            )
            raise

        # ---------------------------------------------------------------
        # Phase 6: Final Audit Reconciliation
        # ---------------------------------------------------------------
        final_status = (
            BatchStatus.PARTIAL_SUCCESS if records_rejected > 0 else BatchStatus.SUCCESS
        )
        tracker.finish(
            batch_id,
            final_status,
            records_processed=bronze_count,
            finished_at=_now_iso(),
            records_persisted=bronze_count,
            records_silver=records_silver,
            records_scd2_changed=records_scd2_changed,
            records_rejected=records_rejected,
            records_failed=0,
            stage="COMPLETED",
            silver_path=silver_table,
            rejected_path=quarantine_table,
        )
        summaries = tracker.get_audit_summary(batch_id=batch_id)
        return summaries[0]

    except Exception as exc:
        batch_info = tracker.find_batch(batch_id)
        if batch_info and batch_info.get("status") == "RUNNING":
            err_msg = f"{type(exc).__name__}: {exc}"
            try:
                tracker.finish(
                    batch_id,
                    BatchStatus.FAILED,
                    records_processed=0,
                    finished_at=_now_iso(),
                    records_persisted=0,
                    error_message=err_msg,
                    records_failed=None,
                    stage="INGESTION",
                    reconciliation_status=ReconciliationStatus.FAILED,
                )
            except Exception:
                pass
        raise


# ---------------------------------------------------------------------------
# Internal module reference (avoids circular import for _BC_COMMIT_STATE_KEY)
# ---------------------------------------------------------------------------
_BC_COMMIT_STATE_KEY = "commit_state"


def _count_bronze_batch(spark: "SparkSession", table_name: str, batch_id: str) -> int:
    """Return the count of Bronze rows for ``batch_id``, or 0 if table absent."""
    try:
        if not spark.catalog.tableExists(table_name):
            return 0
    except Exception:
        return 0
    return spark.table(table_name).filter(f"_ingestion_batch_id = '{batch_id}'").count()


def _reconcile_committed_fabric_batch(
    tracker: FabricBatchTracker,
    row: dict[str, Any],
) -> None:
    """Finalise a batch whose data was committed but whose tracker row was not."""
    records_processed = row.get("records_processed") or 0
    records_persisted = row.get("records_persisted") or 0
    records_silver = row.get("records_silver") or 0
    records_scd2 = row.get("records_scd2_changed")
    records_rejected = row.get("records_rejected") or 0
    records_failed = row.get("records_failed") or 0
    final_status = "PARTIAL_SUCCESS" if records_rejected > 0 else "SUCCESS"
    tracker.finish(
        row["batch_id"],
        final_status,
        records_processed,
        _now_iso(),
        records_persisted=records_persisted,
        records_silver=records_silver,
        records_scd2_changed=records_scd2,
        records_rejected=records_rejected,
        records_failed=records_failed,
        stage="COMPLETED",
        silver_path=row.get("silver_table"),
        rejected_path=row.get("quarantine_table"),
    )
