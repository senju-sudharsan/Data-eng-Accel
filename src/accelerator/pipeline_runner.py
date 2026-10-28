"""Pipeline runner module for end-to-end Bronze and Silver data pipelines.

Coordinates:
- Configuration loading and fingerprinting.
- Batch lifecycle and submission-key idempotency.
- Bronze landing and atomic staging.
- Silver transformation with strict exception preservation.
- Quality rule validation and quarantine partitioning.
- Multi-output atomic publication with crash-safe recovery.
- Record count reconciliation and audit persistence.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from accelerator.bronze_ingestion import (
    BatchTracker,
    BronzeReader,
    LocalBronzeStore,
    _all_destinations_have_batch,
    _count_records_for_batch,
    _destination_has_batch,
    _destination_lock,
    _reconcile_committed_batch,
    read_source,
)
from accelerator.contracts import (
    BatchAuditSummary,
    BatchStatus,
    PipelineConfig,
    ReconciliationStatus,
    load_pipeline_config,
)
from accelerator.quality_validator import QualityValidator
from accelerator.scd2_processor import Scd2Processor
from accelerator.silver_transformations import SilverTransformer


def _pipeline_fingerprint(config: PipelineConfig) -> str:
    """Calculate deterministic fingerprint of source and destination specifications."""
    silver_path_str = str(config.silver.silver_path.resolve()) if config.silver else ""
    rejected_path_str = (
        str(config.silver.rejected_path.resolve())
        if config.silver and config.silver.rejected_path
        else ""
    )
    details = (
        f"{config.source_name}\0"
        f"{config.format}\0"
        f"{config.input_path.resolve()}\0"
        f"{config.bronze_path.resolve()}\0"
        f"{silver_path_str}\0"
        f"{rejected_path_str}"
    )
    return hashlib.sha256(details.encode("utf-8")).hexdigest()


def run_pipeline(
    config_path: str | Path | PipelineConfig,
    submission_key: str | None = None,
) -> BatchAuditSummary:
    """Execute an end-to-end Bronze–Silver data pipeline.

    Parameters
    ----------
    config_path:
        Path to JSON pipeline configuration file.
    submission_key:
        Optional client-supplied identifier for idempotent execution and deduplication.

    Returns
    -------
    BatchAuditSummary:
        Detailed execution and reconciliation metrics for the batch.
    """
    if isinstance(config_path, PipelineConfig):
        config = config_path
    else:
        config = load_pipeline_config(config_path)
    timestamp = datetime.now(timezone.utc).isoformat()
    tracker = BatchTracker(config.metadata_store)
    fingerprint = _pipeline_fingerprint(config)
    lease_token = str(uuid.uuid4())

    silver_path = Path(config.silver.silver_path) if config.silver else None
    rejected_path = Path(config.silver.rejected_path) if config.silver and config.silver.rejected_path else None

    # Handle submission key idempotency and lease management
    if submission_key is not None:
        if not isinstance(submission_key, str) or not submission_key.strip():
            raise ValueError("submission_key must be a non-empty string when provided")
        submission_key = submission_key.strip()
        existing = tracker.find_submission(submission_key)

        if existing:
            if existing["submission_fingerprint"] != fingerprint:
                raise ValueError(
                    "Submission key is already associated with incompatible source or destination details"
                )

            # Check if previous execution already completed successfully
            if existing["status"] in ("SUCCESS", "PARTIAL_SUCCESS"):
                if _all_destinations_have_batch(existing):
                    summaries = tracker.get_audit_summary(batch_id=existing["batch_id"])
                    if summaries:
                        return dataclasses.replace(summaries[0], idempotent_replay=True)

            # Check if execution was committed before crash/failure (resilient recovery)
            with _destination_lock(config.bronze_path):
                if _reconcile_committed_batch(tracker, existing):
                    summaries = tracker.get_audit_summary(batch_id=existing["batch_id"])
                    if summaries:
                        return dataclasses.replace(summaries[0], idempotent_replay=True)

            if existing["status"] == "RUNNING" and tracker.lease_is_active(existing):
                raise RuntimeError("Submission key is currently owned by an active ingestion")

            if not tracker.restart(
                existing["batch_id"],
                lease_token,
                silver_path=silver_path,
                rejected_path=rejected_path,
                stage="BRONZE",
                expected_lease_token=existing["lease_token"],
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
                silver_path=silver_path,
                rejected_path=rejected_path,
                stage="BRONZE",
            )
    else:
        batch_id = tracker.start(
            config,
            timestamp,
            None,
            fingerprint,
            lease_token,
            silver_path=silver_path,
            rejected_path=rejected_path,
            stage="BRONZE",
        )

    # Execute pipeline stages
    try:
        # Phase 1: Bronze Landing
        records: list[dict[str, Any]] = []
        store = LocalBronzeStore()

        with _destination_lock(config.bronze_path):
            if _destination_has_batch(config.bronze_path, batch_id):
                # Re-using already committed Bronze records from prior partial run
                records = list(BronzeReader().read(config.bronze_path, batch_id=batch_id))
                bronze_count = len(records)
                tracker.mark_committed(batch_id)
                tracker.set_commit_state(batch_id, "BRONZE_COMMITTED")
            else:
                for record in read_source(config):
                    records.append({
                        **record,
                        "_ingestion_source_name": config.source_name,
                        "_ingestion_batch_id": batch_id,
                        "_ingestion_timestamp": timestamp,
                    })
                staged, bronze_count = store.stage(config.bronze_path, batch_id, records)
                tracker.prepare(batch_id, bronze_count, staged)
                store.commit(staged, config.bronze_path)
                tracker.mark_committed(batch_id)
                tracker.set_commit_state(batch_id, "BRONZE_COMMITTED")

        # Phase 2: If Bronze-only pipeline, finalize and return
        if config.silver is None:
            tracker.finish(
                batch_id,
                BatchStatus.SUCCESS,
                records_processed=bronze_count,
                finished_at=datetime.now(timezone.utc).isoformat(),
                records_persisted=bronze_count,
                stage="BRONZE",
            )
            summaries = tracker.get_audit_summary(batch_id=batch_id)
            return summaries[0]

        # Phase 3: Silver Transformation
        tracker.update_stage(batch_id, "SILVER")
        transformer = SilverTransformer()
        try:
            candidate_records = transformer.transform(
                records=records,
                silver_config=config.silver,
                batch_id=batch_id,
            )
        except Exception as exc:
            # Strictly preserve exception and record batch-level failure without fabricated record counts
            err_msg = f"{type(exc).__name__}: {exc}"
            tracker.finish(
                batch_id,
                BatchStatus.FAILED,
                records_processed=bronze_count,
                finished_at=datetime.now(timezone.utc).isoformat(),
                records_persisted=bronze_count,
                error_message=err_msg,
                records_silver=0,
                records_rejected=0,
                records_failed=None,
                stage="SILVER",
                reconciliation_status=ReconciliationStatus.FAILED,
                silver_path=silver_path,
                rejected_path=rejected_path,
            )
            raise

        # Phase 4: Quality Validation
        tracker.update_stage(batch_id, "QUALITY_VALIDATION")
        validator = QualityValidator()
        try:
            valid_silver, quarantined, check_summaries = validator.validate(
                records=candidate_records,
                rules=config.silver.quality_rules,
                raw_records=records,
                batch_id=batch_id,
            )
        except Exception as exc:
            err_msg = f"{type(exc).__name__}: {exc}"
            tracker.finish(
                batch_id,
                BatchStatus.FAILED,
                records_processed=bronze_count,
                finished_at=datetime.now(timezone.utc).isoformat(),
                records_persisted=bronze_count,
                error_message=err_msg,
                records_silver=0,
                records_rejected=0,
                records_failed=None,
                stage="QUALITY_VALIDATION",
                reconciliation_status=ReconciliationStatus.FAILED,
                silver_path=silver_path,
                rejected_path=rejected_path,
            )
            raise

        # Phase 5: Publication (SCD2 or Conformed Silver) & Quarantine
        records_silver = len(valid_silver)
        records_rejected = len(quarantined)
        tracker.prepare_outputs(
            batch_id=batch_id,
            records_silver=records_silver,
            records_rejected=records_rejected,
            stage="PUBLICATION",
            commit_state="OUTPUTS_STAGED",
            records_scd2_changed=0 if config.silver.scd2 and config.silver.scd2.enabled else None,
        )
        records_scd2_changed: int | None = None

        staged_silver = silver_path.with_name(f".{silver_path.name}.{batch_id}.staged")
        staged_quarantine = (
            rejected_path.with_name(f".{rejected_path.name}.{batch_id}.staged")
            if (rejected_path and quarantined)
            else None
        )

        try:
            if config.silver.scd2 and config.silver.scd2.enabled:
                # Apply SCD2 when configured
                tracker.update_stage(batch_id, "SCD2")
                scd2_proc = Scd2Processor()
                with _destination_lock(silver_path):
                    scd2_proc.apply_scd2(
                        incoming_records=valid_silver,
                        target_path=silver_path,
                        scd2_config=config.silver.scd2,
                        batch_id=batch_id,
                    )
                # This count is intentionally distinct from records_silver:
                # unchanged valid rows produce no physical SCD2 version.
                records_scd2_changed = _count_records_for_batch(silver_path, batch_id)
                tracker.set_scd2_changed(batch_id, records_scd2_changed)
            else:
                # Conformed Silver publication: batch-aware to prevent duplicates on replay
                silver_already_has_batch = (
                    _destination_has_batch(silver_path, batch_id) if silver_path.exists() else False
                )
                if silver_already_has_batch:
                    existing_silver_count = _count_records_for_batch(silver_path, batch_id)
                    if existing_silver_count != len(valid_silver):
                        # Partial or corrupted batch records exist in silver_path - clean repair
                        with _destination_lock(silver_path):
                            silver_path.parent.mkdir(parents=True, exist_ok=True)
                            with staged_silver.open("w", encoding="utf-8", newline="\n") as sf:
                                with silver_path.open("r", encoding="utf-8") as existing_f:
                                    for line in existing_f:
                                        if not line.strip():
                                            continue
                                        try:
                                            rec = json.loads(line)
                                            if rec.get("_silver_batch_id") == batch_id:
                                                continue
                                            sf.write(line.strip() + "\n")
                                        except json.JSONDecodeError:
                                            continue
                                for r in valid_silver:
                                    sf.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
                                sf.flush()
                                os.fsync(sf.fileno())
                            os.replace(staged_silver, silver_path)
                    # If existing_silver_count == len(valid_silver), publication is already complete!
                elif valid_silver or not silver_path.exists():
                    with _destination_lock(silver_path):
                        silver_path.parent.mkdir(parents=True, exist_ok=True)
                        with staged_silver.open("w", encoding="utf-8", newline="\n") as sf:
                            if silver_path.exists():
                                sf.write(silver_path.read_text(encoding="utf-8"))
                            for r in valid_silver:
                                sf.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
                            sf.flush()
                            os.fsync(sf.fileno())
                        os.replace(staged_silver, silver_path)

            # Quarantine publication: batch-aware to prevent duplicates on replay
            if rejected_path and rejected_path.exists():
                quarantine_already_has_batch = _destination_has_batch(rejected_path, batch_id)
                if quarantine_already_has_batch:
                    existing_q_count = _count_records_for_batch(rejected_path, batch_id)
                    if existing_q_count != len(quarantined):
                        # Partial or corrupted quarantine output - repair
                        staged_q = staged_quarantine or rejected_path.with_name(f".{rejected_path.name}.{batch_id}.staged")
                        with _destination_lock(rejected_path):
                            rejected_path.parent.mkdir(parents=True, exist_ok=True)
                            with staged_q.open("w", encoding="utf-8", newline="\n") as qf:
                                with rejected_path.open("r", encoding="utf-8") as existing_f:
                                    for line in existing_f:
                                        if not line.strip():
                                            continue
                                        try:
                                            rec = json.loads(line)
                                            if rec.get("_rejection_batch_id") == batch_id:
                                                continue
                                            qf.write(line.strip() + "\n")
                                        except json.JSONDecodeError:
                                            continue
                                for r in quarantined:
                                    qf.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
                                qf.flush()
                                os.fsync(qf.fileno())
                            os.replace(staged_q, rejected_path)
            if staged_quarantine and rejected_path and quarantined:
                if not (rejected_path.exists() and _destination_has_batch(rejected_path, batch_id)):
                    with _destination_lock(rejected_path):
                        rejected_path.parent.mkdir(parents=True, exist_ok=True)
                        with staged_quarantine.open("w", encoding="utf-8", newline="\n") as qf:
                            if rejected_path.exists():
                                qf.write(rejected_path.read_text(encoding="utf-8"))
                            for r in quarantined:
                                qf.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
                            qf.flush()
                            os.fsync(qf.fileno())
                        os.replace(staged_quarantine, rejected_path)

            tracker.set_commit_state(batch_id, "COMMITTED")

        except Exception as exc:
            # Cleanup uncommitted staging files on failure
            if staged_silver and staged_silver.exists():
                try:
                    staged_silver.unlink()
                except OSError:
                    pass
            if staged_quarantine and staged_quarantine.exists():
                try:
                    staged_quarantine.unlink()
                except OSError:
                    pass

            err_msg = f"{type(exc).__name__}: {exc}"
            tracker.finish(
                batch_id,
                BatchStatus.FAILED,
                records_processed=bronze_count,
                finished_at=datetime.now(timezone.utc).isoformat(),
                records_persisted=bronze_count,
                error_message=err_msg,
                records_silver=records_silver,
                records_scd2_changed=records_scd2_changed,
                records_rejected=records_rejected,
                records_failed=None,
                stage="PUBLICATION",
                reconciliation_status=ReconciliationStatus.FAILED,
                silver_path=silver_path,
                rejected_path=rejected_path,
            )
            raise

        # Phase 6: Final Audit Reconciliation
        final_status = (
            BatchStatus.PARTIAL_SUCCESS if records_rejected > 0 else BatchStatus.SUCCESS
        )

        tracker.finish(
            batch_id,
            final_status,
            records_processed=bronze_count,
            finished_at=datetime.now(timezone.utc).isoformat(),
            records_persisted=bronze_count,
            records_silver=records_silver,
            records_scd2_changed=records_scd2_changed,
            records_rejected=records_rejected,
            records_failed=0,
            stage="COMPLETED",
            silver_path=silver_path,
            rejected_path=rejected_path,
        )

        summaries = tracker.get_audit_summary(batch_id=batch_id)
        return summaries[0]

    except Exception as exc:
        batch_info = tracker.find_batch(batch_id)
        if batch_info and batch_info["status"] == "RUNNING":
            with _destination_lock(config.bronze_path):
                if _reconcile_committed_batch(tracker, batch_info):
                    raise
            err_msg = f"{type(exc).__name__}: {exc}"
            try:
                tracker.finish(
                    batch_id,
                    BatchStatus.FAILED,
                    records_processed=len(records) if "records" in locals() else 0,
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    records_persisted=0,
                    error_message=err_msg,
                    records_failed=None,
                    stage="INGESTION",
                    reconciliation_status=ReconciliationStatus.FAILED,
                )
            except Exception:
                pass
        raise
