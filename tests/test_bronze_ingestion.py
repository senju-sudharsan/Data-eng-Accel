import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from accelerator import bronze_ingestion
from accelerator.bronze_ingestion import BatchTracker, BronzeReader, ingest, load_config


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


def test_old_batch_schema_migrates_and_preserves_existing_history(tmp_path: Path) -> None:
    database = tmp_path / "metadata" / "batches.db"
    database.parent.mkdir()
    with sqlite3.connect(database) as connection:
        connection.execute("""CREATE TABLE batch_control (
            batch_id TEXT PRIMARY KEY, source_name TEXT NOT NULL, input_path TEXT NOT NULL,
            format TEXT NOT NULL, status TEXT NOT NULL, batch_start_time TEXT NOT NULL,
            batch_end_time TEXT, records_processed INTEGER NOT NULL DEFAULT 0)""")
        connection.execute("INSERT INTO batch_control VALUES ('old', 'legacy', 'x', 'csv', 'SUCCESS', 't', 't', 4)")
    BatchTracker(database)
    with sqlite3.connect(database) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(batch_control)")}
        row = connection.execute("SELECT records_processed, error_message, records_persisted FROM batch_control WHERE batch_id = 'old'").fetchone()
    assert {"error_message", "records_persisted", "submission_key", "staged_path"} <= columns
    assert row == (4, None, 0)


def test_submission_key_is_idempotent_but_keyless_calls_are_new_batches(tmp_path: Path) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    first = ingest(config, submission_key="submission-1")
    replay = ingest(config, submission_key="submission-1")
    second = ingest(config)
    rows = (tmp_path / "bronze/customers.jsonl").read_text(encoding="utf-8").splitlines()
    assert first["batch_id"] == replay["batch_id"]
    assert replay["idempotent_replay"] is True
    assert len(rows) == 2
    assert second["batch_id"] != first["batch_id"]


def test_submission_key_rejects_incompatible_destination(tmp_path: Path) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    ingest(config, submission_key="submission-1")
    changed = json.loads(config.read_text(encoding="utf-8"))
    changed["bronze_path"] = "bronze/other.jsonl"
    config.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ValueError, match="incompatible"):
        ingest(config, submission_key="submission-1")


def test_reader_filters_and_reports_malformed_json(tmp_path: Path) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    result = ingest(config)
    reader = BronzeReader()
    assert len(list(reader.read(tmp_path / "bronze/customers.jsonl", batch_id=result["batch_id"]))) == 1
    malformed = tmp_path / "bad.jsonl"
    malformed.write_text('{"valid": true}\nnot-json\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"bad.jsonl:2"):
        list(reader.read(malformed))


def test_abandoned_staged_batch_is_failed_without_overwriting_newer_destination(tmp_path: Path) -> None:
    config_path = write_config(tmp_path, "customers", "customers.csv")
    config = load_config(config_path)
    tracker = BatchTracker(config.metadata_store)
    batch_id = tracker.start(config, "2000-01-01T00:00:00+00:00", "recovery-key", "fingerprint")
    staged = config.bronze_path.with_name(f".{config.bronze_path.name}.{batch_id}.staged")
    staged.parent.mkdir(parents=True)
    staged.write_text('{"_ingestion_batch_id": "' + batch_id + '"}\n', encoding="utf-8")
    tracker.prepare(batch_id, 1, staged)
    config.bronze_path.write_text('{"_ingestion_batch_id": "newer"}\n', encoding="utf-8")
    with sqlite3.connect(config.metadata_store) as connection:
        connection.execute("UPDATE batch_control SET lease_expires_at = ? WHERE batch_id = ?", ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), batch_id))
    assert tracker.recover_stale() == 1
    assert json.loads(config.bronze_path.read_text(encoding="utf-8"))["_ingestion_batch_id"] == "newer"
    with sqlite3.connect(config.metadata_store) as connection:
        assert connection.execute("SELECT status, records_processed, records_persisted FROM batch_control WHERE batch_id = ?", (batch_id,)).fetchone() == ("FAILED", 1, 0)


def test_active_old_running_batch_is_not_recovered(tmp_path: Path) -> None:
    config = load_config(write_config(tmp_path, "customers", "customers.csv"))
    tracker = BatchTracker(config.metadata_store)
    batch_id = tracker.start(config, "2000-01-01T00:00:00+00:00", "active-key", "fingerprint", "owner")
    assert tracker.recover_stale() == 0
    with sqlite3.connect(config.metadata_store) as connection:
        assert connection.execute("SELECT status FROM batch_control WHERE batch_id = ?", (batch_id,)).fetchone() == ("RUNNING",)


def test_failed_parse_persists_short_error_details(tmp_path: Path) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    payload = json.loads(config.read_text(encoding="utf-8"))
    (tmp_path / "broken.json").write_text("{", encoding="utf-8")
    payload.update({"format": "json", "input_path": "broken.json"})
    config.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        ingest(config)
    with sqlite3.connect(tmp_path / "metadata/batches.db") as connection:
        status, processed, persisted, error = connection.execute("SELECT status, records_processed, records_persisted, error_message FROM batch_control").fetchone()
    assert (status, processed, persisted) == ("FAILED", 0, 0)
    assert error.startswith("JSONDecodeError:") and len(error) <= 1000


def test_staging_failure_preserves_existing_committed_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    first = ingest(config)
    destination = tmp_path / "bronze/customers.jsonl"
    before = destination.read_text(encoding="utf-8")
    def fail_commit(self: object, staged: Path, destination: Path) -> None:
        raise OSError("simulated commit failure")
    monkeypatch.setattr(bronze_ingestion.LocalBronzeStore, "commit", fail_commit)
    with pytest.raises(OSError, match="simulated"):
        ingest(config)
    assert destination.read_text(encoding="utf-8") == before
    with sqlite3.connect(tmp_path / "metadata/batches.db") as connection:
        failed = connection.execute("SELECT status, records_processed, records_persisted FROM batch_control WHERE batch_id != ?", (first["batch_id"],)).fetchone()
    assert failed == ("FAILED", 1, 0)


def test_stage_serialization_failure_preserves_committed_file_and_records_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    first = ingest(config)
    destination = tmp_path / "bronze/customers.jsonl"
    before = destination.read_text(encoding="utf-8")
    original_dumps = bronze_ingestion.json.dumps
    def fail_record_serialization(value: object, *args: object, **kwargs: object) -> str:
        if isinstance(value, dict) and "_ingestion_batch_id" in value:
            raise OSError("simulated staging serialization failure")
        return original_dumps(value, *args, **kwargs)
    monkeypatch.setattr(bronze_ingestion.json, "dumps", fail_record_serialization)
    with pytest.raises(OSError, match="staging serialization"):
        ingest(config)
    assert destination.read_text(encoding="utf-8") == before
    with sqlite3.connect(tmp_path / "metadata/batches.db") as connection:
        failed = connection.execute("SELECT status, records_processed, records_persisted, error_message FROM batch_control WHERE batch_id != ?", (first["batch_id"],)).fetchone()
    assert failed[:3] == ("FAILED", 1, 0)
    assert "simulated staging serialization failure" in failed[3]


def test_post_commit_finalization_failure_reconciles_without_duplicate_records(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    original_finish = BatchTracker.finish
    def fail_success(self: BatchTracker, batch_id: str, status: str, *args: object, **kwargs: object) -> None:
        if status == "SUCCESS":
            raise sqlite3.OperationalError("simulated metadata outage")
        original_finish(self, batch_id, status, *args, **kwargs)
    monkeypatch.setattr(BatchTracker, "finish", fail_success)
    with pytest.raises(sqlite3.OperationalError, match="metadata outage"):
        ingest(config, submission_key="commit-key")
    monkeypatch.setattr(BatchTracker, "finish", original_finish)
    replay = ingest(config, submission_key="commit-key")
    rows = (tmp_path / "bronze/customers.jsonl").read_text(encoding="utf-8").splitlines()
    assert replay["idempotent_replay"] is True
    assert len(rows) == 1


def test_distinct_concurrent_submissions_preserve_both_batches(tmp_path: Path) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda key: ingest(config, submission_key=key), ["key-one", "key-two"]))
    rows = [json.loads(line) for line in (tmp_path / "bronze/customers.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert {row["_ingestion_batch_id"] for row in rows} == {result["batch_id"] for result in results}


def test_same_key_concurrent_submissions_do_not_duplicate_records(tmp_path: Path) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    def submit() -> dict[str, object]:
        return ingest(config, submission_key="shared-key")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(submit) for _ in range(2)]
        outcomes = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except RuntimeError:
                pass
    rows = (tmp_path / "bronze/customers.jsonl").read_text(encoding="utf-8").splitlines()
    assert outcomes
    assert len(rows) == 1


@pytest.mark.skipif(sys.platform != "win32", reason="requires Windows msvcrt file locking")
def test_windows_process_concurrency_preserves_distinct_batches(tmp_path: Path) -> None:
    config = write_config(tmp_path, "customers", "customers.csv")
    code = textwrap.dedent("""
        import sys
        from pathlib import Path
        sys.path.insert(0, sys.argv[1])
        from accelerator.bronze_ingestion import ingest
        ingest(sys.argv[2], submission_key=sys.argv[3])
    """)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(PROJECT_ROOT / "src"), str(config), key],
            cwd=PROJECT_ROOT, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for key in ("process-key-one", "process-key-two")
    ]
    outputs = []
    try:
        for process in processes:
            outputs.append(process.communicate(timeout=20))
    except subprocess.TimeoutExpired:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate()
        pytest.fail("separate-process destination locking did not complete within 20 seconds")
    assert all(process.returncode == 0 for process in processes), outputs
    rows = [json.loads(line) for line in (tmp_path / "bronze/customers.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert len({row["_ingestion_batch_id"] for row in rows}) == 2
