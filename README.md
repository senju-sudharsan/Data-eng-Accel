# Metadata-Driven Bronze–Silver Data Engineering Accelerator

A modular, metadata-driven data engineering framework designed to ingest tabular sources into a Bronze layer, standardize and validate them in a conformed Silver layer, manage historical dimension versioning via Slowly Changing Dimension Type 2 (SCD2), and maintain end-to-end lineage, batch control, and auditability.

---

## Architecture & Contract Overview

The framework separates execution logic from dataset-specific definitions through metadata-driven configuration.

- **Bronze Layer:** Lands raw CSV or JSON data as immutable records. Preserves all source column names and values as strings to prevent precision loss. Enriches records with technical lineage: `_ingestion_source_name`, `_ingestion_batch_id`, and `_ingestion_timestamp`.
- **Silver Layer (Conformed):** Maps source fields to canonical schemas, standardizes data types (`FieldType`), validates quality constraints (`QualityRule`), and enriches conformed records with `_silver_batch_id`, `_silver_processed_at`, and `_record_hash`.
- **Quarantine Store:** Diverts records that fail quality checks to a dedicated quarantine store decorated with `_rejection_reason`, `_failed_rule`, `_rejected_at`, and `_raw_record`.
- **SCD Type 2 Dimension:** Merges validated Silver records into historical dimensions using natural `business_keys`, generating `_scd_key`, `_effective_from`, `_effective_to`, and `_is_current`.
- **Batch Metadata & Audit:** Tracks execution lifecycle (`PENDING`, `RUNNING`, `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`) and record metrics in SQLite (`data/metadata/ingestion.db`).
- **Microsoft Fabric Integration:** The core is platform-independent Python. Storage and tracker abstractions (`BronzeStoreProtocol`, `BatchTrackerProtocol`) allow swapping local JSON Lines and SQLite for Fabric OneLake Delta tables and Fabric Lakehouse metadata without altering transformation logic.

---

## Repository Layout

```
C:\Data-eng-Accel\
├── config/
│   ├── source_customers.json               # Baseline Bronze config (CSV)
│   ├── source_customers_alternate.json     # Alternate Bronze config (CSV)
│   └── dataset_customers_pipeline.json     # Reference end-to-end Bronze-Silver-SCD2 config
├── data/
│   ├── fixtures/                           # Source CSV data fixtures
│   ├── bronze/                             # Landed raw JSONL files (gitignored)
│   ├── silver/                             # Conformed & quarantine JSONL files (gitignored)
│   └── metadata/                           # SQLite batch tracking database (gitignored)
├── docs/
│   ├── architecture/                       # System, component, and data flow architecture
│   ├── data_contracts/                     # Canonical schema and component interface specifications
│   └── decisions/                          # Architectural Decision Records (ADRs AD-001 - AD-010)
├── scripts/
│   └── run_bronze_ingestion.py             # CLI runner for Bronze ingestion
├── src/
│   └── accelerator/
│       ├── __init__.py                     # Package exports
│       ├── contracts.py                    # Schema models, enums, protocols, config loader
│       └── bronze_ingestion.py             # CSV/JSON readers, LocalBronzeStore, BatchTracker
└── tests/
    ├── test_bronze_ingestion.py            # Baseline Bronze ingestion tests
    └── test_contracts.py                   # Schema and component contract tests
```

---

## Setup & Testing

### 1. Initialize Virtual Environment & Dependencies
```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

### 2. Run Test Suite
```powershell
.\.venv\Scripts\python.exe -m pytest -v
```
All baseline Bronze ingestion tests and Phase 0 contract validation tests run and pass.

### 3. Run Bronze Ingestion Demonstration
Run the primary customer fixture:
```powershell
.\.venv\Scripts\python.exe scripts/run_bronze_ingestion.py config/source_customers.json
```

Run the compatible alternate customer source:
```powershell
.\.venv\Scripts\python.exe scripts/run_bronze_ingestion.py config/source_customers_alternate.json
```

Both runs append to their respective raw Bronze files under `data/bronze/` and record execution metrics in `data/metadata/ingestion.db`.

### Bronze reliability and retry behavior

Bronze ingestion has a `RUNNING → SUCCESS` or `RUNNING → FAILED` lifecycle in
SQLite. `records_processed` is the number of source records read for the
attempt; `records_persisted` is the number committed to Bronze. A successful
local commit always has equal counts because the JSONL output is first staged
as a complete replacement snapshot and then atomically published. Failures
store a short exception type/message in `error_message`, not a traceback.

`ingest(config_path)` remains an intentional new batch on every invocation.
Call `ingest(config_path, submission_key="external-request-id")` when retrying
one logical submission: a successful matching key returns the existing batch
summary without writing again. The key is bound to source name, format, input
path, and Bronze destination; reuse with different details raises an error.
Another attempt while that key is `RUNNING` errors while its ownership lease is
active. Once that lease has expired, the attempt first reconciles any already
published Bronze records, then safely restarts the same batch only if it was
not committed.

Each running batch has a persisted ownership lease. `BatchTracker.recover_stale()`
only handles expired leases; age alone never takes ownership from a potentially
active run. Under a destination-scoped Windows file lock, recovery marks a batch
successful when its batch ID is already present in Bronze. It never publishes an
old staged full-file snapshot, because that snapshot could overwrite a newer
commit; instead it marks an uncommitted abandoned batch failed for a safe keyed
retry. The same lock serializes local writers through staging, replacement, and
metadata reconciliation. SQLite and filesystem publication still cannot form a
single transaction, but destination evidence reconciles the small window after
atomic file publication and before SQLite success recording.

Use `BronzeReader().read(path, source_name=..., batch_id=...)` to stream stored
JSONL records with lineage preserved. A missing file raises `FileNotFoundError`;
malformed JSON reports its file and line number.

The local implementation materializes the incoming batch and the current
Bronze JSONL snapshot while it builds an atomic replacement. Consequently,
memory use grows with both the incoming batch and existing destination; a
million-row source can require substantial memory. A future streaming staging
implementation should preserve atomic publication and partial-count/error
semantics before replacing this deliberately simple local approach.

---

## Team Division of Work

- **Teammate 1 (Framework Architect):** Configuration contracts, canonical schemas, component interfaces, execution flow, ADRs.
- **Teammate 2 (Ingestion & Platform):** CSV/JSON source readers, Bronze landing implementation, batch tracking, Fabric platform integration.
- **Teammate 3 (Silver & SCD2):** Silver transformations, conformed schema casting, quality validation rules, quarantine handling, SCD2 dimension merge logic.
