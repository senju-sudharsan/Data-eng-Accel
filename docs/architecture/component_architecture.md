# Component Architecture & Team Ownership Boundaries

## 1. Modular Architecture Overview

The accelerator codebase is structured to enforce strong separation of concerns across teammates while ensuring tight contract adherence.

```
C:\Data-eng-Accel\
├── config/
│   ├── source_customers.json               # Legacy Bronze config 1 (CSV)
│   ├── source_customers_alternate.json     # Legacy Bronze config 2 (CSV alternate)
│   └── dataset_customers_pipeline.json     # Reference end-to-end Bronze-Silver-SCD2 config
├── data/
│   ├── fixtures/                           # Source CSV / JSON test files
│   ├── bronze/                             # Landed raw JSONL files (gitignored)
│   ├── silver/                             # Conformed & quarantine JSONL files (gitignored)
│   └── metadata/                           # SQLite batch tracking database (gitignored)
├── docs/
│   ├── architecture/                       # System, component, and data flow architecture
│   ├── data_contracts/                     # Canonical schema and component interface specifications
│   └── decisions/                          # Architectural Decision Records (ADRs)
├── scripts/
│   ├── run_bronze_ingestion.py             # CLI runner for Bronze landing
│   └── run_pipeline.py                     # CLI runner for full pipeline (Phase 1)
├── src/
│   └── accelerator/
│       ├── __init__.py                     # Clean package exports
│       ├── contracts.py                    # Schema models, enums, protocols, config loader
│       ├── bronze_ingestion.py             # CSV/JSON readers, LocalBronzeStore, BatchTracker
│       ├── silver_transformation.py        # Canonicalization & quality checks (Phase 1)
│       └── scd2_processor.py               # SCD Type 2 dimension merge engine (Phase 1)
└── tests/
    ├── test_bronze_ingestion.py            # Baseline Bronze ingestion unit tests
    └── test_contracts.py                   # Configuration and contract verification tests
```

---

## 2. Team Responsibility Matrix

| Teammate | Role | Primary Components & Files | Concrete Contract Boundaries |
|---|---|---|---|
| **Me (T1)** | **Framework Architect** | `src/accelerator/contracts.py`<br/>`config/dataset_*.json`<br/>`docs/`<br/>`tests/test_contracts.py` | Defines `PipelineConfig`, `SilverConfig`, `ColumnSchema`, `QualityRule`, `Scd2Config`, and all `Protocol` specifications. Enforces configuration validation and framework-wide lineage constants. |
| **Teammate 2** | **Ingestion & Platform Architect** | `src/accelerator/bronze_ingestion.py`<br/>`scripts/run_bronze_ingestion.py`<br/>`tests/test_bronze_ingestion.py` | Implements `SourceReaderProtocol` (CSV/JSON readers), `BronzeStoreProtocol` (`LocalBronzeStore`), and `BatchTrackerProtocol` (`BatchTracker`). Designs Fabric Lakehouse landing adapters. |
| **Teammate 3** | **Silver & SCD2 Implementation** | `src/accelerator/silver_transformation.py`<br/>`src/accelerator/scd2_processor.py`<br/>`tests/test_silver.py`<br/>`tests/test_scd2.py` | Implements `SilverTransformerProtocol` (mapping & type casting), `QualityValidatorProtocol` (rule evaluation & quarantine), and `Scd2ProcessorProtocol` (dimension versioning). |

---

## 3. How Teammates Build Against Shared Contracts

### 3.1 Teammate 2 Implementation Guidance
- Consume `PipelineConfig` produced by `load_pipeline_config()`.
- Ensure all raw records yielded by `read_source()` append the exact lineage keys defined in `contracts.py`:
  - `LINEAGE_SOURCE_NAME = "_ingestion_source_name"`
  - `LINEAGE_BATCH_ID = "_ingestion_batch_id"`
  - `LINEAGE_INGESTION_TIMESTAMP = "_ingestion_timestamp"`
- Maintain string fidelity: Never coerce or infer types in the Bronze stage.

### 3.2 Teammate 3 Implementation Guidance
- Import `SilverTransformerProtocol`, `QualityValidatorProtocol`, and `Scd2ProcessorProtocol` from `accelerator.contracts`.
- Consume conformed `SilverConfig` from `PipelineConfig.silver`.
- For quality failures:
  - If rule severity is `REJECT`, divert to `silver_config.rejected_path` using quarantine keys: `_rejected_at`, `_rejection_reason`, `_failed_rule`, `_raw_record`.
  - If rule severity is `WARN`, log metrics to `QualityCheckResult` and proceed.
  - If rule severity is `CRITICAL`, raise `RuntimeError`.
- For SCD2:
  - Match incoming records to active dimension records on `scd2_config.business_keys`.
  - Track changes across `scd2_config.tracking_columns`.
  - Populate `_scd_key`, `_effective_from`, `_effective_to`, `_is_current`.
