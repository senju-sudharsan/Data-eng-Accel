# Component Architecture

## Current implementation

The local prototype uses a metadata-driven Bronze ingestion flow.

- Configuration: JSON files under `config/`.
- Runner: `scripts/run_bronze_ingestion.py`.
- Ingestion module: `src/accelerator/bronze_ingestion.py`.
- Bronze storage: JSON Lines files under `data/bronze/`.
- Metadata storage: SQLite under `data/metadata/`.
- Tests: `tests/test_bronze_ingestion.py`.

CSV ingestion is implemented and tested. JSON array input is also supported. Silver transformations, Gold models, orchestration, and Microsoft Fabric integration remain future work.
