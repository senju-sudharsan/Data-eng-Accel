# Architecture Decision Records (ADRs)

## AD-001: JSON source configuration
**Decision:** Configure each source using JSON.  
**Rationale:** Compatible sources can share one ingestion implementation.  
**Status:** Implemented (Phase 0).

---

## AD-002: JSON Lines Bronze output
**Decision:** Store one JSON record per line.  
**Rationale:** Retains source fields in a record-oriented format.  
**Status:** Implemented (Phase 0).

---

## AD-003: Preserve CSV values as strings
**Decision:** Do not infer data types during Bronze ingestion.  
**Rationale:** Preserves values such as postal codes with leading zeros and avoid silent loss of precision.  
**Status:** Implemented and tested (Phase 0).

---

## AD-004: SQLite for local metadata
**Decision:** Use SQLite for prototype batch metadata.  
**Rationale:** Lightweight local development storage without external server dependencies.  
**Status:** Implemented. Cloud metastore integration planned.

---

## AD-005: Separate Bronze outputs by source
**Decision:** Each demonstration configuration writes to a separate Bronze file while sharing metadata storage.  
**Status:** Implemented in sample configurations (Phase 0).

---

## AD-006: Canonical Silver Schema Strategy & Typed Models
**Decision:** Implement an entity-agnostic canonical schema specification engine using Python dataclasses (`ColumnSchema`, `ColumnMapping`, `SilverConfig`).  
**Rationale:** Decouples heterogeneous source schemas from analytics dimensions. Enables column renaming, type casting, default values, and strict schema validation before records enter Silver.  
**Status:** Implemented in `src/accelerator/contracts.py` and tested.

---

## AD-007: Quarantine Store for Rejected Records
**Decision:** Records failing data quality validation with `REJECT` severity are diverted to a dedicated quarantine store (JSON Lines / Delta table) rather than being silently dropped or crashing the entire batch.  
**Rationale:** Preserves bad records for diagnostic inspection, root-cause analysis, and reprocessing without corrupting conformed Silver data.  
**Status:** Contract defined in `QualityValidatorProtocol`; quarantine record schema formalized.

---

## AD-008: Slowly Changing Dimension Type 2 (SCD2) Strategy
**Decision:** Formalize an SCD Type 2 dimension model using business natural keys, surrogate keys (`_scd_key`), date intervals (`_effective_from`, `_effective_to`), and an active record flag (`_is_current`).  
**Rationale:** Preserves accurate historical point-in-time context for changing entity attributes while enabling point-in-time queries and current state reporting.  
**Status:** Contract defined in `Scd2ProcessorProtocol`; schema rules formalized.

---

## AD-009: Decoupled Component Interfaces via Python Protocols
**Decision:** Define all inter-component boundaries using `typing.Protocol` with `@runtime_checkable` (`SourceReaderProtocol`, `BronzeStoreProtocol`, `SilverTransformerProtocol`, `QualityValidatorProtocol`, `Scd2ProcessorProtocol`, `BatchTrackerProtocol`).  
**Rationale:** Allows independent, parallel development across the three teammates. Teammate 2 can implement readers/storage and Teammate 3 can implement Silver/SCD2 without circular code dependencies.  
**Status:** Implemented in `src/accelerator/contracts.py` and tested.

---

## AD-010: Microsoft Fabric Integration Strategy
**Decision:** Design the accelerator core as platform-independent Python, using storage and metadata abstractions that map directly to Microsoft Fabric Lakehouse Delta tables, OneLake paths, and Fabric Notebook orchestration.  
**Rationale:** Satisfies the assessment requirement for a verifiable Fabric integration path while allowing full local development and testing without cloud vendor lock-in or billable cloud infrastructure.  
**Status:** Architecture and mapping documented.

---

## Open Decisions for Phase 1 & 2
- Automated schema evolution (handling schema drift over time).
- Production metastore choice (Fabric Lakehouse Delta table vs Azure SQL vs external Postgres).
- Production orchestration engine (Fabric Data Factory pipeline vs Managed Airflow vs CLI orchestrator).
- Exact deduplication and idempotent overwrite mechanics for reruns.
