# Architecture Decisions

## AD-001: JSON source configuration

**Decision:** Configure each source using JSON.

**Rationale:** Compatible sources can share one ingestion implementation.

**Status:** Implemented.

## AD-002: JSON Lines Bronze output

**Decision:** Store one JSON record per line.

**Rationale:** Retains source fields in a record-oriented format.

**Status:** Implemented.

## AD-003: Preserve CSV values as strings

**Decision:** Do not infer data types during Bronze ingestion.

**Rationale:** Preserves values such as postal codes with leading zeros.

**Status:** Implemented and tested.

## AD-004: SQLite for local metadata

**Decision:** Use SQLite for prototype batch metadata.

**Rationale:** Lightweight local development storage.

**Status:** Implemented. Production metadata storage remains undecided.

## AD-005: Separate Bronze outputs by source

**Decision:** Each demonstration configuration writes to a separate Bronze file while sharing metadata storage.

**Status:** Implemented in sample configurations.

## Open decisions

- Canonical Silver schema and schema versioning.
- SCD Type 1 and Type 2 behavior.
- Data-quality and rejected-record policies.
- Idempotency and duplicate handling.
- Production storage, orchestration, and Fabric integration.
- Team ownership and pull-request review policy.
