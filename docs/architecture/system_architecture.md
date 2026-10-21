# System Architecture

## Purpose

Provide reusable, metadata-driven file ingestion that avoids a separate ingestion implementation for every compatible source.

## Current local components

| Component | Responsibility |
|---|---|
| JSON source configuration | Defines source format, paths, and parsing settings |
| Python CLI runner | Starts configured ingestion |
| Bronze ingestion module | Reads records, adds lineage, writes JSONL |
| Local filesystem | Stores fixtures and Bronze output |
| SQLite | Stores ingestion batch metadata |
| Pytest | Validates ingestion behavior |

The prototype runs locally and does not currently integrate with Microsoft Fabric.

## Target architecture

- Bronze: raw source records and ingestion lineage.
- Silver: canonicalization, transformations, and quality checks.
- Gold: curated analytical outputs.
- Metadata and audit: execution status and processing metrics.
- Orchestration: coordinate pipeline execution.

Target capabilities must be implemented and tested before being described as production-ready.
