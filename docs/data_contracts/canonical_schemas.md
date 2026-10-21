# Canonical Schemas

## Current status

Bronze ingestion preserves source fields and does not yet enforce a canonical Silver customer schema.

## Bronze record contract

- Preserve source columns and values.
- CSV values remain strings, including identifiers with leading zeros.
- Add `_ingestion_source_name`, `_ingestion_batch_id`, and `_ingestion_timestamp`.
- Write one JSON object per line in JSONL format.

## Silver schema decisions required

The team must agree on canonical field names, data types, required fields, nullability, business keys, duplicate handling, invalid-record handling, and schema versioning.

SCD Type 1 and Type 2 behavior must also be defined, including business keys and effective-date/current-record fields where needed.

The canonical Silver schema is not yet implemented.
