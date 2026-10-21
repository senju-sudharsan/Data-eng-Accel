# Data Flow

## Implemented Bronze flow

1. Select a source JSON configuration.
2. Resolve input, output, and metadata paths relative to the configuration directory.
3. Read source records.
4. Add ingestion lineage fields.
5. Write records to Bronze JSONL storage.
6. Record batch metadata in SQLite.
7. Validate behavior with automated tests.

The sample configurations are `config/source_customers.json` and `config/source_customers_alternate.json`. They use separate Bronze outputs and a shared metadata database.

## Planned downstream flow

Source files -> Bronze -> Silver canonicalization and quality checks -> Gold analytical outputs.

Silver and Gold are planned, not implemented. Bronze output currently appends records on reruns, so duplicate handling and idempotency remain open tasks.
