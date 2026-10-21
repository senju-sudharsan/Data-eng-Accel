# Component Interfaces

## Source configuration

The runner accepts a JSON configuration path. Current fields include:

- `source_name`: logical source identifier.
- `format`: input format.
- `input_path`: source file path.
- `bronze_path`: destination JSONL path.
- `metadata_store`: SQLite database path.
- `delimiter`: CSV delimiter.
- `encoding`: text encoding.

Relative paths resolve from the configuration file directory.

## Command-line interface

Example:

`python scripts/run_bronze_ingestion.py config/source_customers.json`

The runner prints the ingestion result as formatted JSON.

## Bronze output

Each JSONL line represents one record containing source fields and ingestion lineage.

## Planned Silver interface

The team still needs to define inputs, canonical schema configuration, output location, validation results, rejected-record handling, and batch reporting. This interface is not yet implemented.
