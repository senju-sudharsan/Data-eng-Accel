# Metadata-Driven Bronze–Silver Data Engineering Accelerator

This Week 1 vertical slice lands raw CSV records in local Bronze JSON Lines storage under configuration control. It preserves CSV values as strings and appends source, batch, and UTC ingestion-time lineage to each record. Batch status is recorded in a local SQLite database, following the batch-control lifecycle used in the original Olist proof of concept.

Run the primary fixture from the project root:

```powershell
python scripts/run_bronze_ingestion.py config/source_customers.json
```

Change only the config argument to land the compatible second source:

```powershell
python scripts/run_bronze_ingestion.py config/source_customers_alternate.json
```

Both runs append to `data/bronze/customers_raw.jsonl`; each record carries `_ingestion_source_name`, `_ingestion_batch_id`, and `_ingestion_timestamp`. Batch records are stored in `data/metadata/ingestion.db`.

Run tests (after installing the development dependency):

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

`src/accelerator/bronze_ingestion.py` also contains the smallest JSON extension: it accepts an array of objects or NDJSON objects when configuration sets `"format": "json"`. No JSON fixture is required for the CSV acceptance demonstration.

The landing store is deliberately local for this assessment. The reader and `LocalBronzeStore` are separate so a future Microsoft Fabric adapter can replace the storage implementation; Fabric integration is not implemented here.
