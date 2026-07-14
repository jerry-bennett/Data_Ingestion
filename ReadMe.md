# Local Event Ingestion & Deduplication Pipeline

An end-to-end local data engineering pipeline that simulates real-time web analytics clickstream tracking, stages nested JSON payloads, and incrementally ingests, flattens, and deduplicates records into an analytical warehouse.

This project is designed to demonstrate key data engineering paradigms—such as the decoupling of storage and compute, handling highly nested JSON schemas, and enforcing data-quality-oriented pipeline idempotency—using a lightweight, highly efficient local stack.

---

## Architecture Overview

The pipeline separates the generation of telemetry data from the transformation and storage layer.

[Phase 1: Event Generator]
│  Generates raw telemetry logs simulating Adobe/GTM payloads
▼
[Phase 2: Local Landing Zone]
│  C:/tmp/analytics/landing/ (Raw nested JSON file batches)
▼
[Phase 3: DuckDB Ingestor]
│  Bulk-reads JSON, flattens nested schemas, deduplicates payloads via window functions
▼
[Phase 4: Warehouse Database]
└── C:/tmp/analytics/warehouse.db (Persistent columnar storage)

### Key Engineering Decisions
* **Data Layer Simulation:** Instead of generic flat mock data, the generator outputs heavily nested JSON structures mirroring a real enterprise web data layer (e.g., user contexts and dynamic dynamic-commerce object arrays).
* **DuckDB for OLAP:** DuckDB is utilized as an embedded, in-process analytical engine. It handles schema inference, flattens JSON on-the-fly, and provides lightning-fast columnar processing without the overhead of spinning up a heavyweight database server.
* **Idempotency & Deduplication:** Web tracking is notoriously duplicate-heavy. The pipeline handles data quality issues at the ingestion gate by utilizing analytical SQL window functions (`ROW_NUMBER() OVER(PARTITION BY event_id...)`) to guarantee that only the latest version of an event is merged.

---

## Technologies Used

* **Python 3.x:** Event generation scripting, pipeline control flow.
* **DuckDB:** Columnar SQL engine for staging, processing, and warehousing.
* **JSON Lines:** Standard format for high-throughput streaming and bulk raw event logs.

---

## Directory Structure

```text
├── data_generator.py           # Simulates real-time telemetry streaming batches to disk
├── data_ingestion.py           # Staging, flattening, and upserting logic (DuckDB SQL)
└── README.md                   # Documentation
```

---

## Getting Started

1. Prerequisites

Ensure you have Python installed, then install DuckDB:
Bash

pip install duckdb

2. Run the Generator

Start the event streaming simulator to continuously drop batches of raw web telemetry JSON into your local staging area (C:/tmp/analytics/landing):
Bash

python generator.py

Let this run for 10–15 seconds to populate raw logs, then hit Ctrl+C to stop.
3. Run the Ingestor

Run the pipeline script to process, clean, and write the staged tracking data to the persistent warehouse database (C:/tmp/analytics/warehouse.db):
Bash

python ingest.py

## Future Roadmap

    [ ] Orchestration: Wrap the execution flow in an Apache Airflow or Dagster DAG to automate micro-batch runs.

    [ ] Data Modeling: Transition the flat silver_events table into a clean star-schema dimensional model (separating facts and dimensions).

    [ ] Cloud Migration: Replace the local /tmp directories with AWS S3 / GCP GCS and connect DuckDB to query cloud-stored Parquet files.


---

### One Quick Tip Before You Push:
Create a `.gitignore` file in your repository root to make sure you don't accidentally commit your local temporary data or warehouse files to GitHub. Add these lines to it:

```text
# Local database and temp files
tmp/
*.db
*.json
.DS_Store
__pycache__/