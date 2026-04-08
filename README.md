# pEyeON-Analytics

Data pipeline and exploration app for EyeOn metadata.

This repository includes:
- `dlt` to load EyeOn JSON into DuckDB
- `dbt` to build Gold analytics models
- `Streamlit` to browse batches, metadata, and certificates
- `extras/` notebooks for ad hoc exploration

## Overview

EyeOn produces JSON observations for scanned files. This project turns them into queryable analytics tables.

The [pEyeON](https://github.com/llnl/pEyeON) source is available separately.

The pipeline has three layers:

- Bronze: raw JSON for traceability
- Silver: normalized tables loaded from the JSON structure
- Gold: dbt models for analysis-friendly datasets

## Quickstart

### Prerequisites
This quickstart assumes a local system with:

- The binaries you want to scan
- Docker
- A Python environment with this project's requirements installed

### Configure paths

1. Prepare a Python environment for the Streamlit app. If needed, install `uv`: https://docs.astral.sh/uv/getting-started/installation/
2. Copy `EyeOnData.toml-template` to `EyeOnData.toml`.
3. Set `datasets.dataset_path` to the top-level directory where EyeOn batch directories will be written and loaded from.

### Generate data using EyeOn CLI

Run EyeOn to scan a directory of binaries:

`eyeon-parse.sh UTIL_CD SOURCE THREADS`

This creates a new batch directory under `datasets.dataset_path`. You can override it with `--dataset-path` or `EYEON_DATASET_PATH`.

### Load data into database and explore it

1. Run the app with `uv run streamlit run EyeOnData.py` or `streamlit run EyeOnData.py`.
2. On first run, if no database exists yet, the app will prompt you to load one or more batches and choose the DB directory.
3. Browse the loaded data in the app.

## Components

### DLT
`load_eyeon.py` loads EyeOn JSON into DuckDB.

It writes Bronze raw JSON tables, Silver observation and metadata tables, and schema change tracking data.

### dbt
The `dbt_eyeon_gold/` project builds Gold models from Silver.

Examples include file summaries, batch summaries, and certificate-focused models.

### Streamlit
The Streamlit app provides a lightweight interface for loading batches, browsing tables, exploring certificate analytics, and inspecting schema evolution.

## Architecture

```text
EyeOn JSON files
   |
   v
DLT load
   |
   +--> bronze.*
   |
   +--> silver.*
            |
            v
          dbt models
            |
            v
          gold.*
            |
            v
       Streamlit app
```
