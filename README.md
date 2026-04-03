# pEyeON-Analytics

Data loading, transformation, and exploration pipeline for EyeOn metadata.

This repository contains the data engineering layer for EyeOn:
- `dlt` loads EyeOn JSON into DuckDB using a medallion-style layout
- `dbt` builds Gold-layer analytics models from the Silver layer
- `Streamlit` provides interactive views for batch, metadata, and certificate exploration
- `Extras` directory includes starter jupyter notebooks for exploration

## Overview

EyeOn produces JSON observations for scanned files. This project turns those JSON observations into queryable analytics tables.

The source for the [pEyeON](https://github.com/llnl/pEyeON) is also available.

The pipeline is organized into three layers:

- Bronze:
  raw JSON as-ingested for forensic traceability
- Silver:
  normalized DLT-loaded tables derived from the JSON structure
- Gold:
  dbt models that create business-friendly and analysis-friendly datasets

## Components

### DLT
`load_eyeon.py` loads EyeOn JSON into DuckDB.

It writes:
- Bronze raw JSON tables
- Silver observation and metadata tables
- schema change tracking data

### dbt
The `dbt_eyeon_gold/` project builds Gold models from Silver.

Examples include:
- file-centric summary tables
- batch summaries
- certificate dimensions and facts
- analytical marts for certificate usage, dates, issuers, and organizations

### Streamlit
The Streamlit app provides a lightweight interface for:
- viewing and loading batches of data
- browsing DLT-loaded tables
- exploring certificate analytics
- inspecting schema evolution

## Architecture

```text
EyeOn JSON
   |
   v
DLT
   |
   +--> bronze.*
   |
   +--> silver.*
           |
           v
         dbt
           |
           v
         gold.*
           |
           v
      Streamlit UI
```

## Quickstart - Uses Streamlit GUI to load and process data

_Prequisite: you have data created by EyeOn accessible in the host you're running the Streamlit app on_

1. Install UV which will manage python and dependencies
    https://docs.astral.sh/uv/getting-started/installation/

2. Copy `EyeOnData.toml-template` to `EyeOnData.toml`. You must edit the `datasets->dataset_path` value to a the top level of your EyeOn batches of JSON files.

3. Run the app
    `uv run streamlit run EyeOnData.py`

4. On initial exedcution, as there is no existing database, you'll be presented with a form for loading your first batch(es) of data.  Just select the batch(es) to load and click the `Create DB directory` button.

5. Once loaded, browse thru the app to see data!
