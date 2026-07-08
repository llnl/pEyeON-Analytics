---
title: "Metadata Curation (Silver vs Gold)"
type: pipeline
confidence: high
grounded_by:
  - load_eyeon.py
  - dbt_eyeon_gold/models/gold/all_metadata.sql
  - dbt_eyeon_gold/models/gold/metadata_type_drift.sql
  - pages/Schema_Blame.py
policy: agent-editable
last_validated: 2026-07-07
component: pEyeON-analytics
tags: [dlt, dbt, silver, gold, streamlit, metadata]
---

## Mental Model

Silver (DLT) is **discovery**: it loads whatever metadata keys appear in EyeON JSON and routes each metadata object into a `silver.metadata_*` table.
<!-- GROUND_TRUTH: load_eyeon.py §metadata_resource (table_name=lambda item: item["_metadata_table_name"]) -->

### Operational Note: DLT Schema vs DuckDB Tables

DLT's pipeline schema can accumulate "known" tables over time (across runs). If your DuckDB file was initialized from a static snapshot (`schemas/schema.sql`) that doesn't include those newer tables, DLT may attempt inserts into tables that do not exist yet.

The loader mitigates this by creating missing tables/columns in the destination from the pipeline schema prior to loading each batch.
<!-- GROUND_TRUTH: load_eyeon.py §_ensure_destination_tables -->

Gold (dbt) is **curation**: it defines which metadata types are "known/modeled" by explicitly unioning staging models into `gold.all_metadata`.
<!-- GROUND_TRUTH: dbt_eyeon_gold/models/gold/all_metadata.sql -->

Streamlit pages that use "known metadata types" should prefer gold as the gate; pages that need discovery/debugging should show silver-only types as "unmodeled".

The Streamlit search dropdown for "Metadata Type" should derive its list of curated types from `gold_staging.stg_metadata_*` (dbt staging models). Add a separate `no_metadata` option for observations that have no silver metadata rows at all, and reserve `unknown` to mean scanner-produced `silver.metadata_unknown`.

## Adding A New Curated Metadata Type

1. Confirm the type exists in silver (after loading data): `silver.metadata_<type>`.
1. Add a staging model: `dbt_eyeon_gold/models/staging/stg_metadata_<type>.sql` selecting from `source('silver', 'metadata_<type>')`.
1. Add that staging model into the union in `dbt_eyeon_gold/models/gold/all_metadata.sql`.
1. Run dbt so gold models refresh.

## Drift Signal

`gold.metadata_type_drift` compares:

1. Discovered metadata tables in `silver` (actual presence)
1. Curated metadata types based on which `gold_staging.stg_metadata_*` models exist (modeled support, even if currently empty)

Unmodeled entries indicate new/extra metadata types present in silver that do not yet have downstream dbt/Streamlit support.
