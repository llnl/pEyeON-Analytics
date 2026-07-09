---
title: "Pipeline: DLT Load (load_eyeon.py)"
type: pipeline
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
  - ../pEyeON-Analytics/load_eyeon.py
policy: agent-editable
component: pEyeON-analytics
last_validated: 2026-07-09
tags: [dlt, load, bronze, silver, duckdb]
---

# Pipeline: DLT Load (load_eyeon.py)

## Role in the Pipeline

The DLT load step is the transition from EyeOn JSON batches into queryable
DuckDB tables. The README identifies `dlt` as the loader technology and
`load_eyeon.py` as the script that loads EyeOn JSON into DuckDB.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §top -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §repo-layout -->

## Layer Outputs

The loader contributes to two layers:

| Layer | Output described by README |
| --- | --- |
| `bronze` | `bronze.raw_json` for traceable raw JSON retention |
| `silver` | `silver.raw_obs` and metadata tables normalized from EyeOn JSON |

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §overview -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Operational Paths

The preferred operational path is through Streamlit's `Load Selected` button,
which handles DLT loading and dbt modeling for normal usage. The manual loader
path is retained for development and troubleshooting:

```bash
uv run python load_eyeon.py --utility_id UTIL_CD --source /path/to/batch --log-level INFO
```

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-batches-from-the-app -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-a-batch-with-the-loader -->

## Schema Bootstrap Note

DLT persists pipeline metadata (including schemas) under the invoking user's dlt state directory (eg `~/.dlt/pipelines/eyeon_metadata/`). On a first run, `load_eyeon.py` may not have a persisted schema yet.

To avoid failing on first-run, `load_eyeon.py` only attempts to pre-create destination tables when a dataset schema is present; otherwise it relies on `pipeline.run(...)` to bootstrap state.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/load_eyeon.py §pipeline = dlt.pipeline -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/load_eyeon.py §_ensure_destination_tables -->

## Related

- [[wiki/components/load_eyeon]] — loader component
- [[wiki/pipeline/base_schema_derivation]] — base-schema derivation and schema_blame corpus-selection notes
- [[wiki/overview/data_flow]] — full data flow
- [[wiki/pipeline/dbt_models]] — downstream dbt model build
