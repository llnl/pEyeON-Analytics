---
title: "Pipeline: DLT Load (load_eyeon.py)"
type: pipeline
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: agent-editable
component: pEyeON-analytics
last_validated: 2026-06-26
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

## Related

- [[wiki/components/load_eyeon]] — loader component
- [[wiki/overview/data_flow]] — full data flow
- [[wiki/pipeline/dbt_models]] — downstream dbt model build
