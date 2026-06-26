---
title: "Component: load_eyeon.py (DLT Pipeline)"
type: component
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: agent-editable
component: pEyeON-analytics
last_validated: 2026-06-26
tags: [dlt, duckdb, bronze, silver]
---

# Component: load_eyeon.py (DLT Pipeline)

## Purpose

`load_eyeon.py` loads EyeOn JSON batches into DuckDB through `dlt`. In the
documented data flow, it receives an EyeOn JSON batch and writes both
`bronze.raw_json` and silver normalized observation/metadata tables.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §repo-layout -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Normal Invocation

Normal users are expected to run the loader indirectly through the Streamlit app:
select one or more EyeOn batch directories and click `Load Selected`. The README
states that this path handles the DLT load and dbt modeling steps for normal
usage.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-batches-from-the-app -->

## Manual Invocation

The README keeps a direct loader command for development, troubleshooting, and
incremental reruns:

```bash
uv run python load_eyeon.py --utility_id UTIL_CD --source /path/to/batch --log-level INFO
```

Useful log levels are `INFO` for normal runs and `DEBUG` for more verbose
troubleshooting.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-a-batch-with-the-loader -->

## Inputs and Outputs

Input is a specific EyeOn batch directory containing one JSON observation per
scanned file. Output is persisted to DuckDB according to the configured database
location, with raw JSON retained in bronze and normalized tables in silver.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §overview -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Related

- [[wiki/overview/data_flow]] — bronze/silver/gold flow
- [[wiki/pipeline/dlt_load]] — pipeline-level loader notes
- [[wiki/components/streamlit_app]] — app-driven loading workflow
