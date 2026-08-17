---
title: "Decision: DuckDB + DLT + dbt Stack"
type: decision
status: accepted
decided_on: 2026-06-26
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: human-review-required
last_validated: 2026-06-26
repo_scope: pEyeON-Analytics
implementation_area: dlt-pipeline
format_domain: none
audience: mixed
source_paths: wiki/decision/duckdb_dlt_dbt.md
tags: [duckdb, dlt, dbt, analytics]
---

# Decision: DuckDB + DLT + dbt Stack

## Context

`pEyeON-Analytics` needs a local analytics stack that can ingest EyeOn JSON,
build analysis-friendly models, and support interactive exploration without
requiring a remote warehouse.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §top -->

## Decision

Use `dlt` to load EyeOn JSON into DuckDB, `dbt` to build analysis-friendly
models, and Streamlit to explore batches, metadata, schema changes, and
certificate data.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §top -->

## Rationale

The README presents the project as a local analytics stack and names these tools
as the core workflow pieces. DuckDB is the local database target; `dlt` handles
JSON loading; dbt builds modeled reporting tables; Streamlit gives users a UI
for batch loading and exploration.

## Consequences

Users configure local paths in `EyeOnData.toml`, run the Streamlit app locally,
and can fall back to manual `load_eyeon.py` and `dbt build` commands for
development, troubleshooting, or incremental reruns.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §configure-local-paths -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §optional-manual-workflow -->

## Alternatives Considered

The README does not document alternatives such as a hosted data warehouse,
single-purpose Python scripts without dbt, or a non-Streamlit UI. Those remain
outside the README-grounded decision record.
