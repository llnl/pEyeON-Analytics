---
title: "Schema: Silver Layer (DLT Tables)"
type: schema
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: agent-editable
component: pEyeON-analytics
last_validated: 2026-06-26
tags: [silver, dlt, duckdb, raw_obs]
---

# Schema: Silver Layer (DLT Tables)

## Purpose

The silver layer contains normalized observation and metadata tables loaded from
EyeOn JSON. In the README data-flow diagram, the silver layer is represented as
`silver.raw_obs` and metadata tables.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §overview -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Producer

`load_eyeon.py` loads EyeOn JSON into DuckDB via `dlt`, producing the silver
tables after retaining raw JSON in bronze.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §repo-layout -->

## Schema Artifacts

The repository layout notes that `schemas/` contains the generated DLT schema
plus bootstrap SQL. The README also states that
`schemas/eyeon_metadata.schema.yaml` is generated as part of the DLT workflow and
is intentionally checked in.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §repo-layout -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §notes -->

## Consumer

The dbt project consumes silver tables and builds `gold.*` reporting and
exploration models.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Related

- [[wiki/components/load_eyeon]] — producer
- [[wiki/schemas/gold_layer]] — downstream modeled layer
- [[wiki/overview/data_flow]] — full pipeline overview
