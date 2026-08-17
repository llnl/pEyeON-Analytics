---
title: "Schema: Silver Layer (DLT Tables)"
type: schema
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: agent-editable
last_validated: 2026-06-26
repo_scope: pEyeON-Analytics
implementation_area: dlt-pipeline
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/schema/silver_layer.md
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

The checked-in `schemas/schema.sql` is the bootstrap DDL exported from an evolved
DuckDB database. The base-schema derivation page records the recovered workflow:
use representative EyeON sample data to let DLT discover the broadest practical
silver schema, materialize `schema_blame`, export the database schema, and then
reuse that DDL for stable bootstrap databases.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/schema.sql lines 1-62 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/load_eyeon.py lines 304-319 -->

## Consumer

The dbt project consumes silver tables and builds `gold.*` reporting and
exploration models.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Related

- [[wiki/component/load_eyeon]] — producer
- [[wiki/pipeline/base_schema_derivation]] — recovered base-schema derivation methodology
- [[wiki/schema/gold_layer]] — downstream modeled layer
- [[wiki/overview/data_flow]] — full pipeline overview
