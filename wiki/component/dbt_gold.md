---
title: "Component: dbt Gold Models"
type: component
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: agent-editable
last_validated: 2026-06-26
repo_scope: pEyeON-Analytics
implementation_area: dbt-gold
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/component/dbt_gold.md
tags: [dbt, gold, duckdb, reporting]
---

# Component: dbt Gold Models

## Purpose

`dbt_eyeon_gold/` is the dbt project that builds modeled analytics tables for
reporting and exploration. In the README's layer model, `gold` contains dbt
models for reporting and exploration.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §repo-layout -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §overview -->

## Normal Invocation

For normal usage, dbt runs as part of the Streamlit app's `Load Selected`
workflow after selected batches are loaded.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-batches-from-the-app -->

## Manual Invocation

The README documents a direct dbt command for development,
troubleshooting, or incremental reruns:

```bash
uv run dbt build --project-dir dbt_eyeon_gold --profiles-dir dbt_eyeon_gold
```

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §build-dbt-models-manually -->

## Inputs and Outputs

The README's data-flow diagram shows dbt consuming silver tables, especially
`silver.raw_obs` and metadata tables, and producing `gold.*` models for the
Streamlit pages.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Related

- [[wiki/pipeline/dbt_models]] — dbt pipeline lineage
- [[wiki/overview/data_flow]] — bronze/silver/gold flow
- [[wiki/component/streamlit_app]] — UI consumer of gold models
