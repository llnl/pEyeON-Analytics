---
title: "Schema: Gold Layer (dbt Models)"
type: schema
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: agent-editable
component: pEyeON-analytics
last_validated: 2026-06-26
tags: [gold, dbt, reporting, marts]
---

# Schema: Gold Layer (dbt Models)

## Purpose

The gold layer contains dbt models for reporting and exploration. The README
data-flow diagram represents this layer as `gold.*`, produced by
`dbt_eyeon_gold` and consumed by Streamlit pages.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §overview -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Producer

`dbt_eyeon_gold/` is the dbt project for modeled analytics tables. It consumes
the silver layer and builds the gold layer.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §repo-layout -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Build Paths

Normal usage builds gold models through the Streamlit `Load Selected` workflow.
The manual build path is:

```bash
uv run dbt build --project-dir dbt_eyeon_gold --profiles-dir dbt_eyeon_gold
```

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-batches-from-the-app -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §build-dbt-models-manually -->

## Consumer

Streamlit pages consume the gold models for interactive reporting and
exploration.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Related

- [[wiki/components/dbt_gold]] — dbt component
- [[wiki/pipeline/dbt_models]] — lineage notes
- [[wiki/schemas/silver_layer]] — upstream normalized layer
