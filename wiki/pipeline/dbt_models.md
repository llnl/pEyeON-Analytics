---
title: "Pipeline: dbt Model Lineage"
type: pipeline
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
source_paths: wiki/pipeline/dbt_models.md
tags: [dbt, lineage, gold, staging]
---

# Pipeline: dbt Model Lineage

## Role in the Pipeline

The dbt step transforms silver observation and metadata tables into `gold.*`
models for reporting and exploration. The Streamlit pages then consume the gold
layer for interactive analysis.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Build Paths

Normal usage runs dbt from the Streamlit app's `Load Selected` workflow after
loading the selected batches. The manual build command is:

```bash
uv run dbt build --project-dir dbt_eyeon_gold --profiles-dir dbt_eyeon_gold
```

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-batches-from-the-app -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §build-dbt-models-manually -->

## README-Level Lineage

The README-level lineage is:

```text
silver.raw_obs and metadata -> dbt_eyeon_gold -> gold.* -> Streamlit pages
```

Detailed model-by-model lineage requires reading the dbt project files directly.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Related

- [[wiki/component/dbt_gold]] — dbt component
- [[wiki/schema/gold_layer]] — gold schema documentation
- [[wiki/overview/data_flow]] — full pipeline overview
