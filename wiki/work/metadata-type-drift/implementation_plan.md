---
title: "Metadata Type Drift: Implementation Plan"
type: pipeline
confidence: medium
grounded_by:
  - dbt_eyeon_gold/models/gold/all_metadata.sql
  - load_eyeon.py
  - pages/Schema_Blame.py
policy: agent-editable
last_validated: 2026-07-07
component: pEyeON-analytics
tags: [dlt, dbt, gold, silver, metadata]
---

## Goal

Make the "silver discovers types" vs "gold curates known types" flow visible and easy to debug.

## Checklist

1. Add a dbt gold model `gold.metadata_type_drift` that compares discovered silver metadata tables to curated `gold.all_metadata`. (Completed)
1. Surface `gold.metadata_type_drift` in the Streamlit Schema Blame page. (Completed)
1. Document the curation flow (how to add a new curated type) and the drift signal. (Completed)

## Notes

`gold.all_metadata` is the curated gate: only metadata types unioned there are treated as "known" for downstream analytics.
