---
title: "Component: Streamlit App (EyeOnData)"
type: component
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: agent-editable
component: pEyeON-analytics
last_validated: 2026-06-26
tags: [streamlit, ui, batch-selection]
---

# Component: Streamlit App (EyeOnData)

## Purpose

The Streamlit app is the preferred user interface for loading and exploring
EyeOn batches in `pEyeON-Analytics`. The README describes Streamlit as the tool
for exploring batches, metadata, schema changes, and certificate data.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §top -->

## Launch

The app is launched with:

```bash
uv run streamlit run EyeOnData.py
```

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §launch-the-streamlit-app -->

## Load Selected Workflow

The preferred workflow is to run the load pipeline from the app:

1. Choose the dataset root or database location if prompted.
2. Select one or more EyeOn batch directories.
3. Click `Load Selected`.

The README states that this path handles both the DLT load and dbt modeling
steps for normal usage.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-batches-from-the-app -->

## Pages and Utilities

The repository layout identifies `pages/` as Streamlit pages and `utils/` as
shared app and schema utilities.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §repo-layout -->

## Related

- [[wiki/components/load_eyeon]] — DLT load triggered by normal workflow
- [[wiki/components/dbt_gold]] — dbt modeling triggered by normal workflow
- [[wiki/pipeline/cert_analysis]] — certificate data exploration
