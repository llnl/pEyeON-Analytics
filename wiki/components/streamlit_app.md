---
title: "Component: Streamlit App (EyeOnData)"
type: component
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
  - ../pEyeON-Analytics/pages/ObservationHierarchy.py
  - ../pEyeON-Analytics/pages/pages.py
policy: agent-editable
component: pEyeON-analytics
last_validated: 2026-06-30
tags: [streamlit, ui, batch-selection, hierarchy]
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

## Observation Hierarchy Page

<!-- GROUND_TRUTH: ../pEyeON-Analytics/pages/ObservationHierarchy.py -->
`pages/ObservationHierarchy.py` displays `silver.raw_obs` parent/child
relationships created by container and Binwalk extraction. Users choose a root
observation, usually a container or firmware image, then inspect descendants up
to a selected depth.

Because a single container can produce thousands of child observations, the page
does not render every row as an expanded tree by default. Instead, it introduces
a semantic summary layer grouped by metadata type plus fallback `kind` where
available, such as `opkg_file:control`, `text_file:shell`, `web_asset:html`, or
`elf_file:unspecified`. This is more useful than raw MIME type for EyeON because
metadata type is already the scanner's normalized interpretation, and `kind`
captures the generic fallback classes added for firmware filesystem artifacts.

The page provides:
- Root selection with child counts.
- Max-depth and row-limit controls.
- Summary metrics for rows, descendants, and semantic groups.
- A semantic summary table grouped by depth, metadata type, kind, extension, and MIME type.
- Drilldown rows for a selected semantic group.
- A limited hierarchy preview so large containers remain navigable.

## Related

- [[wiki/components/load_eyeon]] — DLT load triggered by normal workflow
- [[wiki/components/dbt_gold]] — dbt modeling triggered by normal workflow
- [[wiki/pipeline/cert_analysis]] — certificate data exploration
