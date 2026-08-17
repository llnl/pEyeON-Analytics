---
title: "Decision: Bronze/Silver/Gold Medallion Architecture"
type: decision
status: accepted
decided_on: 2026-06-26
confidence: high
grounded_by:
  - ../pEyeON-Analytics/README.md
policy: human-review-required
last_validated: 2026-06-26
repo_scope: pEyeON-Analytics
implementation_area: dbt-gold
format_domain: none
audience: mixed
source_paths: wiki/decision/bronze_silver_gold.md
tags: [medallion, bronze, silver, gold]
---

# Decision: Bronze/Silver/Gold Medallion Architecture

## Context

EyeOn emits one JSON observation per scanned file. `pEyeON-Analytics` needs to
retain raw inputs for traceability while also producing normalized and
analysis-friendly tables.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §overview -->

## Decision

Organize the analytics pipeline into three layers:

| Layer | Purpose |
| --- | --- |
| `bronze` | Raw JSON retained for traceability |
| `silver` | Normalized observation and metadata tables loaded from EyeOn JSON |
| `gold` | dbt models for reporting and exploration |

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §overview -->

## Rationale

The layered design separates traceable source retention from normalized tables
and reporting models. The README data-flow diagram maps this directly as
`bronze.raw_json`, `silver.raw_obs` and metadata, and `gold.*` outputs consumed
by Streamlit pages.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Consequences

The Streamlit app and manual workflow must run both load and modeling steps for
normal reporting use: DLT populates bronze and silver, then dbt builds gold.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §load-batches-from-the-app -->

## Alternatives Considered

The README does not document alternatives such as a single flat table, raw JSON
only, or direct Streamlit analysis over unmodeled JSON.
