---
title: "Dashboard Ideas: Cleanup Streamlit App"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/dbt_eyeon_gold/models/gold/gold_files.sql
  - ../pEyeON-Analytics/dbt_eyeon_gold/models/gold/batch_summary.sql
  - ../pEyeON-Analytics/dbt_eyeon_gold/models/gold/mart_batch_changes.sql
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/cleanup-streamlit-app/dashboard_ideas.md
tags: [feature-work, streamlit, dashboard, metrics, supply-chain]
---

# Dashboard Ideas: Cleanup Streamlit App

Engineer review of the dashboard/summary views (2026-08-17). All nine ideas
were approved by the Architect and implemented as Phase 3 (see
[[wiki/work/cleanup-streamlit-app/design]] §Phase 3).

## Review findings

- **Three gold models were built but never surfaced**: `gold.gold_files`
  (per-file inventory with all hashes and `authenticode_integrity`),
  `gold.gold_filetype_counts`, and `gold.obs_with_multiple_uuid`.
- The "Metadata Types" metric held a comma-joined list; metrics are for
  numbers.
- No time dimension anywhere despite `run_ts`/`observation_ts` (the legacy
  parquet-era app had both an observation timeline and a new-batches
  scatter).
- `batch_summary.num_rows` measures scan volume, not inventory size; the
  distinct-content count and dedup factor were missing.
- Risk data (signing, cert expiry) existed but was buried as mid-page
  charts, never as top-line numbers.

## Approved ideas → implementation map

| # | Idea | Where it landed |
|---|---|---|
| 1 | Cross-utility overlap (shared binaries = shared exposure) | Inventory §Cross-Utility Overlap |
| 2 | Signing posture per utility (restores lost legacy `signed_count`) | Security Posture §Code-Signing |
| 3 | Cert hygiene KPIs (expired / expiring ≤12mo / weak RSA / in-use) | Security Posture §Cert Hygiene + Summary "Posture & Quality" row |
| 4 | Filetype composition + data-quality panel (errors, coverage, multi-type, drift) | Inventory §Filetype Composition; Data Quality page |
| 5 | Largest / most-duplicated artifacts | Inventory §Top Artifacts |
| 6 | Batch-over-batch change detection (new mart) | Change Detection page + `gold.mart_batch_changes` |
| 7 | Variant clusters via imphash/telfhash | Variant Clusters page |
| 8 | OS/arch matrix (restores lost legacy `md_feature_summary`) | Inventory §OS/Architecture Matrix |
| 9 | Container/firmware stats from `parent` lineage | Inventory §Containers & Firmware |
| — | Load timeline (review finding, not numbered) | Data Quality §Load Timeline |

## Data caveats discovered during implementation

- `imphash` uses the literal placeholder `'N/A'` for non-PE files and `''`
  for PE files without import tables; `telfhash` uses `'-'` and `'tnull'`.
  Cluster queries must exclude these or they form one giant fake cluster.
  <!-- GROUND_TRUTH: ../pEyeON-Analytics/pages/VariantClusters.py §PLACEHOLDERS -->
- `gold.gold_files` is one row per **uuid** (file instance), not per
  content hash; "unique content" requires `count(distinct sha256)`.
- `authenticode_integrity` observed values: `OK`,
  `BAD_DIGEST | BAD_SIGNATURE`, NULL.
- `rsa_key_size` is a varchar like `'4096 bits'`; numeric comparisons need
  `regexp_extract` + `try_cast`.

## Future candidates (not yet approved)

- Pairwise ssdeep similarity scoring within clusters.
- Known-hash/advisory matching (NSRL, vendor advisories) — needs external
  data EyeON doesn't collect today.
- Vendor/publisher rollup from cert subjects on the inventory page.
