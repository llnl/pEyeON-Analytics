---
title: "Current State Analysis: Cleanup Streamlit App"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/EyeOnData.py
  - ../pEyeON-Analytics/utils/app_init.py
  - ../pEyeON-Analytics/utils/batches.py
  - ../pEyeON-Analytics/common/dqautil.py
  - ../pEyeON-Analytics/common/page_frags.py
  - ../pEyeON-Analytics/extras/streamlit_box_ui.py
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/cleanup-streamlit-app/current_state.md
tags: [feature-work, streamlit, ui, cleanup, analysis]
---

# Current State Analysis: Cleanup Streamlit App

Analysis of the tracked app (`EyeOnData.py`, `pages/`, `utils/`) and the
untracked `common/` and `extras/` directories, as of 2026-08-17 on `main`
(02d0745).

## Architecture Overview

Entry point `EyeOnData.py` is 11 lines: `app_base_config()` +
`sidebar_config(app_pages())`. If a DuckDB exists it immediately
`st.switch_page`s to the Summary page; otherwise it renders a database-init
form (utility ID, dataset path, batch selector, DB path validation).
<!-- GROUND_TRUTH: ../pEyeON-Analytics/utils/utils.py §app_base_config -->

Six pages are registered in a hand-rolled `Page` dataclass list:
EyeOn Summary, X509 Certificates, Observation Hierarchy, Browse/Search
Observations, Schema Inspector (Schema Blame), Debug Tools.
<!-- GROUND_TRUTH: ../pEyeON-Analytics/pages/pages.py §app_pages -->

Every page repeats the same boilerplate: `st.set_page_config(...)`,
`sidebar_config(app_pages())`, a `LandingPage(BasePageLayout)` class, and a
`main()` + `__main__` stanza. The sidebar (logo, title, page links, schema
chooser, root-table selector, table-hierarchy expander, clear-selections
button) is rebuilt by each page via `sidebar_config`/`_db_settings`.
<!-- GROUND_TRUTH: ../pEyeON-Analytics/utils/utils.py §sidebar_config -->

### Page inventory

| Page | What it does | Notable |
|---|---|---|
| `EyeOnSummary.py` | Batch dashboard from `gold.batch_summary` (KPIs, per-utility bar charts) + "All Batches" loader | Cleanest page; uses `st.bar_chart` |
| `certs.py` | Certificate dashboards from gold cert marts | Mixes `st.bar_chart` and Altair; checks 9 required models |
| `ObservationHierarchy.py` | Parent/child tree over `silver.raw_obs` with recursive CTEs, metadata-type summary, drilldown | Builds a UNION over all silver `metadata_*` tables per render; TODO.md flags container-hierarchy correctness |
| `BrowseDltData.py` | Recursive master-detail explorer driven by `utils/schema_ext.EnrichedTable` | Depends on sidebar's `st.session_state.root_table_selector`; contains `bronze.raw_json` "hack"; "Inspect File" shells out to VS Code (`code`) |
| `Schema_Blame.py` | Schema-change changelog explorer over `utils/schema_blame` | Sidebar diverges from other pages (own controls) |
| `debug_page.py` | Ad-hoc SQL box, session-state dump, DuckDB UI toggle | Hand-rolled shadow-state hacks for cross-page widget persistence |

## Structural Debt (cleanup targets)

1. **Vestigial OO layer.** `BasePageLayout` is an ABC whose only method wraps
   `page_content()`; `display_page()` is never called, and five of six page
   classes are all named `LandingPage`. The class layer adds nothing to
   Streamlit's execution model.
   <!-- GROUND_TRUTH: ../pEyeON-Analytics/pages/_base_page.py -->
2. **Boilerplate duplication.** Per-page `set_page_config` + `sidebar_config`
   + registry import. Streamlit's `st.navigation`/`st.Page` API now covers
   page registration, sidebar nav, and per-page titles natively.
3. **`utils/utils.py` is a grab-bag** (413 lines): app config, init form,
   batch-dir parsing, DLT-load orchestration, dbt runner, sidebar rendering,
   directory listing, and batch queries in one module — UI and pipeline
   concerns interleaved.
4. **Dead code.** `run_eyeon()` is never called and reads
   `st.session_state.data_dir`, which nothing sets.
   <!-- GROUND_TRUTH: ../pEyeON-Analytics/utils/utils.py §run_eyeon -->
5. **Fragile cross-page coupling.** `BrowseDltData` requires
   `st.session_state.root_table_selector` (set only by the sidebar widget);
   `_db_settings` hard-indexes `schema_list.index("silver")` (KeyError-style
   crash if silver is absent) and hardwires `raw_obs` as default root
   (flagged TODO in code).
6. **Widget state hacks.** Shadow-variable workarounds for Streamlit's
   multipage state clearing are re-implemented ad hoc in `debug_page`;
   legacy `common/utils.py` has generic `store_keys`/`load_keys` helpers
   that were never adopted.
7. **Inconsistent conventions.** File naming (`BrowseDltData.py` vs
   `certs.py` vs `Schema_Blame.py`), chart libraries (`st.bar_chart` vs
   Altair), and error-handling styles vary page to page.
8. **Feedback gaps** (from `TODO.md`, untracked): no in-app progress meter
   during DLT load; poor feedback for mount-path entry (`./mypath` works,
   `mypath` fails); "general page cleanup/better hierarchy browsing".

## `common/` — orphaned legacy layer (untracked)

No tracked code imports `common/`. It is the pre-DLT, parquet-view era app
layer, partially adapted to this repo (it imports `utils.config`), with clear
Wintap ancestry — `rawutil.py`'s first line is
`# wintappy.datautils.rawutil`.
<!-- GROUND_TRUTH: ../pEyeON-Analytics/common/rawutil.py §header -->

| File | Contents | Missed-feature candidates | Suggested disposition |
|---|---|---|---|
| `page_frags.py` | `auth_filter()` OneID login gate (`st.login`/`st.user`, dev-mode bypass); summary-metric fragment; new-batches scatter; debug expander | **Authentication** — the tracked app has no auth at all | Port auth decision to design; rest superseded by EyeOnSummary |
| `utils.py` | Sidebar with **user identity + logout**; **multi-dataset/database chooser**; `store_keys`/`load_keys` widget-state helpers; `valid_uuid` | Auth UX; dataset chooser; state helpers (would replace debug_page hacks) | Port selectively |
| `dqautil.py` | Parquet-view bootstrap (`observations`, `sigs_n_certs`, ...), raw-JSON view, `new_batches` anti-join, `convert_from_json` parquet ETL; named-SQL loader | The `--# name:` named-SQL-file pattern; new-batches detection | ETL superseded by DLT/dbt; pattern worth considering |
| `st_graph_util.py` | networkx→cytoscape conversion, graphml/JSON export, commented-out `st_cytoscape` display page | **Graph visualization** (cert chains, observation hierarchy) | Pairs with `extras/x509-graphs.ipynb`; candidate feature |
| `st_content_util.py` | Wintap process-summary display (`pid_hash`, `process_name`) | none | Discard (wrong domain) |
| `rawutil.py` | Wintappy parquet/glob utilities | none directly | Discard (superseded) |
| `queries.sql`, `cert_queries.sql`, `metadata_queries.sql` | Named queries: observation-time bucketing, sig/md feature summaries, RSA key sizes | **Observation-timeline chart**; os/arch metadata matrix | Queries target parquet-era views; port ideas to silver/gold |

## `extras/` — prototypes (four files untracked)

| File | Contents | Candidate |
|---|---|---|
| `streamlit_box_ui.py` (untracked) | 15-line Box folder/file lister using `eyeon.upload` + Box OAuth | **Box browsing page** — ties to open tension [[wiki/tension/box_vs_local]] |
| `x509-graphs.ipynb` (untracked) | Cert issuer→subject trust-graph analysis with networkx (connected components, cross-signing notes) | **Cert-chain graph page** (pairs with `common/st_graph_util.py`) |
| `Schema.ipynb` (untracked) | `schema_ext` exploration notebook; imports pre-`utils/` module paths (stale) | Dev doc for `utils/schema_ext`; refresh or discard |
| `DataLoading.drawio` (untracked) | Data-loading architecture diagram | Documentation asset; commit and/or reflect in wiki |
| `MinamalRows.ipynb`, `Schema_Blame.md` (tracked) | Existing extras | Unaffected |

## What already works well (preserve)

- Config-driven settings via `EyeOnData.toml` (`utils/config.py`) with the
  init-form persistence flow.
- `utils/schema_ext.EnrichedTable` overlay model powering the sidebar tree
  and master-detail explorer.
- EyeOnSummary's guarded queries (graceful "not available yet" states) and
  certs.py's required-model preflight — patterns worth standardizing.
- Schema Blame's materialize/changelog flow.

## Related

- [[wiki/work/cleanup-streamlit-app/brief]]
- [[wiki/work/cleanup-streamlit-app/references]]
- [[wiki/component/streamlit_app]] — canonical page to update at close-out
