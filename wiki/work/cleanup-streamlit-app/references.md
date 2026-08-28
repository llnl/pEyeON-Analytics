---
title: "Feature References: Cleanup Streamlit App"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/EyeOnData.py
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/cleanup-streamlit-app/references.md
tags: [feature-work, streamlit, ui, cleanup, references]
---

# Feature References: Cleanup Streamlit App

## Live Repo Sources

Tracked app:
- `../pEyeON-Analytics/EyeOnData.py` — entry point
- `../pEyeON-Analytics/pages/pages.py` — page registry
- `../pEyeON-Analytics/pages/_base_page.py` — vestigial base class
- `../pEyeON-Analytics/pages/{EyeOnSummary,certs,ObservationHierarchy,BrowseDltData,Schema_Blame,debug_page}.py`
- `../pEyeON-Analytics/utils/utils.py` — app config, sidebar, load orchestration
- `../pEyeON-Analytics/utils/{config,db,schema_ext,schema_blame,search_forms}.py`
- `../pEyeON-Analytics/TODO.md` (untracked) — overlapping quick-fix list

Untracked legacy/prototypes (local working tree only — cited by path; they
have no committed state to re-read from git):
- `../pEyeON-Analytics/common/{page_frags,utils,dqautil,st_graph_util,st_content_util,rawutil}.py`
- `../pEyeON-Analytics/common/{queries,cert_queries,metadata_queries}.sql`
- `../pEyeON-Analytics/extras/{streamlit_box_ui.py,x509-graphs.ipynb,Schema.ipynb,DataLoading.drawio}`

## External Sources

- Streamlit multipage API (`st.navigation`, `st.Page`) — candidate
  replacement for the hand-rolled registry and sidebar; see
  https://docs.streamlit.io/develop/concepts/multipage-apps
- Streamlit auth (`st.login`/`st.user`) — used by legacy `common/page_frags.py`
- Wintappy (`wintappy.datautils.rawutil`) — origin of `common/rawutil.py`

## Related Wiki Pages

- [[wiki/component/streamlit_app]] — canonical component page (update at close-out)
- [[wiki/component/load_eyeon]], [[wiki/component/dbt_gold]] — pipeline the app orchestrates
- [[wiki/pipeline/cert_analysis]] — certificate marts behind certs.py
- [[wiki/tension/box_vs_local]] — open tension the Box UI prototype touches
- [[wiki/pipeline/metadata_curation]] — drift view surfaced in Schema Blame

## Libraries And APIs

streamlit, duckdb, pandas, altair, dlt, dbt (dbtRunner), networkx
(legacy graph prototypes), streamlit `st_cytoscape` (commented-out legacy).

## Notes

- Nothing tracked imports `common/`; it is safe to treat as a
  feature-candidate quarry rather than live code.
- `run_eyeon()` in `utils/utils.py` is dead (never called; reads a session
  key nothing sets).
- Auth exists only in the legacy layer; the tracked app is unauthenticated.
