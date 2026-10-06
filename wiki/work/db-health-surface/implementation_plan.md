---
title: "Implementation Plan: DB Health Surface"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/utils/dlt_state.py
  - ../pEyeON-Analytics/utils/utils.py
  - ../pEyeON-Analytics/pages/debug_page.py
policy: agent-editable
last_validated: 2026-08-31
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: developer
status: draft
source_paths: wiki/work/db-health-surface/implementation_plan.md
tags: [feature-work, streamlit, dlt, state-consistency]
---

# Implementation Plan: DB Health Surface

## Scope

Surface `_meta.consistency_log` and the doctor report in the Streamlit app;
all logic in Streamlit-free helpers.

## Steps

1. `utils/dlt_state.py`:
   - `recent_events(conn, limit=10)` → list of (ts, event, detail) rows,
     newest first; `[]` when `_meta.consistency_log` is absent.
   - `unresolved_instance_change(conn, datasets=("bronze","silver"))` →
     dict {ts, detail} for the latest `db_instance_changed` event when no
     `_dlt_loads` row in any dataset is newer than it; else `None`.
     Guards: missing `_meta` / missing `_dlt_loads` tables.
2. `load_eyeon.py`: `doctor_text(conn) -> str` factored out of `doctor()`;
   `doctor()` prints it. Reuses `_build_pipeline` + `_doctor_dataset_roots`.
3. `utils/utils.py`: `sidebar_db_health()` — warning banner (from
   `unresolved_instance_change`) + collapsed "DB Health" expander with the
   events dataframe; called from `sidebar_db_chooser()` inside the existing
   `db.exists()` / `st.sidebar` context. Wrap in try/except so a health
   query can never break page rendering.
4. `pages/debug_page.py`: "DLT State Doctor" expander with a button that
   calls `load_eyeon.doctor_text(db.get_conn())` into `st.code`, try/except
   to `st.error`.
5. Tests in `tests/test_dlt_state_consistency.py`: helpers against real
   DuckDB fixtures (no `_meta`; events; unresolved vs resolved change), and
   switch the doctor regression test to `doctor_text`.

## Files Likely To Change

utils/dlt_state.py, load_eyeon.py, utils/utils.py, pages/debug_page.py,
tests/test_dlt_state_consistency.py

## Tests To Add Or Update

See step 5; plus import smoke checks for `utils.utils` / `pages.debug_page`.

## Migration Or Compatibility Notes

Pre-`_meta` databases: helpers return empty/None; expander shows a caption.
No schema or data changes.

## Rollback Plan

Revert the commit; no persisted state involved.

## Done Checklist

- [x] Helpers implemented + tested
- [x] doctor_text refactor (CLI unchanged)
- [x] Sidebar wiring
- [x] Debug page expander
- [x] Full suite green (20/20); verification.md filled
- [x] wiki/log.md entry
