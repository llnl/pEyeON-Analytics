---
title: "Implementation Plan: Cleanup Streamlit App"
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
status: draft
source_paths: wiki/work/cleanup-streamlit-app/implementation_plan.md
tags: [feature-work, streamlit, ui, cleanup, implementation]
---

# Implementation Plan: Cleanup Streamlit App

Phased; this plan covers Phase 1 (see
[[wiki/work/cleanup-streamlit-app/design]] for settled scope).

## Scope

Phase 1: `st.navigation` migration + dead-code removal. No behavior changes
beyond navigation mechanics, tab titles, and sidebar ordering.

## Steps

1. Rewrite `EyeOnData.py`: single `set_page_config`, sidebar logo/title,
   `st.Page` list (six pages, Summary default) behind `db.exists()`, init
   page fallback, `sidebar_db_chooser()` before `pg.run()`.
2. Strip boilerplate from all six page files (set_page_config,
   sidebar_config, registry imports, class wrapper) and end each with a
   `main()` call guarded for `__main__`/`__page__`.
3. Delete `pages/pages.py` and `pages/_base_page.py`.
4. Remove `run_eyeon`, `app_base_config`, `sidebar_config`, and unused
   imports from `utils/utils.py`.
5. Verify (see below) and record in
   [[wiki/work/cleanup-streamlit-app/verification]].

## Files Likely To Change

`EyeOnData.py`; `pages/EyeOnSummary.py`, `pages/certs.py`,
`pages/ObservationHierarchy.py`, `pages/BrowseDltData.py`,
`pages/Schema_Blame.py`, `pages/debug_page.py`; `utils/utils.py`;
deletions: `pages/pages.py`, `pages/_base_page.py`.

## Tests To Add Or Update

No test suite exists for the app layer. Verification is:
- `python -m py_compile` on all changed files.
- Static check: no remaining references to deleted symbols
  (`app_pages`, `BasePageLayout`, `sidebar_config`, `app_base_config`,
  `run_eyeon`).
- App smoke launch (`streamlit run EyeOnData.py` headless) on both the
  no-DB (init form) and DB-present (full nav) paths, environment permitting
  (the local `.venv` symlinks are currently broken — see verification).

## Migration Or Compatibility Notes

- Per-page browser-tab titles are replaced by the app-wide title.
- Sidebar ordering changes: nav menu on top, logo/title/DB chooser below.
- `streamlit run pages/<page>.py` direct runs still work via the
  `__main__` guard.

## Rollback Plan

Single commit on `grantj-cleanup-streamlit`; revert the commit.

## Done Checklist

- [x] EyeOnData.py rewritten around st.navigation
- [x] Six pages de-boilerplated
- [x] pages/pages.py and pages/_base_page.py deleted
- [x] run_eyeon/app_base_config/sidebar_config removed from utils/utils.py
- [x] Static verification clean (compile + no dangling references)
- [x] Smoke launch attempted and results recorded (AppTest, old-vs-new baseline)
- [x] verification.md updated; log.md entry appended
- [x] Committed on grantj-cleanup-streamlit
