---
title: "Implementation Plan: Cleanup Streamlit App"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/EyeOnData.py
policy: agent-editable
last_validated: 2026-08-27
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

## Phase 2 Steps (shared-code refactor, settled 2026-08-17)

Per [[wiki/work/cleanup-streamlit-app/refactoring_candidates]]: all three
slices approved by the Architect, plus the `list_dirs` fix ride-along.

1. Slice A: `utils/metadata_catalog.py` (MetadataCatalog class),
   `utils/queries.py` (Query façade), `utils/sqlutil.py`; migrate all
   duplicated discovery/naming/query sites.
2. Slice B: `utils/st_widgets.py` (select_rows, metric_row,
   shadow_init/shadow_sync); extend sqlutil (ilike_pattern); parameterize
   search_forms user input.
3. Slice C: split `utils/utils.py` → `utils/{batches,loader,app_init,sidebar}.py`;
   fix `list_dirs` empty-frame columns.

## Phase 3 Done Checklist (dashboard pages)

- [x] Five new pages (Inventory, SecurityPosture, DataQuality,
      ChangeDetection, VariantClusters) implemented with guarded queries
- [x] gold.mart_batch_changes dbt model added and validated on real data
- [x] Sectioned navigation (Overview/Analysis/Admin) in EyeOnData.py
- [x] EyeOnSummary Posture & Quality KPI row with page links
- [x] showSidebarNavigation regression found by Architect and fixed
- [x] AppTest clean on all ten pages; artifacts updated
- [x] Committed after Architect sample-dataset testing

## Phase 2 Done Checklist

- [x] Slice A implemented, migrated, AppTest clean (commit a774056)
- [x] Slice B implemented, migrated, AppTest clean (commit 6933fd0)
- [x] Slice C implemented, utils/utils.py retired, AppTest clean (commit 85ef486)
- [x] list_dirs fix verified against the previously failing repro
- [x] verification.md Phase 2 section recorded; log.md entry appended
- [x] Committed on grantj-cleanup-streamlit

## Status: Paused 2026-08-27 — Resume Points

Phases 1–3 are implemented, verified, and committed on
`grantj-cleanup-streamlit`. The hierarchy-display spike
([[wiki/work/cleanup-streamlit-app/spike]]) is complete with a hybrid
recommendation, and the previously untracked reference material (`TODO.md`,
legacy `common/`, `extras/` prototypes and notebooks) is now committed on
this branch so the feature docs' `grounded_by` paths resolve from git.

Remaining work when this feature resumes:

- [ ] Spike follow-ups (Architect decisions): try
      `extras/spike_hierarchy_views.py`, pick which surfaces get the
      document view first (BrowseDltData, ObservationHierarchy, or a new
      page), and choose the default detail source (bronze vs silver).
- [ ] If the spike is adopted: promote the generic re-nester into `utils/`
      (next to `schema_ext`) with tests, per spike.md follow-ups.
- [ ] Execute `common/` and `extras/` per-file dispositions (brief
      acceptance criterion; candidate table in
      [[wiki/work/cleanup-streamlit-app/current_state]]) — port, spin off,
      or discard each.
- [ ] Settle the open auth question (OneID `st.login` from legacy
      `common/page_frags.py`).
- [ ] Overlapping `TODO.md` items: mount-path feedback in the init form,
      container-hierarchy correctness in ObservationHierarchy.
- [ ] On completion: promote durable facts to
      [[wiki/component/streamlit_app]] per the brief's Done When.
