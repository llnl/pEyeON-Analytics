---
title: "Verification: Cleanup Streamlit App"
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
source_paths: wiki/work/cleanup-streamlit-app/verification.md
tags: [feature-work, streamlit, ui, cleanup, verification]
---

# Verification: Cleanup Streamlit App

## Phase 1 — st.navigation migration + dead code removal (2026-08-17)

Environment note: the repo's `.venv` symlinks are unreadable on this box
(filesystem quirk; `uv run` fails to canonicalize `.venv/bin/python3`), and
the compiled `duckdb` wheel is cp313-only. Verification used a fresh env:
`UV_PROJECT_ENVIRONMENT=/tmp/eyeon-venv uv sync` and
`/tmp/eyeon-venv/bin/python` with `PYTHONPATH=.`.

## Test Commands

1. `python3 -m py_compile EyeOnData.py utils/utils.py pages/*.py`
2. Dangling-reference grep for removed symbols
   (`app_pages`, `BasePageLayout`, `sidebar_config`, `app_base_config`,
   `run_eyeon`, `pages.pages`, `_base_page`) across the app.
3. Streamlit AppTest harness (`streamlit.testing.v1.AppTest`, streamlit
   1.58.0) on the entrypoint for both startup paths — run against the NEW
   code and, via `git stash`, against the OLD code as a regression baseline.
4. AppTest direct runs of the five standalone-safe pages with a valid
   dataset path (temp `EyeOnData.toml` via `EYEON_EYEONDATA_TOML`).

## Manual Checks

- Diff review of the AST-assisted page transforms (boilerplate removal,
  one-level dedent, `__main__`/`__page__` guard).
- Confirmed `sidebar_db_chooser()` runs on every page via the entrypoint
  before `pg.run()`, preserving `st.session_state.root_table_selector` for
  BrowseDltData under normal navigation.

## Results

```
compile OK
dangling refs: none (grep exit 1)

[NEW] no-db path (init form): exceptions=0
    markdown[:2]=['# Initialize Database', 'Initialize Database']
    sidebar widgets: selectbox=0 text_input=3
[NEW] db-present path: exceptions=1
    exc: CannotRestorePipelineException: Pipeline with `pipeline_name=eyeon_metadata`
         ... No local pipeline state found in '/home/ubuntu/.dlt/pipelines/eyeon_metadata'

[OLD] no-db path (init form): exceptions=0
[OLD] db-present path: exceptions=1
    exc: CannotRestorePipelineException (identical)

Direct page runs (valid dataset path):
pages/EyeOnSummary.py: exceptions=none
pages/certs.py: exceptions=none
pages/ObservationHierarchy.py: exceptions=none
pages/Schema_Blame.py: exceptions=none
pages/debug_page.py: exceptions=none
```

The single db-present failure is byte-for-byte identical on old and new
code: `dlt.attach("eyeon_metadata")` inside `db.get_schema()` requires local
dlt pipeline state (`~/.dlt/pipelines/`), which exists on the dev's machine
but not this box. Environment limitation, not a Phase 1 regression.
Behavior parity confirmed on both paths.

## Deviations From Handoff

- None. (Scope settled in-session by the Architect; recorded in
  [[wiki/work/cleanup-streamlit-app/design]] §Phase 1.)

## Known Gaps

- **Pre-existing latent bug discovered (not fixed — out of Phase 1 scope):**
  `utils/utils.py §list_dirs` returns an empty fallback DataFrame with
  columns `["directory_name", "modified_time"]` but no `directory_path`,
  while `list_all_batches`' SQL references `d.directory_path` — a DuckDB
  Binder Error whenever the configured dataset path does not exist. Masked
  previously because the old sidebar crashed first on this box. Likely
  related to TODO.md's mount-path feedback item. One-line fix candidate for
  the next slice.
- Direct `streamlit run pages/BrowseDltData.py` (without the entrypoint)
  now lacks the sidebar-set `root_table_selector` key — the fragile
  coupling deliberately left alone this round; normal navigation via
  `EyeOnData.py` is unaffected.
- Full end-to-end run with real data (DLT state + gold models) not possible
  on this box; needs a machine where the pipeline has run.

## Follow-Ups

- ~~Fix `list_dirs` empty-frame columns~~ — approved and fixed in Phase 2
  Slice C.
- Phase 3+: UIX direction, coupling/schema-chooser fixes,
  `common/`/`extras/` dispositions (see brief).

---

## Phase 2 — shared-code refactor, 3 slices (2026-08-17)

Commits: a774056 (Slice A), 6933fd0 (Slice B), 85ef486 (Slice C).
Same environment workaround as Phase 1 (`/tmp/eyeon-venv`).

## Test Commands (Phase 2)

After every slice:
1. `python3 -m py_compile EyeOnData.py pages/*.py utils/*.py`
2. AST unused-import scan over changed files.
3. AppTest direct runs of the five standalone-safe pages plus
   EyeOnSummary, with a valid dataset path.
After Slice C additionally:
4. AppTest of the entrypoint on both startup paths.
5. AppTest of EyeOnSummary with the repo's real `EyeOnData.toml`
   (nonexistent dataset path) — the `list_dirs` bug repro.
6. `grep` for any remaining `utils.utils` references.

## Results (Phase 2)

```
compile OK (every slice)
unused-import scan: clean (after removing pd from BrowseDltData, EyeOnSummary)

AppTest after each slice — all of:
pages/certs.py: exceptions=none
pages/ObservationHierarchy.py: exceptions=none
pages/Schema_Blame.py: exceptions=none
pages/debug_page.py: exceptions=none
pages/EyeOnSummary.py: exceptions=none

Entrypoint after Slice C:
no-db path (init form): exceptions=0 (form renders, 3 text inputs)
db-present path: exceptions=1 — identical pre-existing
  CannotRestorePipelineException (missing local dlt state; matches the
  Phase 1 old-code baseline)

list_dirs repro (real toml, nonexistent dataset path):
before fix: DuckDB Binder Error 'Table "d" does not have a column named
  "directory_path"' — after fix: exceptions=[] and the page renders.

utils.utils references remaining: none
```

## Deviations From Handoff (Phase 2)

- `pages/certs.py`: the missing-models preflight now runs before the
  `fct_observation_certificates` count query — the original ordering
  queried a gold model before checking whether models exist, so a missing
  model crashed the page instead of showing the intended warning.
- `utils/loader.py §load_me_some_data` clears `st.cache_data` after dbt
  runs so cached MetadataCatalog discovery sees newly loaded types (the
  catalog now shares one cache where previously only search_forms cached).
- Display consistency: Schema_Blame's heatmap short names now also strip
  the `_file` suffix (shared `catalog.short_name`), and BrowseDltData's
  "table not found" message renders as `st.warning` instead of `st.write`.
- `list_dirs` fix extended to the scandir-found-nothing path (same
  missing-column defect), not just the guard-clause empty frame.

---

## Phase 3 — dashboard feature pages (2026-08-17, UNCOMMITTED)

Implemented per Architect approval of dashboard ideas 1–9; intentionally
left uncommitted for Architect testing with sample datasets.

## Test Commands (Phase 3)

1. Local schema/value-domain inspection of `database/eyeon.duckdb`
   (read-only) to ground SQL: PE/ELF column names, `authenticode_integrity`
   values, hash placeholder values, cert mart columns.
2. Change-detection SQL validated read-only against silver before writing
   the dbt model; then `dbt run --select mart_batch_changes` materialized
   only the new view into the local DB.
3. `python3 -m py_compile` on all pages + entrypoint.
4. AppTest direct runs of all five new pages + updated EyeOnSummary +
   the four prior pages, against the local DB (real data: 4 utilities,
   4,861 observations).

## Results (Phase 3)

```
dbt: PASS=1 (mart_batch_changes)
AppTest — all pages exceptions=none, real metrics rendered:
Inventory: Unique Content 2,529 / Observed Files 4,861 / Dedup 1.92x /
  Shared Binaries 172 / Containers 15 / Max Depth 3
SecurityPosture: PE 7 / Signed OK 2 / Problems 5 / Certs 3 / Expired 1 /
  Files w/ Expired Cert 7
ChangeDetection: 3 batches compared / 718 new / 2,557 disappeared (test utility)
VariantClusters: 5 telfhash clusters (largest 8 variants) after
  placeholder filtering; no real imphash clusters in this dataset
DataQuality: Coverage 73.5% / Errors 1 / Multi-Type 2,119 / Unmodeled 2
```

Issues found and fixed during verification:

- Initial "Unique Files" KPI conflated uuids with content; corrected to
  `count(distinct sha256)` with an explicit dedup-factor definition.
- imphash/telfhash placeholder values (`N/A`, `-`, `tnull`, `''`) formed
  one giant fake cluster; now excluded (see dashboard_ideas caveats).
- `st.page_link` raised on direct page runs (no navigation context);
  wrapped as `st_widgets.page_link` which degrades to a caption.

## Deviations From Handoff (Phase 3)

- Navigation grouped into Overview/Analysis/Admin sections (11 pages were
  unwieldy as a flat list) — easily flattened if the Architect prefers.
- Load timeline (a review finding, not one of the numbered ideas) included
  on the Data Quality page.
- `gold.mart_batch_changes` was materialized into the local DB via a
  targeted dbt run so verification used real data — the same idempotent
  action the app's loader performs.

## Known Gaps (Phase 3)

- **Architect-reported regression (fixed 2026-08-17):** the sidebar nav
  menu never rendered — `.streamlit/config.toml` had
  `client.showSidebarNavigation = false`, set in the pre-`st.navigation`
  era to hide the auto-generated `pages/` nav in favor of the hand-drawn
  `page_link` menu. That setting also suppresses the `st.navigation` menu.
  Flipped to `true`. Root cause of the test escape: AppTest exercises page
  scripts, not client-side sidebar chrome, so Phase 1's parity checks could
  not see the missing menu.
- Entrypoint-context navigation could not be AppTested on this box (the
  pre-existing missing-dlt-state failure in the sidebar chooser persists);
  direct page runs + nav registration compile are the verification here.
- ssdeep is displayed but not pairwise-scored in Variant Clusters.
- Change detection keys on sha256 only; renamed-but-identical files are
  invisible to it by design, and content-similar (rebuilt) files appear as
  new+disappeared pairs.

## Known Gaps (Phase 2)

- `search_forms.search_raw_obs` now returns the SQL with a
  `/* params=[...] */` suffix so BrowseDltData's query-signature hashing
  distinguishes filters; any future caller wanting executable SQL alone
  should split on the comment.
- Full end-to-end run with real dlt state still requires the Architect's
  machine (unchanged from Phase 1).
