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

- Fix `list_dirs` empty-frame columns (with Architect approval).
- Phase 2+: UIX direction, utils/utils.py split, coupling/schema-chooser
  fixes, `common/`/`extras/` dispositions (see brief).
