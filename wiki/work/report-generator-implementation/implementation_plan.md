---
title: "Implementation Plan: Report Generator Implementation"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/work/report-generator-implementation/brief.md
  - ../pEyeON-Analytics/extras/spike_report_typst.py
  - ../pEyeON-Analytics/extras/spike_report_data.py
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: developer
status: draft
source_paths: wiki/work/report-generator-implementation/implementation_plan.md
tags: [feature-work, reporting, implementation, typst, streamlit, cli]
---

# Implementation Plan: Report Generator Implementation

## Scope

Per [[wiki/work/report-generator-implementation/brief]]: `reports/`
package + `eyeon-report` CLI + Streamlit Reports page, two report types,
PDF only, plotly charts. Reference pattern:
`extras/spike_report_typst.py` (keep the spike files untouched).

## Module Layout (settled by the Engineer)

```
reports/
  __init__.py        # public API: available_reports(), render(name, con=None, **params) -> bytes
  queries.py         # SQL -> plain dicts; adapted from extras/spike_report_data.py
                     #   batch_change_data(con, utility=None, detail_limit=40|None)
                     #   dossier_data(con, uuid)
  charts.py          # plotly figures -> SVG bytes (kaleido); grouped bar of
                     #   changes by utility/type, ported from the spike's matplotlib
  render.py          # typst.compile wrapper: dict-of-sources in memory
                     #   {main.typ, common.typ, data.json, chart.svg}; returns PDF bytes
  templates/
    common.typ       # page setup, styled-table, chunked() hash helper, metric tiles
    batch_changes.typ
    dossier.typ
  cli.py             # argparse: eyeon-report {batch-changes|dossier}; --db override,
                     #   -o/--output required; batch-changes: --utility, --full/--limit N;
                     #   dossier: --uuid (required)
pages/Reports.py     # Streamlit page: report picker + params + generate + st.download_button
```

Key mechanics (all proven in the spike):

- Templates load via `importlib.resources` from `reports/templates/` and
  compile with `typst.compile({...})` — in-memory, no temp files, no
  `@preview` packages.
- Data crosses as `data.json`; charts as `chart.svg`. The `chunked()`
  helper from the spike handles 64-char digests in table cells.
- DB connection: default `duckdb.connect(str(duckdb_path()), read_only=True)`
  from `utils/config.py`; both CLI (`--db` override) and page pass/derive a
  connection, keeping `reports/` free of Streamlit imports.
- Dossier observation picker on the page: `select uuid, filename from
  silver.raw_obs order by filename` into a selectbox (uuid in label).

## Steps

1. **rgi-01 — reports package core.** Add deps (`typst`, `plotly`,
   `kaleido`) to `pyproject.toml`; create `reports/` per the layout; port
   spike queries verbatim-where-possible (parameterize utility filter and
   dossier uuid — the spike hardcoded "first signed observation"); port the
   chart to plotly with matching content; split the spike's single Typst
   source into `common.typ` + two report templates; `render()` returns PDF
   bytes.
2. **rgi-02 — CLI.** `reports/cli.py` with argparse; register
   `eyeon-report = "reports.cli:main"` in `[project.scripts]`; friendly
   errors for missing db/uuid.
3. **rgi-03 — Reports page.** `pages/Reports.py` following the post-Phase-1
   page pattern (`main()` guarded for `__main__`/`__page__`, no
   set_page_config); register in `EyeOnData.py` navigation under Analysis;
   generate on click → `st.download_button` with
   `mime="application/pdf"` and a timestamped filename.
4. **rgi-04 — verification + wiki.** Run the Test Plan checks; record full
   output and deviations in `verification.md`; tick this plan's checklist;
   append `wiki/log.md`.

## Files Likely To Change

New: `reports/` (7 files), `pages/Reports.py`. Modified: `pyproject.toml`,
`uv.lock` (regenerated, gitignored), `EyeOnData.py`. Untouched:
`extras/spike_report_*.py`, dbt models, loader.

## Tests To Add Or Update

No app-layer test suite exists; scripted verification per the brief's Test
Plan:

- `uv run python -m py_compile` over all new/changed files.
- CLI renders: both commands against `database/eyeon.duckdb`; assert output
  starts with `%PDF` and pypdf page count > 2.
- AppTest smoke: existing pattern extended to the Reports page (page loads,
  no exceptions; generate path exercised with the sample dossier uuid).
- Offline: strace connect-trace around one CLI render (0 non-local
  connects), or documented by-design argument if strace unavailable.

## Migration Or Compatibility Notes

- **Environment**: the repo is sshfs-mounted from macOS and the in-repo
  `.venv` contains unresolvable Mac symlinks. Use
  `export UV_PROJECT_ENVIRONMENT=/home/ubuntu/.venvs/peyeon-analytics`
  before any `uv` command (venv already built there).
- `kaleido` bundles a headless renderer; first `to_image` call may be
  slow — acceptable.
- Navigation change mirrors the Phase-3 sectioned-nav pattern from
  cleanup-streamlit-app.

## Rollback Plan

Single commit on `grantj-seth-clauding`; revert the commit. Spike files
remain the fallback reference.

## Done Checklist

- [ ] rgi-01 reports package core (deps, queries, charts, templates, render)
- [ ] rgi-02 eyeon-report console script
- [ ] rgi-03 Reports page + navigation registration
- [ ] rgi-04 verification.md complete (commands, full output, deviations)
- [ ] wiki/log.md entry appended
- [ ] Committed on grantj-seth-clauding
