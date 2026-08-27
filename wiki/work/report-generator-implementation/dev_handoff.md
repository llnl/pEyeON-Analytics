---
title: "Dev Handoff: Report Generator Implementation"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/work/report-generator-implementation/implementation_plan.md
policy: human-review-required
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: developer
status: reviewed
source_paths: wiki/work/report-generator-implementation/dev_handoff.md
tags: [feature-work, reporting, dev-handoff]
---

# Dev Handoff: Report Generator Implementation

**Status:** Approved
**Architect Approval:** Approved 2026-08-27

## Copy/Paste Prompt

Use this prompt to hand the work to a Developer session:

    As the Developer, implement the feature: report-generator-implementation.

    Use these wiki files as the handoff context:

    - wiki/work/report-generator-implementation/dev_handoff.md
    - wiki/work/report-generator-implementation/implementation_plan.md
    - wiki/work/report-generator-implementation/brief.md
    - wiki/decision/2026-08-27-report-generation-typst.md

    Reference pattern (read, do not modify):
    - extras/spike_report_typst.py
    - extras/spike_report_data.py

    Goal: ship the batch-change and dossier PDF reports (Typst) via a new
    reports/ package, an eyeon-report console script, and a Streamlit
    Reports page, per the approved plan.

    Environment: run `export UV_PROJECT_ENVIRONMENT=/home/ubuntu/.venvs/peyeon-analytics`
    before any uv command (the in-repo .venv has unresolvable Mac-side
    symlinks over sshfs — do not try to delete it).

    Before editing, read AGENTS.md and confirm this handoff is Approved.

## Handoff Summary

Everything architectural is settled: engine (ADR), module layout, CLI
shape, page surface, dependency set (`typst`, `plotly`, `kaleido` —
Architect-approved), and mechanics proven in the spike (in-memory
`typst.compile`, JSON data file, SVG charts, hash chunking). The work is
porting the spike into a clean package, parameterizing the two queries,
swapping the chart to plotly, and wiring two thin front-ends onto one
rendering core. Follow `implementation_plan.md` §Module Layout and §Steps
exactly; deviations go in `verification.md`.

## Primary Sources For The Dev Agent

- `implementation_plan.md` — layout, steps rgi-01..rgi-04, test plan.
- `extras/spike_report_typst.py` — the Typst template to split into
  `common.typ` + `batch_changes.typ` + `dossier.typ`, incl. the `chunked()`
  hash helper and styled-table.
- `extras/spike_report_data.py` — the queries to adapt (parameterize
  `utility` filter and dossier `uuid`; drop the "first signed observation"
  auto-pick except as the page's default selection).
- `EyeOnData.py` + any existing page in `pages/` — the post-cleanup page
  and navigation pattern to match.
- `utils/config.py §duckdb_path` — database resolution.

## Recommended First Implementation Slice

rgi-01 (package core) end-to-end for `batch-changes` only, proven by a
python one-liner writing PDF bytes to a file — then dossier, then CLI, then
page. Keep each of rgi-01..rgi-03 individually verifiable.

## Non-Goals For This Slice

HTML output, CSV export, inventory/security-posture reports, artifact
persistence, template polish beyond the spike's look, any change to
`extras/`, dbt, the loader, or `../pEyeON`.

## Testing Expectations

Per the plan's Tests section: py_compile sweep; both CLI renders against
`database/eyeon.duckdb` with `%PDF` magic + pypdf page-count assertions;
AppTest smoke including the Reports page; offline evidence (strace connect
trace preferred). Record every command and its FULL output in
`verification.md` — the verification page is the audit artifact. Report
full test output to the Architect; do not summarize pass/fail.

## Closeout Instructions

- Update wiki/work/report-generator-implementation/verification.md with
  commands run, full output, and Deviations From Handoff (`None` if none).
- Tick the implementation_plan.md done checklist as units complete; fill
  `actual_hours` per unit in metrics.md where derivable.
- Append a concise entry to wiki/log.md.
- Commit on grantj-seth-clauding (do not push without explicit direction).
- Leave close-out (Velocity computation, rollup row, promotion to canonical
  pages) to the Engineer after Architect acceptance.
