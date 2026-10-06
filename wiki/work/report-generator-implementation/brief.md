---
title: "Feature Brief: Report Generator Implementation"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/work/report-generator-implementation/interview.md
  - ../pEyeON-Analytics/wiki/decision/2026-08-27-report-generation-typst.md
  - ../pEyeON-Analytics/extras/spike_report_typst.py
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/report-generator-implementation/brief.md
tags: [feature-work, reporting, pdf, typst, streamlit, cli]
---

# Feature Brief: Report Generator Implementation

## Problem

The report engine is selected
([[wiki/decision/2026-08-27-report-generation-typst]]) and proven in spike
prototypes, but EyeON still cannot produce a report: the templates live in
`extras/`, there is no rendering module, no CLI, and no app surface.

## Goals

- A `reports` package: shared rendering core (DuckDB SQL → JSON →
  in-memory `typst.compile` → PDF bytes) with the two spike-proven report
  types productionized: **batch change-detection** and **observation
  dossier**.
- A new **Reports page** in the Streamlit app: report picker, parameters,
  generate, browser download.
- An **`eyeon-report` console script** (`[project.scripts]`) rendering the
  same reports headless with `-o/--output`.
- Charts via **plotly static SVG export** (kaleido), replacing the spike's
  matplotlib chart.
- Dependencies added (Architect-approved): `typst`, `plotly`, `kaleido`.

## Non-Goals

- HTML report output (deferred to its own feature).
- CSV/text exports, inventory and security-posture report types
  (follow-ons).
- Artifact persistence/management beyond browser download and `-o` path.
- Changes to DLT load, dbt models, or `../pEyeON`.

## User-Facing Behavior

- App: sidebar navigation gains a Reports page (Analysis section). User
  picks "Batch changes" (optional utility filter, row limit or full) or
  "Observation dossier" (observation picker), clicks Generate, gets a
  download button for the PDF.
- CLI: `uv run eyeon-report batch-changes -o changes.pdf [--utility X]
  [--full]` and `uv run eyeon-report dossier --uuid <uuid> -o dossier.pdf`
  against the configured database (override with `--db`).

## Acceptance Criteria

Frozen at feature open (2026-08-27):

1. `uv run eyeon-report batch-changes -o <path>` and
   `uv run eyeon-report dossier --uuid <uuid> -o <path>` produce correct
   PDFs from `database/eyeon.duckdb` with no network access at render.
2. The Streamlit Reports page generates both report types and delivers
   them via `st.download_button`; the app's existing AppTest smoke pattern
   passes for the new page and the navigation change.
3. Report charts are plotly-rendered SVGs embedded in the PDF.
4. `typst`, `plotly`, `kaleido` are project dependencies; `eyeon-report`
   is a `[project.scripts]` entry; `uv sync` succeeds.
5. `verification.md` records the exact commands and full output, with an
   explicit Deviations-From-Handoff section.

## Affected Areas

New: `reports/` package (queries, charts, render core, `.typ` templates,
CLI). Modified: `pyproject.toml` (deps + script entry), `EyeOnData.py`
(navigation), `pages/` (new Reports page). Reference (unchanged):
`extras/spike_report_{data,typst}.py`.

## References

[[wiki/work/report-generator-implementation/interview]],
[[wiki/decision/2026-08-27-report-generation-typst]],
[[wiki/work/implement-a-report-generator-ability/spike]] (measured
baselines), `extras/spike_report_typst.py` (reference pattern).

## Open Questions

None blocking — module layout details delegated to the Engineer and
recorded in the implementation plan.

## Test Plan

Scripted verification (no app-layer test suite exists): `uv run python -m
py_compile` on changed files; PDF-magic + page-count checks on both CLI
outputs; AppTest smoke for the Reports page; offline check (strace connect
trace or documented by-design argument). Full output into
`verification.md`.

## Done When

Acceptance criteria met, verification recorded, Architect accepts the
verification, and durable facts (reports module, CLI, page) are promoted to
canonical wiki pages at close-out.
