---
title: "Feature Brief: Implement a Report Generator Ability"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/work/implement-a-report-generator-ability/interview.md
  - ../pEyeON-Analytics/load_eyeon.py
  - ../pEyeON-Analytics/dbt_eyeon_gold/dbt_project.yml
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: mixed
status: stable
source_paths: wiki/work/implement-a-report-generator-ability/brief.md
tags: [feature-work, reporting, pdf, tool-selection, duckdb]
---

# Feature Brief: Implement a Report Generator Ability

## Problem

EyeON observation data (DLT-loaded silver tables, dbt gold marts in
`eyeon.duckdb`) can only be explored interactively through the Streamlit app.
There is no way to produce a distributable report — a polished PDF/HTML
document or a plain CSV/text export — of inventory, security posture, batch
changes, or a single observation's detail. Before any reports can be built,
the project must choose the report-generation software; that choice is this
feature's entire scope.

## Goals

- Select the report-generation tool for EyeON, recorded in an accepted ADR.
- Evaluate open-source, fully offline candidates: Python-native first,
  Java-based (Jasper family, BIRT) admitted with a burden of proof.
- Ground the decision with working spike prototypes of 1–2 finalists
  rendering a small real report from `eyeon.duckdb` via SQL.
- Evaluation anchored on the four report kinds named by the Architect:
  inventory summaries, security posture, change detection, per-item detail
  dossier.

## Non-Goals

- Building production reports or integrating a report runner into the
  Streamlit app or CLI (follow-on feature).
- Report artifact distribution (directories, downloads, email) — deferred.
- Changes to the DLT load, dbt models, or scanner (`../pEyeON`).

## User-Facing Behavior

None in this feature. The deliverables are wiki artifacts (comparison,
spikes, ADR). The *selected tool* must eventually support: rendering
triggered from both the Streamlit app and a headless CLI; PDF as the
first-class output with HTML secondary and CSV/text trivially available;
developer-operated report design (GUI designer nice-to-have, not gating).

## Acceptance Criteria

Frozen at feature open (2026-08-27), per
[[wiki/decision/2026-08-27-adopt-velocity-mini-lab]] guardrail 1:

1. A written comparison matrix in `design.md` covering the candidate field
   (including Jasper-family and at least the leading Python-native options)
   against the constraint set: open-source license, fully offline design and
   render, DuckDB/SQL data access, PDF quality, HTML/CSV/text support,
   Python API ergonomics (or bridge cost for JVM tools), app+CLI
   embeddability, designer experience, maturity/maintenance health.
2. Working spike prototypes of one or two finalists, each rendering a small
   real report (PDF) from `eyeon.duckdb` data via SQL, with the spike code
   in `extras/` and findings in `spike.md`.
3. An accepted ADR in `wiki/decision/` naming the selected tool, with
   Options Considered and Tradeoffs populated from the matrix and spikes.

## Affected Areas

Wiki only for this feature (`wiki/work/implement-a-report-generator-ability/`,
new ADR in `wiki/decision/`), plus spike prototype code under `extras/`. The
follow-on implementation feature would touch the Streamlit app, a CLI entry
point, and dependency manifests.

## References

See [[wiki/work/implement-a-report-generator-ability/interview]] for the
resolved context. `references.md` will accumulate candidate documentation as
the evaluation proceeds.

## Open Questions

- Where report templates/definitions will live (follow-on concern; only
  template-format portability matters for the evaluation).
- Artifact distribution (deferred to follow-on feature).

## Test Plan

Spike prototypes must run offline against the local `eyeon.duckdb` with
`uv run` (Python) or a documented local JVM invocation (Java finalists), and
produce a PDF a reviewer can open. No production test suite in this feature.

## Done When

The comparison matrix and finalist spikes exist, and the Architect accepts
the ADR naming the selected report-generation tool.
