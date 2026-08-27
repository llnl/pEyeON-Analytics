---
title: "Feature Interview: Implement a Report Generator Ability"
type: concept
confidence: high
grounded_by: []
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/implement-a-report-generator-ability/interview.md
tags: [feature-work, interview, reporting]
---

# Feature Interview: Implement a Report Generator Ability

## Initial Idea

As given by the Architect (2026-08-27): "The basic idea is we want to
generate reports from the observation data. Output could be pdf, text, html
or csv. The first thing is to determine what software we should use for
report generation. Constraints: data is in parquet, accessed thru SQL/duckdb.
Python is our current primary language environment, but we would consider a
java report tool (maybe jasper?) as they tend to be more mature. API and GUI
for the report designer would be best."

## Context Established Before Questioning

- The DLT loader writes natively into `eyeon.duckdb`
  (`load_eyeon.py` uses `dlt.destinations.duckdb`); dbt builds `gold.*`
  marts; Streamlit consumes them.
  <!-- GROUND_TRUTH: ../pEyeON-Analytics/load_eyeon.py §run_pipeline -->
- A `parquet_glob` macro exists in `dbt_eyeon_gold/macros/` but no model
  references it today; DuckDB reads parquet natively either way, so
  "SQL through DuckDB" covers both the live database and parquet-based
  deployments.
- Report-shaped content already exists in the app: EyeOnSummary, Inventory,
  SecurityPosture, DataQuality, ChangeDetection pages and
  `gold.mart_batch_changes` (Phase 3 of
  [[wiki/work/cleanup-streamlit-app/design]]).

## Interview Log

### Round 1

**Q:** Deliverable of this feature — tool decision only, decision + one
working report, or full subsystem?
**A:** Tool decision only.
**Outcome:** decision — implementation is a follow-on feature.

**Q:** How are reports triggered/consumed — Streamlit app, headless CLI,
both, or a standalone report server?
**A:** Both app and CLI.
**Outcome:** decision — favors tools with a clean programmatic core callable
from both contexts.

**Q:** Which report kinds matter first (multi-select)?
**A:** All four: inventory summaries, security posture, change detection,
per-item detail dossier.
**Outcome:** decision — formatted multi-section documents are in scope, so a
real report engine is justified over plain query exports.

**Q:** Is a JVM in the deployment acceptable (Jasper/BIRT-class tools)?
**A:** Prefer Python; JVM only if clearly better.
**Outcome:** constraint — Java candidates stay in the evaluation but carry a
burden of proof.

### Round 2

**Q:** Which output format is the forcing function?
**A:** PDF first.
**Outcome:** decision — HTML secondary; CSV/text fall out of DuckDB nearly
free and are not decision drivers.

**Q:** What evidence closes the tool decision?
**A:** Comparison + spike of finalists.
**Outcome:** decision — evaluation matrix plus working spike prototypes of
1–2 finalists rendering a small real report from `eyeon.duckdb` data,
closing in an ADR. Becomes the frozen acceptance criteria.

**Q:** Licensing/environment constraints on candidates?
**A:** Must work offline and be open-source only.
**Outcome:** constraint — no SaaS designers, no license phone-home, no
commercial-only tools; both design and render must be fully offline.

### Round 3

**Q:** Who is the GUI report designer for, and how hard is that requirement?
**A:** Developers only, nice-to-have.
**Outcome:** decision — code-first templating tools remain fully in play; a
GUI designer earns bonus points but is not gating.

## Decisions

- Scope: tool decision only (research → comparison → finalist spikes → ADR).
- Acceptance: comparison matrix + 1–2 finalist spikes on real data + ADR.
- Evaluation anchored on all four report kinds; PDF is the must-do-well
  output; both app and CLI render contexts.
- GUI designer is developer-operated and nice-to-have, not gating.

## Constraints

- Data interface is SQL through DuckDB (live `eyeon.duckdb` today; parquet
  readable natively — covers parquet-based deployments).
- Python primary; a JVM tool must clearly win on maturity/designer to
  justify deployment weight. Jasper explicitly on the candidate list.
- Open-source only; fully offline design and render.

## Delegations

- Candidate-list assembly, evaluation-criteria weighting, and finalist
  selection for the spikes are delegated to the Engineer, to be reviewed by
  the Architect in `design.md` before spiking.

## Deferred / Open Questions

- Artifact distribution (where rendered reports land: directory, app
  download, email) — deferred to the follow-on implementation feature.
- Whether report templates/definitions live in this repo or a new location —
  deferred to the follow-on feature; noted for the evaluation only insofar
  as template format portability matters.

## Playback Summary

Confirmed by the Architect 2026-08-27 (no corrections offered): this feature
selects the report-generation software for EyeON observation data and ends
with an accepted ADR. Evidence: a structured comparison of open-source,
offline-capable candidates (Python-first, Java admitted with burden of
proof), plus spike prototypes of 1–2 finalists rendering a small real report
(PDF) from `eyeon.duckdb` via SQL, callable in a way that suits both
Streamlit and CLI contexts.

## Sealed — human estimates

<!-- SEALED: any agent that will produce its own estimates must not read
this section until feature close-out. See
wiki/decision/2026-08-27-adopt-velocity-mini-lab. NOTE: this session's
Engineer ran the interview, so the AI-side seal is broken for this feature;
metrics.md ai_est_* fields are null per the ADR's adaptation 2. -->

**Q: If you had to build this exact scope alone, without AI, how many working
hours would it take? And on what date would it realistically have been
available? (Forced counterfactual — answer even if you would not have
attempted it solo. The hours are the feature's solo-hours: the Velocity
numerator and portfolio weight. The calendar date absorbs weekends and
distractions.)**
**A:** "Human would take 8 days" — recorded verbatim. No hours figure or
calendar date was given; the Engineer's conversion (8 working days ≈ 64
working hours at 8 h/day) is an interpretation to be applied at close-out
and flagged as such in metrics.md.

**Q: With the AI workflow, on what date do you predict this feature will be
available? (Calendar prediction, open date to availability.)**
**A:** "AI should take 2 days" — recorded verbatim. No calendar date was
given; feature opened 2026-08-27, so the Engineer's interpretation is
availability 2026-08-29, to be flagged as an interpretation at close-out.
