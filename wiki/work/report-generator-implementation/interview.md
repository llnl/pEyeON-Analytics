---
title: "Feature Interview: Report Generator Implementation"
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
source_paths: wiki/work/report-generator-implementation/interview.md
tags: [feature-work, interview, reporting]
---

# Feature Interview: Report Generator Implementation

## Initial Idea

Architect (2026-08-27): follow-on to
[[wiki/work/implement-a-report-generator-ability/brief]] — implement the
report-generation ability using the selected engine
([[wiki/decision/2026-08-27-report-generation-typst]]). "Commit so far,
start the feature. We can do all this on the same branch."

## Context Established Before Questioning

- The ADR's Consequences section is the charter: add `typst` as a
  dependency, graduate the spike's query layer and templates from
  `extras/`, wire rendering into Streamlit and a CLI, settle distribution
  (deferred at the prior feature's open).
- `pyproject.toml` has no `typst`/chart deps and no `[project.scripts]`
  entry points — both are new.
- Spike prototypes (`extras/spike_report_{data,typst}.py`) are the proven
  reference pattern: in-memory `typst.compile`, data as JSON virtual file,
  charts as pre-rendered SVG, hash-chunking for unbreakable strings.
- Environment: repo is sshfs-mounted from macOS; project venv must live on
  local disk (`UV_PROJECT_ENVIRONMENT`), per the spike record.

## Interview Log

### Round 1

**Q:** Which reports ship in this feature?
**A:** The two spike reports (batch change-detection + observation dossier).
**Outcome:** decision — inventory and security-posture reports are
follow-ons on the established pattern.

**Q:** How does report generation surface in the Streamlit app?
**A:** New Reports page.
**Outcome:** decision — one page: report picker + parameters + download.

**Q:** Where do rendered artifacts go?
**A:** App download button + CLI `-o/--output` path.
**Outcome:** decision — nothing persisted server-side; no reports
directory; artifact management out of scope.

**Q:** Dependency sign-off: typst + matplotlib?
**A:** typst + plotly instead.
**Outcome:** decision + constraint — charts via plotly static SVG export
(requires `kaleido`); consistent with the app's existing chart look; the
spike's matplotlib chart is ported to plotly.

### Round 2

**Q:** HTML secondary output now or deferred?
**A:** Defer to a later feature.
**Outcome:** decision — PDF only in this slice.

**Q:** CLI shape?
**A:** Console script.
**Outcome:** decision — `eyeon-report` in `[project.scripts]`.

**Q:** Execution model?
**A:** Separate Developer session.
**Outcome:** decision — this session (Engineer) delivers
brief/plan/handoff; implementation happens in a fresh Developer session
from the approved handoff.

## Decisions

- Two reports: batch change-detection, observation dossier (Typst PDFs).
- New Streamlit Reports page; browser download; no persistence.
- `eyeon-report` console script with `-o/--output`.
- Dependencies approved: `typst`, `plotly` (+ `kaleido` for static SVG).
- HTML deferred; separate Developer session implements.

## Constraints

- Offline render: no `@preview` Typst packages; charts pre-rendered SVG;
  hash chunking pattern for 64-char digests.
- Data via DuckDB SQL (configured db per `utils/config.py`).
- One shared rendering core called by both app and CLI.
- JVM-free, pip-only (per ADR).

## Delegations

- Module layout, template organization, and PDF-size tuning delegated to
  the Engineer, reviewable in the dev handoff.

## Deferred / Open Questions

- HTML report path (own feature).
- CSV/text exports (nearly free from DuckDB; on demand).
- Inventory + security-posture reports (follow-ons).
- Artifact management/distribution beyond download/`-o`.

## Playback Summary

Confirmed by the Architect 2026-08-27 (no corrections): productionize the
two spike-proven reports on Typst via a shared `reports` rendering core,
surfaced through a new Streamlit Reports page (download button) and an
`eyeon-report` console script (`-o` output); typst+plotly+kaleido deps
approved; offline constraints carried from the ADR; HTML deferred;
implementation by a separate Developer session from an Architect-approved
handoff.

## Sealed — human estimates

<!-- SEALED: any agent that will produce its own estimates must not read
this section until feature close-out. See
wiki/decision/2026-08-27-adopt-velocity-mini-lab. Seal-order note: the
Engineer's independent ai_est_* values were written to metrics.md BEFORE
these questions were asked. -->

**Q: If you had to build this exact scope alone, without AI, how many working
hours would it take? And on what date would it realistically have been
available? (Forced counterfactual.)**
**A:** "2 days" — recorded verbatim. No hours figure or calendar date
given; Engineer conversion (2 working days ≈ 16 working hours) to be
applied and flagged at close-out.

**Q: With the AI workflow, on what date do you predict this feature will be
available? (Calendar prediction, open date to availability.)**
**A:** "<1 day" — recorded verbatim. Feature opened 2026-08-27; Engineer
interpretation: availability 2026-08-27/28, flagged at close-out.
