---
title: "Feature Metrics: Report Generator Implementation"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/work/report-generator-implementation/interview.md
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: llm-agent
status: draft
source_paths: wiki/work/report-generator-implementation/metrics.md
tags: [feature-work, metrics, velocity, reporting]
---

# Feature Metrics: Report Generator Implementation

Velocity results governed by
[[wiki/decision/2026-08-27-adopt-velocity-mini-lab]];
metric definition in [[wiki/concept/velocity-metric]].
SEAL NOTE: `interview.md` `## Sealed — human estimates` must not be read by
the estimating agent until close-out. Seal-order note: the `ai_est_*` values
below were written by the interviewing Engineer BEFORE the human sealed
questions were asked or answered — the AI estimate is independent this time
(unlike the prior feature), though both estimates live with the same session
afterward; the human must not read this file's estimates before answering.

## Results

<Filled at close-out.>

- **Estimated delivery speed:** —
- **Plausible range:** —
- **Estimate confidence:** —
- **Why confidence is —:** —
- **Delivered in:** — (2026-08-27 → —)
- **Estimated solo effort:** —

## Technical Record

```yaml
feature_slug: report-generator-implementation
feature_abbrev: rgi
status: open
opened: 2026-08-27
closed: null

# --- Feature Velocity (computed at close-out) ---
ts_open: 2026-08-27
ts_available: null
availability_anchor: ""
criteria_amendments: []
lead_time_days: null
solo_hours: null
feature_velocity: null
velocity_uncertainty: ""
comparability: null

# --- AI pre-exploration sealed estimates (written BEFORE the human sealed
# --- questions were asked; independent) ---
ai_est_solo_hours: 28
ai_est_solo_available_date: 2026-09-04
ai_est_ai_available_date: 2026-08-28
ai_est_basis: "two proven spike templates to productionize + one Streamlit page + console-script CLI + tests; pattern already established"

# --- Per-unit development estimates (Engineer, at handoff drafting,
# --- BEFORE implementation; actual_hours filled at close-out from
# --- verification.md where derivable) ---
units:
  - id: rgi-01   # reports package core (deps, queries, charts, templates, render)
    est_hours: 2.0
    basis: "port of proven spike code; parameterization + plotly swap are the new work"
    actual_hours: null
  - id: rgi-02   # eyeon-report console script
    est_hours: 0.5
    basis: "thin argparse over render(); one pyproject entry"
    actual_hours: null
  - id: rgi-03   # Streamlit Reports page + navigation
    est_hours: 1.0
    basis: "one page on the established post-cleanup pattern + nav registration + AppTest"
    actual_hours: null
  - id: rgi-04   # verification + wiki updates
    est_hours: 0.5
    basis: "scripted checks already specified in the plan"
    actual_hours: null

# --- Human sealed estimates (copied from interview.md at close-out ONLY) ---
human_est_solo_hours: null
human_est_solo_available_date: null
human_est_ai_available_date: null

# --- Diagnostics (optional; never part of Results; filled at close-out) ---
attention_hours: null
attention_coverage: ""
actual_api_cost_usd: null
closeout_attempted_without_ai: null

findings: ""
```

## Close-Out Tabulation

<Filled at close-out.>
