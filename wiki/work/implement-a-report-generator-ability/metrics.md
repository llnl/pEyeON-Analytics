---
title: "Feature Metrics: Implement a Report Generator Ability"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/work/implement-a-report-generator-ability/interview.md
  - ../pEyeON-Analytics/wiki/decision/2026-08-27-report-generation-typst.md
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: llm-agent
status: reviewed
source_paths: wiki/work/implement-a-report-generator-ability/metrics.md
tags: [feature-work, metrics, velocity, reporting]
---

# Feature Metrics: Implement a Report Generator Ability

Velocity results governed by
[[wiki/decision/2026-08-27-adopt-velocity-mini-lab]];
metric definition in [[wiki/concept/velocity-metric]].
This was the first feature run under the overlay. The AI-side seal was
broken (the interviewing and estimating sessions were the same), so
`ai_est_*` fields are null per the ADR's local adaptation 2.

## Results

- **Estimated delivery speed:** **11.2× one solo developer's pace**
- **Plausible range:** **about 5.6×–22× faster**
- **Estimate confidence:** **Low**
- **Why confidence is Low:** There is no independent estimate pair (the
  AI-side seal was broken), the human answer required a unit
  interpretation, and a same-day close makes the day-grain denominator
  maximally calendar-noisy.
- **Delivered in:** **1 calendar day** (2026-08-27 → 2026-08-27, same-day;
  day-grain minimum applied)
- **Estimated solo effort:** **64 hours** (sealed answer "8 days",
  Engineer-converted at 8 h/working day — flagged interpretation)

The feature closed on the Architect's acceptance of
[[wiki/decision/2026-08-27-report-generation-typst]], which satisfied the
last of the three acceptance criteria frozen at open (matrix ✓ 2026-08-27,
finalist spikes ✓ 2026-08-27, accepted ADR ✓ 2026-08-27). The close-out
comparability answer was a plain "Yes" (would have attempted solo), so the
feature is fully comparable and counts in the fitted trend.

## Technical Record

```yaml
feature_slug: implement-a-report-generator-ability
feature_abbrev: rpt
status: closed
opened: 2026-08-27
closed: 2026-08-27

# --- Feature Velocity (computed at close-out) ---
ts_open: 2026-08-27
ts_available: 2026-08-27
availability_anchor: "wiki/decision/2026-08-27-report-generation-typst.md — Architect acceptance of the ADR completed the third frozen criterion; spike.md (same date) evidences the first two"
criteria_amendments: []
lead_time_days: 1              # raw same-day close; day-grain minimum of 1 applied (see note)
solo_hours: 64                 # sealed "8 days" x 8h/day — Engineer interpretation, flagged
feature_velocity: 11.2         # 64 / (5.714 x 1)
velocity_uncertainty: "5.6-22" # single sealed estimate; default ±2x band
comparability: none            # close-out answer: "Yes" — plain counterfactual

# --- AI pre-exploration sealed estimates (null: seal broken — this session
# --- ran the interview; see ADR adaptation 2) ---
ai_est_solo_hours: null
ai_est_solo_available_date: null
ai_est_ai_available_date: null
ai_est_basis: "seal broken: interviewing session and estimating session are the same"

# --- Per-unit development estimates ---
# Process lesson: rpt-01/rpt-02 estimates were NOT recorded before
# implementation (protocol deviation — first feature under the overlay);
# est_hours left null rather than backfilled. rpt-03 estimated before work.
units:
  - id: rpt-01   # WeasyPrint spike prototype
    est_hours: null
    basis: "estimate-before-implementation step missed; deviation logged"
    actual_hours: null
  - id: rpt-02   # Typst spike prototype
    est_hours: null
    basis: "estimate-before-implementation step missed; deviation logged"
    actual_hours: null
  - id: rpt-03   # ADR + close-out after Architect decision
    est_hours: 0.5
    basis: "single ADR from an already-written matrix + metrics close-out"
    actual_hours: null   # not derivable from a verification record; single-session Engineer work

# --- Human sealed estimates (copied from interview.md at close-out) ---
human_est_solo_hours: 64             # verbatim "8 days"; hours are Engineer interpretation
human_est_solo_available_date: null  # no date given
human_est_ai_available_date: 2026-08-29  # verbatim "2 days" from open; date is Engineer interpretation

# --- Diagnostics (optional; never part of Results; filled at close-out) ---
attention_hours: null
attention_coverage: ""
actual_api_cost_usd: null
closeout_attempted_without_ai: "Yes"

findings: "Same-day close vs predicted 2 days; single-estimate band; both finalists passed spikes, Architect chose Typst over Engineer's WeasyPrint lean"
```

## Close-Out Tabulation

| Quantity | Human (sealed) | AI (sealed) | Actual |
|---|---|---|---|
| Solo effort | "8 days" (≈64 h) | — (seal broken) | unverifiable counterfactual |
| Solo availability date | not given | — | — |
| AI-workflow availability | "2 days" → ~2026-08-29 | — | **2026-08-27 (same day)** |

Calculation, in plain language: with 64 solo-hours delivered in one
day-grain calendar day, the feature arrived at an estimated **11.2× one solo
developer's pace, plausible range about 5.6×–22× faster**. Limitations,
stated directly: the numerator rests on a single sealed estimate that
required an hours conversion; the true lead time was under one day, so the
day-grain convention makes 11.2 the *conservative* reading (a finer-grained
denominator would inflate it); and this is one point — the trend across
features, not this number, is the product. The same-day close beat the
human's sealed two-day prediction. Row appended to [[wiki/metrics]].
