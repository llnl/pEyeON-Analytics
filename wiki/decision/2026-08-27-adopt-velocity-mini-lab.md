---
title: "Decision: Adopt the interview stage and Velocity metrics overlay from the Wintap ecosystem"
type: decision
status: accepted
decided_on: 2026-08-27
grounded_by:
  - ../Wintap-Analytics/wiki/decision/ai-velocity-roi-mini-lab.md
  - ../Wintap-Analytics/wiki/concept/velocity-metric.md
  - ../Wintap-Analytics/wiki/concept/llm-assisted-feature-workflow.md
  - ../Wintap-Analytics/wiki/concept/feature-work-template.md
  - ../Wintap-Analytics/wiki/concept/metrics-template.md
policy: human-review-required
tags: [decision, workflow, metrics, llm, velocity, interview, solo-hours]
---

# Decision: Adopt the Interview Stage and Velocity Metrics Overlay

**Date:** 2026-08-27 · **Status:** Accepted (Architect-directed)

## Context

The Wintap ecosystem's LLM-assisted feature workflow — the sibling of this
repo's [[wiki/concept/llm_assisted_feature_workflow]] — gained two additions
after our version was written:

1. An **interactive interview stage** at feature kickoff
   (`../Wintap-Analytics` commit `1016c01`), so the brief reflects the
   human's actual intent rather than the agent's first guess.
2. A **Velocity metrics overlay** ("AI Velocity and ROI Mini-Lab", iterated
   to v2.1 through a pilot feature and external review), measuring how fast
   features arrive relative to one unassisted developer.

On 2026-08-27 the Architect reviewed both and directed that this repository
adopt **both, in full**. The Wintap protocol record
(`../Wintap-Analytics/wiki/decision/ai-velocity-roi-mini-lab.md`) remains the
origin document, including its revision history, pilot data, honest
limitations, and full alternatives analysis; this ADR records the adoption
and the local adaptations.

## Decision

Adopt both additions into this repo's workflow, keeping the metric's formula,
field names, and question phrasing identical to the Wintap ecosystem's v2.1
protocol so results remain comparable across the two project ecosystems.

### Interview stage

Every feature invocation runs an adaptive Q&A between the Architect and the
Engineer before `brief.md` is drafted: ground in wiki/repo context first, ask
in small batches (2–4 questions), classify resolved items as
decision/constraint/delegated/deferred, play back for confirmation, and
optionally record the session in `interview.md`. A
`(no interview)` invocation variant skips it. Protocol details:
[[wiki/concept/llm_assisted_feature_workflow]] §Interview Stage; skeleton:
[[wiki/concept/feature_work_template]].

### Velocity metrics overlay

Every feature carries an **optional** metrics overlay:

- **Headline metric:** `Velocity = solo-hours / (5.714 × days)`,
  dimensionless, unit = solo-FTE equivalents; Feature Velocity (per-feature
  speedup) and Portfolio Velocity (windowed delivered throughput) are two
  views sharing that unit. Full definition, calendar-time rationale, and
  what-it-is-not: [[wiki/concept/velocity-metric]].
- **Sealed dual estimates at feature open:** the interview ends with exactly
  two sealed questions to the Architect (forced-counterfactual solo hours +
  realistic solo date; predicted date with the AI workflow), recorded
  verbatim in `interview.md` `## Sealed — human estimates`. The Engineer
  writes its own independent estimates of the same quantities to
  `metrics.md` **before** reading that section, plus a one-line basis.
  Independence is the point — an anchored estimate is worthless.
- **Human question budget: three, hard cap, with a ratchet.** The two sealed
  questions at open, plus one at close: *"Would you have attempted this
  feature at all without AI?"* (the comparability flag and scope-inflation
  signal). Any future metrics question must displace an existing one. The
  cap applies to metrics questions only; the interview's normal adaptive
  questioning is unaffected.
- **Guardrails:** acceptance criteria frozen at feature open (amendments
  logged in `criteria_amendments`); availability finality (post-acceptance
  defects are maintenance, never retroactive); comparability flagging with
  the capability-vs-willingness distinction (`capability-exceeded` features
  are excluded from the fitted trend; `willingness-only` features are not).
- **Never-gates rule:** metrics never block, delay, or nag the workflow.
  Skipped questions or missing files are missing data; no agent re-asks. A
  feature with an empty or absent `metrics.md` is a normal feature.
- **Close-out duties (Engineer):** record the availability anchor (the dated
  `verification.md` artifact demonstrating the frozen acceptance criteria,
  plus a one-line why); compute lead time and Feature Velocity to one
  decimal with its uncertainty band (default ±2× until calibration narrows
  it); unseal and tabulate estimates vs. actuals; record the comparability
  answer; write the plain-language Results block; append the feature's row
  to [[wiki/metrics]]. File format: [[wiki/concept/metrics-template]].
- **Attention diagnostic:** demoted, optional, coverage-annotated,
  never-headline (15-minute-gap message-timestamp clustering), computed by
  the main session only when cheap. Cost is a companion field, never folded
  into Velocity.

### Local adaptations (deviations from the Wintap origin)

1. **Roles mapping.** Wintap's "human" is our **Architect**; its estimating
   "Engineer" agent maps to our **Engineer** role; per-unit estimates attach
   to dev-handoff units and their `verification.md` records rather than
   Wintap's instruction/audit documents.
2. **Seal-broken handling is the expected common case.** Our Engineer
   typically runs the interview and the exploration in one session, so the
   AI-side seal will often be broken; per protocol the `ai_est_*` fields are
   then left null. When independent AI estimates are wanted, a fresh session
   (or the Developer session at handoff) that has not read the sealed
   section writes them before reading it.
3. **Standing-rule home.** Wintap keeps the sealed-before-reading rule in
   its agent definitions; we do not modify agent definitions from the
   Engineer role, so the standing rules live in this ADR and the pages it
   governs. An `AGENTS.md` pointer (adding `interview.md` and `metrics.md`
   to the §Feature Work artifact list) was approved and applied by the
   Architect on 2026-08-27.
4. **Availability anchor artifact.** The first Architect-accepted validation
   event is evidenced by a dated `verification.md` entry (this repo's audit
   artifact per `AGENTS.md`).
5. **Question areas** in the interview are re-domained to this ecosystem:
   scanner extraction, observation schema, DLT bronze→silver, dbt gold,
   Streamlit, container/VM builds.

## Options Considered

- **Adopt both additions in full (chosen).** Keeps the two ecosystems'
  workflows and metrics comparable; the interview improves brief quality
  regardless of metrics; the overlay's cost is capped at three questions per
  feature by construction.
- **Adopt the interview stage only.** Lower commitment and no new pages, but
  forfeits comparable velocity data while the sibling ecosystem accumulates
  it, and retrofitting metrics later loses sealed baselines for interim
  features (the Wintap pilot demonstrated retrofit estimates are
  low-confidence by definition).
- **Defer both.** No cost now, but each feature closed without sealed
  estimates is a permanently lost datapoint, and the workflows drift
  further apart.

## Tradeoffs

- The overlay adds four wiki pages and close-out bookkeeping for the
  Engineer; the never-gates rule bounds the downside (worst case: null
  fields).
- The seal-broken adaptation (2 above) means many features here may carry
  only one sealed estimate, widening uncertainty bands relative to Wintap's
  dual-sealed points; accepted rather than forcing a session split.
- Velocity's honest limitations carry over unchanged: unverifiable
  counterfactual numerator, N=1 developer, calendar noise dominating short
  features, not gameable-proof. Only the trend is load-bearing. See the
  origin ADR's Honest Limitations section.
- Citing `../Wintap-Analytics` live paths creates a cross-ecosystem
  dependency for provenance; mitigated by carrying the full metric
  definition locally in [[wiki/concept/velocity-metric]].

## Consequences

- [[wiki/concept/llm_assisted_feature_workflow]] gains the Interview Stage
  section, the sealed-questions step, the `(no interview)` invocation
  variant, and metrics close-out duties.
- [[wiki/concept/feature_work_template]] gains `interview.md` (with the
  sealed section) and `metrics.md` skeletons.
- New pages: [[wiki/concept/velocity-metric]] (metric definition),
  [[wiki/concept/metrics-template]] (file format), [[wiki/metrics]]
  (cross-feature rollup, one row per closed feature).
- Features opened from 2026-08-27 onward run the interview by default and
  may carry the metrics overlay; features already in flight (e.g.
  cleanup-streamlit-app) are not retrofitted — a retrofit estimate would be
  unsealed and low-confidence by definition.
- Field names and question phrasing are frozen to the Wintap v2.1 set;
  changes require revising this ADR and the origin-tracking pages together.

## Supersedes / Superseded By

None. This overlays [[wiki/concept/llm_assisted_feature_workflow]] and the
Architect/Engineer/Developer methodology without changing either's core
flow. Where this record and the origin protocol
(`../Wintap-Analytics/wiki/decision/ai-velocity-roi-mini-lab.md`, v2.1)
disagree on metric mechanics, the origin protocol wins; where they disagree
on role/path mapping, this ADR wins.
