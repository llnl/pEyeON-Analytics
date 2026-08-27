---
title: "Velocity Rollup (Cross-Feature Metrics)"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/decision/2026-08-27-adopt-velocity-mini-lab.md
  - ../pEyeON-Analytics/wiki/concept/velocity-metric.md
policy: agent-editable
last_validated: 2026-08-27
repo_scope: cross-repo
implementation_area: dev-environment
format_domain: none
audience: mixed
status: draft
source_paths: wiki/metrics.md
tags: [metrics, velocity, rollup, feature-work, solo-hours]
---

# Velocity Rollup

One row per closed feature, appended at feature close-out. Metric definition:
[[wiki/concept/velocity-metric]]; protocol:
[[wiki/decision/2026-08-27-adopt-velocity-mini-lab]].
`Velocity = solo-hours / (5.714 × days)` — one decimal, uncertainty range
attached. The trend across features, not any single point, is the product.

## Closed Features

| Feature | Lead time (days) | Solo estimate (h) | Feature Velocity (± uncertainty) | Comparability flag | Metrics |
|---|---|---|---|---|---|
| implement-a-report-generator-ability | 1 (same-day; day-grain minimum) | 64 (sealed "8 days", hours interpreted) | **11.2** (5.6–22) — single sealed estimate (AI seal broken); Low confidence | none: Q3 "Yes" — plain counterfactual; counts in the fitted trend | [[wiki/work/implement-a-report-generator-ability/metrics]] |

Features already in flight at adoption (e.g. cleanup-streamlit-app) are not
retrofitted; see the ADR's Consequences.

## Portfolio Velocity

**Pending N ≥ several closed features.** Computed as
`sum(solo-hours of features closed in window) / (5.714 × window-days)` over a
trailing window ≥ 4× the median lead time, and read as a smoothed trend only
— never as a per-window score. Until then, only the per-feature points above
are meaningful.

## Related

- [[wiki/concept/velocity-metric]] — metric definition
- [[wiki/decision/2026-08-27-adopt-velocity-mini-lab]] — adoption ADR and protocol
- [[wiki/concept/metrics-template]] — the per-feature file this page aggregates
