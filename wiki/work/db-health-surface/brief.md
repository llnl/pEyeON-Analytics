---
title: "Feature Brief: DB Health Surface"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/utils/dlt_state.py
  - ../pEyeON-Analytics/utils/utils.py
  - ../pEyeON-Analytics/pages/debug_page.py
policy: agent-editable
last_validated: 2026-08-31
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/db-health-surface/brief.md
tags: [feature-work, streamlit, dlt, state-consistency]
---

# Feature Brief: DB Health Surface

> **Workflow note:** Lightweight variant (see
> [[wiki/concept/llm_assisted_feature_workflow]] §Lightweight Variant).
> Scope settled in the 2026-08-31 session that shipped
> [[wiki/work/dlt-state-consistency/brief]]; this is the deferred UI slice
> the Architect approved with "Proceed", including the mid-session request
> that `_meta` events "can be surfaced in the UIs or anywhere else easily".

## Problem

The consistency layer records self-heal events in `_meta.consistency_log`
and can produce a full doctor report, but both are CLI/SQL-only. A person at
the Streamlit app never learns the database was replaced or healed without
opening a terminal.

## Goals

- Sidebar **DB Health** expander on every page: recent consistency events.
- Sidebar **warning banner** when the database was replaced and no load has
  completed since (i.e. dropped pending packages likely mean batches need
  re-loading).
- Full doctor report on the Debug page, on demand.
- Logic stays in Streamlit-free helpers (`utils/dlt_state.py`,
  `load_eyeon.doctor_text`) so it stays testable and reusable.

## Non-Goals

- New top-level page; charts/history visualization; auto-refresh.
- Handling databases predating `_meta` beyond degrading gracefully.

## User-Facing Behavior

- Every page's sidebar gains a collapsed "DB Health" expander listing the
  last 10 `_meta.consistency_log` events (empty caption when none/no
  `_meta`).
- If the latest `db_instance_changed` event has no completed load after it,
  a sidebar warning explains that pending packages from the previous
  database were dropped and batches may need re-loading.
- Debug page gains a "DLT State Doctor" expander with a button that renders
  the same report as `load_eyeon.py --doctor`.

## Acceptance Criteria

- `recent_events()` and `unresolved_instance_change()` helpers unit-tested
  against real DuckDB fixtures, including the no-`_meta` case.
- `doctor_text()` returns the CLI report string given only a connection;
  the `--doctor` CLI is unchanged in behavior.
- App modules import cleanly with the new wiring.

## Affected Areas

- `utils/dlt_state.py` — `recent_events`, `unresolved_instance_change`.
- `load_eyeon.py` — factor `doctor_text(conn)` out of `doctor()`.
- `utils/utils.py` — `sidebar_db_health()` wired into `sidebar_db_chooser()`.
- `pages/debug_page.py` — doctor expander.
- `tests/test_dlt_state_consistency.py` — helper tests.

## References

- [[wiki/diagnostic/dlt-three-store-consistency]] — the layer being surfaced.

## Open Questions

None — deferred ideas (auto-refresh, richer history) go to follow-ups.

## Test Plan

Unit tests for both helpers (events present / absent / unresolved vs
resolved instance change); `doctor_text` exercised via the existing doctor
regression test; import smoke checks for `utils.utils` and
`pages.debug_page`.

## Done When

Tests pass; verification.md filled; wiki/log.md updated.
