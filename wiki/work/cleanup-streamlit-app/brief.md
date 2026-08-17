---
title: "Feature Brief: Cleanup Streamlit App"
type: concept
confidence: medium
grounded_by:
  - ../pEyeON-Analytics/EyeOnData.py
  - ../pEyeON-Analytics/utils/utils.py
  - ../pEyeON-Analytics/pages/pages.py
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/cleanup-streamlit-app/brief.md
tags: [feature-work, streamlit, ui, cleanup]
---

# Feature Brief: Cleanup Streamlit App

## Problem

`EyeOnData.py` and its `pages/` grew organically and carry structural debt:
duplicated per-page boilerplate, a vestigial OO page layer, a grab-bag
`utils/utils.py` mixing UI and pipeline concerns, dead code, fragile
cross-page session-state coupling, and inconsistent naming/chart styles.
Separately, the untracked `common/` and `extras/` directories contain
partially-adapted legacy code and prototypes from a previous migration whose
features (auth, dataset chooser, graph visualization, Box browsing, cert-chain
analysis) were never carried into the current app. See
[[wiki/work/cleanup-streamlit-app/current_state]] for the full analysis.

## Goals

- Overhaul the app structure: eliminate boilerplate, dead code, and the
  vestigial page-class layer; give shared utilities a coherent home.
- Decide the fate of each `common/` and `extras/` candidate feature
  (port, defer as its own feature, or discard).
- UIX changes directed by the Architect (to be settled in design sessions).
- Leave the app functionally equivalent or better after each slice.

## Non-Goals

- No changes to DLT load, dbt models, or scanner behavior (the app calls
  them; their internals are out of scope).
- No changes to `../pEyeON`.
- Porting every `common/`/`extras/` candidate is not required — decisions
  first, implementation per-candidate.

## User-Facing Behavior

To be directed by the Architect. Current page inventory and behavior are
recorded in [[wiki/work/cleanup-streamlit-app/current_state]].

## Acceptance Criteria

Draft — to be settled with the Architect during design:

- All pages render and core flows work (init DB, load batches, browse,
  hierarchy, certs, schema blame, debug).
- No dead code paths in the app layer (`run_eyeon`, unused helpers).
- Single source of page registration/boilerplate.
- Each `common/`/`extras/` file has an explicit disposition recorded.

## Affected Areas

`EyeOnData.py`, `pages/*`, `utils/utils.py` (UI portions), `common/`
(disposition), `extras/` (disposition), `TODO.md` items that overlap.

## References

See [[wiki/work/cleanup-streamlit-app/references]] and
[[wiki/work/cleanup-streamlit-app/current_state]].

## Open Questions

- Which UIX direction does the Architect want (navigation model, page set,
  visual identity)?
- Adopt modern `st.navigation`/`st.Page` API to replace the hand-rolled
  registry and sidebar?
- Is authentication (OneID via `st.login`, present in legacy `common/`)
  required for the overhauled app?
- Which candidate features from `common/`/`extras/` are in scope vs. spun
  off as separate features (Box UI, cert-chain graphs, dataset chooser)?

## Test Plan

To be defined in the implementation plan once design settles. At minimum:
app smoke launch (`uv run streamlit run EyeOnData.py`), per-page render
checks, and load-flow verification against a small batch.

## Done When

Design settled with the Architect, implementation slices verified, `common/`
and `extras/` dispositions executed, and durable facts promoted to
[[wiki/component/streamlit_app]].
