---
title: "Dev Handoff: DB Health Surface"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/work/db-health-surface/brief.md
  - ../pEyeON-Analytics/wiki/work/db-health-surface/implementation_plan.md
policy: agent-editable
last_validated: 2026-08-31
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: developer
status: draft
source_paths: wiki/work/db-health-surface/dev_handoff.md
tags: [feature-work, streamlit, dlt, state-consistency]
---

# Dev Handoff: DB Health Surface

**Status:** Approved
**Architect Approval:** Approved 2026-08-31 (in-session "Proceed" on the
recommended next implementations; lightweight variant)

## Copy/Paste Prompt

    As the Developer, implement the feature: db-health-surface.

    Use these wiki files as the handoff context:

    - wiki/work/db-health-surface/brief.md
    - wiki/work/db-health-surface/implementation_plan.md

    Goal: surface _meta.consistency_log and the DLT state doctor in the
    Streamlit app, with all logic in Streamlit-free helpers.

    Before editing, read AGENTS.md and confirm this handoff is Approved.

## Handoff Summary

UI slice deferred from dlt-state-consistency. Five small steps in the
implementation plan; no architectural decisions remain.

## Primary Sources For The Dev Agent

`utils/dlt_state.py` (helpers live here), `utils/utils.py`
(`sidebar_db_chooser`/`sidebar_config`), `pages/debug_page.py` (expander
pattern), `load_eyeon.py` (`doctor()`, `_build_pipeline`,
`_doctor_dataset_roots`).

## Recommended First Implementation Slice

All of the implementation plan — one coherent slice.

## Non-Goals For This Slice

New pages, charts, auto-refresh, pre-`_meta` backfills.

## Testing Expectations

`uv run pytest tests/ -v` green (or the documented parallel-env equivalent);
full output in verification.md.

## Closeout Instructions

- Update wiki/work/db-health-surface/verification.md with commands run and results.
- Update the wiki/work/db-health-surface/implementation_plan.md done checklist.
- Append a concise entry to wiki/log.md.
- Promote durable facts into canonical wiki pages once behavior stabilizes.
