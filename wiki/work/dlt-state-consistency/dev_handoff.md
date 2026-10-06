---
title: "Dev Handoff: DLT State Consistency"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/wiki/work/dlt-state-consistency/brief.md
  - ../pEyeON-Analytics/wiki/work/dlt-state-consistency/implementation_plan.md
policy: agent-editable
last_validated: 2026-08-31
repo_scope: pEyeON-Analytics
implementation_area: dlt-pipeline
format_domain: none
audience: developer
status: draft
source_paths: wiki/work/dlt-state-consistency/dev_handoff.md
tags: [feature-work, dlt, duckdb, schema, state-consistency]
---

# Dev Handoff: DLT State Consistency

**Status:** Approved
**Architect Approval:** Approved 2026-08-31 (in-session — Architect: "I like
this whole approach! … Proceed", lightweight workflow, same session as the
diagnostic interview; `_meta` event-log addition requested mid-session)

## Copy/Paste Prompt

Use this prompt to hand the work to a Developer session:

    As the Developer, implement the feature: dlt-state-consistency.

    Use these wiki files as the handoff context:

    - wiki/work/dlt-state-consistency/brief.md
    - wiki/work/dlt-state-consistency/implementation_plan.md

    Goal: heal, detect, explain, and record dlt three-store state drift so
    a deleted/re-bootstrapped dev database never again fails a load with a
    Binder Error.

    Before editing, read AGENTS.md and confirm this handoff is Approved.

## Handoff Summary

Root cause and design are settled (see `brief.md` Problem/Goals). Four
deliverables: fixed self-heal guard in a new `utils/dlt_state.py`, DB
instance-identity reconcile with pending-package drop, `--doctor` report,
`_meta.consistency_log` event trail, plus a regression test reproducing the
2026-08-31 incident. No architectural decisions remain.

## Primary Sources For The Dev Agent

- `load_eyeon.py` — current (dead) `_ensure_destination_tables`, pipeline
  construction, bronze/silver phases.
- `utils/db.py` — `init()` bootstrap from `schemas/schema.sql`.
- `utils/config.py` — `duckdb_path()`, `EYEON_EYEONDATA_TOML` /
  `EYEON_DUCKDB_PATH` env overrides (use them for test isolation, plus
  `DLT_DATA_DIR`).
- Installed dlt 1.28.1 APIs verified: `default_schema`,
  `default_schema_name`, `has_pending_data`, `working_dir`,
  `list_extracted_load_packages`, `list_normalized_load_packages`,
  `drop_pending_packages(with_partial_loads=True)`.

## Recommended First Implementation Slice

All of `implementation_plan.md` — it is one coherent slice.

## Non-Goals For This Slice

Streamlit UI, `schema.sql` regeneration, pipeline split, `../pEyeON` changes.

## Testing Expectations

`uv run pytest tests/test_dlt_state_consistency.py -v` green; full output in
`verification.md`. Existing tests must not regress.

## Closeout Instructions

- Update wiki/work/dlt-state-consistency/verification.md with commands run and results.
- Update the wiki/work/dlt-state-consistency/implementation_plan.md done checklist.
- Append a concise entry to wiki/log.md.
- Promote durable facts into canonical wiki pages once behavior stabilizes.
