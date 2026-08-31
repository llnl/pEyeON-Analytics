---
title: "Feature Brief: DLT State Consistency"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/load_eyeon.py
  - ../pEyeON-Analytics/utils/db.py
  - ../pEyeON-Analytics/schemas/schema.sql
  - ../pEyeON-Analytics/wiki/pipeline/base_schema_derivation.md
policy: agent-editable
last_validated: 2026-08-31
repo_scope: pEyeON-Analytics
implementation_area: dlt-pipeline
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/dlt-state-consistency/brief.md
tags: [feature-work, dlt, duckdb, schema, state-consistency]
---

# Feature Brief: DLT State Consistency

> **Workflow note:** This feature uses the *lightweight* variant of the
> LLM-assisted feature workflow. The interview stage happened organically in
> the diagnostic session of 2026-08-31 (Architect and Engineer root-causing a
> production `Binder Error` together); no separate `interview.md` was kept and
> the Velocity metrics overlay was skipped by Architect instruction.

## Problem

DLT schema state lives in three places that must move together:

1. The local pipeline working dir `~/.dlt/pipelines/eyeon_metadata/` — live
   schema, pipeline state, and **pending load packages**.
2. The destination's dlt metadata tables (`_dlt_version`, `_dlt_loads`,
   `_dlt_pipeline_state`) inside the DuckDB file.
3. The physical tables in the DuckDB file.

On a dev machine it is routine to delete the DuckDB file (and re-bootstrap it
from the intentionally-stale `schemas/schema.sql` base schema) without
resetting the pipeline working dir. A pending load package that survives the
deletion skips dlt's reflect-and-migrate step on retry, so its merge SQL binds
against columns the re-bootstrapped table lacks, failing deep in the load with
a cryptic `Binder Error` (observed 2026-08-31: `raw_obs__signatures__certs`
missing `rfc822_name`, `load_id=1787872472.1437767`).

This was a *regression of intent*: `_ensure_destination_tables()` in
`load_eyeon.py` was written to self-heal exactly this drift, but it keys
`pipeline.schemas[...]` by dataset name (`"bronze"`/`"silver"`) when the
mapping is keyed by schema name (`"eyeon_metadata"`), so it silently
no-ops on every run. Diagnosing the recurrence cost roughly an hour of
Architect time.

## Goals

- **Heal:** physical-table drift (missing tables/columns vs the dlt schema)
  is repaired automatically before every load, including staging datasets.
- **Detect:** the "database was replaced under a live pipeline" state is
  detected deterministically (instance identity marker), reconciled
  automatically (drop orphaned pending packages), and reported in one
  self-explanatory log line.
- **Explain:** a `--doctor` mode prints the three-store state comparison on
  demand so any future drift is diagnosed by a program, not archaeology.
- **Record:** consistency events are persisted in the database itself
  (`_meta.consistency_log`: timestamp, event, JSON detail) — DB initialized
  from `schema.sql`, instance mismatch detected, pending packages dropped,
  columns healed — so UIs and future sessions can surface the history with a
  plain `SELECT` (Architect request, 2026-08-31).
- **Pin:** a regression test reproduces the incident end-to-end so the fix
  cannot silently die again.

## Non-Goals

- Regenerating `schemas/schema.sql` (needs a loaded corpus; separate task).
- Streamlit UI surface for the doctor report (follow-up; the function should
  be importable so a page can call it later).
- Splitting bronze/silver into two pipeline names (recorded as an option; not
  this slice).
- Any change to `../pEyeON` or to observation semantics.

## User-Facing Behavior

- Normal loads behave as today, plus: missing tables/columns are created
  before each `pipeline.run()`; legitimate pending packages are drained
  *before* the bronze/silver dataset flip using the pipeline's stored
  dataset name.
- If the DB file changed identity since the pipeline last ran, the load logs
  a loud warning naming both instance ids and the dropped pending package
  load_ids, then proceeds cleanly.
- `uv run python load_eyeon.py --doctor` prints the consistency report and
  exits (no `--utility_id`/`--source` required).

## Acceptance Criteria

- `_ensure_destination_tables` successor demonstrably executes (unit test
  asserts it adds a missing column) for the target dataset and its
  `_staging` sibling.
- Deleting the DB, re-bootstrapping from a stale `schema.sql`-shaped DDL, and
  re-running the load succeeds with no manual intervention — including when a
  pending package from the old DB instance exists (it is dropped and logged).
- `--doctor` runs against a healthy and an inconsistent DB without error and
  reports: db path, instance ids, local schema version/hash, per-dataset
  destination version/hash, pending packages, physical column drift, and
  recent `_meta.consistency_log` events.
- Every heal/reconcile action leaves a row in `_meta.consistency_log`
  queryable with plain SQL.
- Existing load path unchanged for a healthy DB (idempotent, add-only DDL).

## Affected Areas

- `load_eyeon.py` — reconcile + ensure + drain + `--doctor` wiring.
- `utils/dlt_state.py` (new) — all consistency logic, importable by Streamlit.
- `utils/db.py` — `init()` stamps the DB instance id.
- `tests/test_dlt_state_consistency.py` (new).

## References

- `wiki/pipeline/base_schema_derivation.md` — base-schema bootstrap design.
- `wiki/work/dlt-state-consistency/implementation_plan.md`
- dlt APIs verified against installed dlt 1.28.1: `pipeline.default_schema`,
  `pipeline.default_schema_name`, `pipeline.has_pending_data`,
  `pipeline.list_extracted_load_packages()`,
  `pipeline.list_normalized_load_packages()`,
  `pipeline.drop_pending_packages(with_partial_loads=True)`,
  `pipeline.working_dir`.

## Open Questions

- Should the doctor report also be shown in the Streamlit sidebar? (Deferred;
  design the function to return text so a page can render it.)
- Should `schema.sql` exports strip `_dlt_*` tables? (Deferred to the next
  base-schema regeneration.)

## Test Plan

- Unit-ish: ensure-tables adds a missing column to an existing table and
  creates a missing table, in both `silver` and `silver_staging`.
- Regression (the incident): load a sample batch → leave a pending package →
  delete the DB file → recreate stale-shaped tables (simulating the
  `schema.sql` bootstrap) → run the load again → assert success, dropped
  package logged, column present.
- Doctor: report generated for both healthy and drifted states.

## Done When

- All tests above pass via `uv run pytest`.
- `verification.md` records the exact commands and full output.
- `wiki/log.md` updated; durable facts promoted to a canonical
  `wiki/diagnostic/` page after the fix stabilizes.
