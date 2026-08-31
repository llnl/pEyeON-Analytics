---
title: "Implementation Plan: DLT State Consistency"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/load_eyeon.py
  - ../pEyeON-Analytics/utils/db.py
  - ../pEyeON-Analytics/utils/config.py
policy: agent-editable
last_validated: 2026-08-31
repo_scope: pEyeON-Analytics
implementation_area: dlt-pipeline
format_domain: none
audience: developer
status: draft
source_paths: wiki/work/dlt-state-consistency/implementation_plan.md
tags: [feature-work, dlt, duckdb, schema, state-consistency]
---

# Implementation Plan: DLT State Consistency

## Scope

Heal/detect/explain/record the three-store dlt state drift described in
`brief.md`. One slice; no schema.sql regeneration, no Streamlit UI.

## Steps

1. **New module `utils/dlt_state.py`** — all consistency logic, no Streamlit
   imports so both CLI and app can use it:
   - `_duckdb_type()` / `_q_ident()` moved from `load_eyeon.py`.
   - `ensure_meta_tables(conn)` — creates `_meta` schema,
     `_meta.db_instance(uuid VARCHAR, created_at TIMESTAMP)` (one row,
     inserted if empty) and
     `_meta.consistency_log(ts TIMESTAMP, event VARCHAR, detail JSON)`.
     Returns the instance uuid.
   - `log_event(conn, event, detail: dict)` — appends to
     `_meta.consistency_log`.
   - `ensure_destination_tables(schema, conn, dataset_name)` — the fixed
     guard: takes a dlt `Schema` object (NOT a dataset-name lookup), creates
     missing tables and adds missing columns; caller invokes it for the
     dataset and its `<dataset>_staging` sibling. Logs one
     `tables_healed` event when it changed anything.
   - `reconcile_db_instance(pipeline, conn)` — reads the sidecar marker
     `Path(pipeline.working_dir) / "eyeon_db_instance"`, compares with
     `_meta.db_instance`:
     - marker missing → write it; if pending packages exist, WARN (cannot
       prove orphaned; advise `--doctor`).
     - match → no-op.
     - mismatch → WARN with both uuids; list pending package load_ids via
       `list_extracted_load_packages()` + `list_normalized_load_packages()`;
       `drop_pending_packages(with_partial_loads=True)`; write new marker;
       `log_event("db_instance_changed", {...})` with old/new uuid and
       dropped load_ids.
   - `doctor_report(pipeline, conn, datasets) -> str` — three-store
     comparison: db path + instance ids (db vs sidecar), local schema
     name/version/hash, per-dataset `_dlt_version` max version/hash and
     `_dlt_loads` count, pending packages, per-dataset physical drift
     (schema tables/columns missing in DuckDB), last 10 consistency_log
     events.
2. **`load_eyeon.py`**:
   - Delete inline `_duckdb_type`, `_q_ident`, `_ensure_destination_tables`;
     import from `utils.dlt_state`.
   - `main()`: after pipeline construction — `ensure_meta_tables(conn)`,
     `reconcile_db_instance(pipeline, conn)`; drain legitimate pending
     packages with a bare `pipeline.run()` (uses the pipeline's stored
     dataset_name, avoiding the bronze/silver flip landing packages in the
     wrong dataset) guarded by `pipeline.has_pending_data`; then per phase
     call `ensure_destination_tables` for the dataset and `_staging` sibling
     using `pipeline.default_schema` (skip when `default_schema_name` is
     unset — first run bootstrap).
   - After a successful run, refresh the sidecar marker.
   - `--doctor` CLI flag: `utility_id`/`source` become optional; validate
     post-parse (error if absent in normal mode); doctor mode prints
     `doctor_report()` and exits.
3. **`utils/db.py`**: `init()` calls `ensure_meta_tables(con)` and
   `log_event(con, "db_initialized", {"source": "schemas/schema.sql"})`.
4. **Tests `tests/test_dlt_state_consistency.py`** (isolate with
   `DLT_DATA_DIR`, `EYEON_DUCKDB_PATH`, `EYEON_EYEONDATA_TOML` → tmp):
   - ensure-tables adds a missing column / creates a missing table.
   - incident regression: load sample batch → create a pending package
     (extract-only) → delete DB → recreate stale-shaped certs table +
     empty `_dlt_*` tables (simulated schema.sql bootstrap) → run load →
     succeeds; pending dropped; `consistency_log` has
     `db_instance_changed`; the previously-missing column exists.
   - doctor: returns a report containing the expected sections for both
     healthy and drifted DBs.

## Files Likely To Change

- `utils/dlt_state.py` (new)
- `load_eyeon.py`
- `utils/db.py`
- `tests/test_dlt_state_consistency.py` (new)

## Tests To Add Or Update

See step 4. Run with `uv run pytest tests/test_dlt_state_consistency.py -v`.

## Migration Or Compatibility Notes

- All DDL is add-only and idempotent (`IF NOT EXISTS` / column-diff before
  `ALTER`); healthy DBs are untouched except for the new `_meta` schema.
- Existing DBs gain `_meta` on the next load; the first run after upgrade has
  no sidecar marker and only warns if pending packages exist (never drops on
  first contact).
- `schemas/eyeon_metadata.schema.yaml` remains export-only; untouched.

## Rollback Plan

Revert the branch. The `_meta` schema in any DB it touched is inert (nothing
else reads it) and can be dropped with `DROP SCHEMA _meta CASCADE`.

## Done Checklist

- [ ] `utils/dlt_state.py` implemented
- [ ] `load_eyeon.py` wired (reconcile, drain, ensure, --doctor)
- [ ] `utils/db.py` init() stamps instance + logs event
- [ ] Tests pass via `uv run pytest`
- [ ] `verification.md` filled with exact commands + full output
- [ ] `wiki/log.md` entry appended
