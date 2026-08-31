---
title: "Diagnostic: DLT Three-Store State Consistency"
type: diagnostic
confidence: high
grounded_by:
  - ../pEyeON-Analytics/utils/dlt_state.py
  - ../pEyeON-Analytics/load_eyeon.py
  - ../pEyeON-Analytics/utils/db.py
  - ../pEyeON-Analytics/tests/test_dlt_state_consistency.py
  - ../pEyeON-Analytics/schemas/schema.sql
policy: agent-editable
last_validated: 2026-08-31
repo_scope: pEyeON-Analytics
implementation_area: dlt-pipeline
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/diagnostic/dlt-three-store-consistency.md
tags: [dlt, duckdb, schema, state-consistency, doctor, troubleshooting]
---

# Diagnostic: DLT Three-Store State Consistency

## The Model

DLT schema state lives in **three stores** that must move together:

| Store | Location | Contents |
| --- | --- | --- |
| Local pipeline dir | `~/.dlt/pipelines/eyeon_metadata/` | Live schema, pipeline state, **pending load packages**, DB instance marker |
| Destination metadata | `_dlt_version`, `_dlt_loads`, `_dlt_pipeline_state` tables in the DuckDB file | What DLT believes it has applied to this database |
| Physical tables | The DuckDB file | What actually exists |

DLT tolerates drift as long as it can *see* it: when the destination has no
record of the current schema version, DLT reflects `information_schema` and
generates the missing `CREATE`/`ALTER` statements. Drift becomes fatal when
DLT wrongly believes a schema version is already applied — then merge SQL
runs against tables missing columns and fails with a DuckDB **Binder Error**.
<!-- GROUND_TRUTH: ../pEyeON-Analytics/utils/dlt_state.py §module docstring -->

The classic dev-machine trigger (incident of 2026-08-31): delete the DuckDB
file, re-bootstrap it from the intentionally-stale `schemas/schema.sql` base
schema, keep the pipeline dir. A pending load package from the old database
skips migration on retry and binder-errors on columns the recreated tables
lack. **The DuckDB file and the pipeline working dir are one unit of state:
reset both or neither.**

Two snapshots in `schemas/` are *not* stores of live state:

- `eyeon_metadata.schema.yaml` — export-only (`export_schema_path` in
  `load_eyeon.py`; nothing reads it). Its staleness is free; devs should
  revert incidental local churn rather than commit it.
- `schema.sql` — the "maximum base schema" bootstrap input read once by
  `utils/db.py init()` (see [[wiki/pipeline/base_schema_derivation]]). Its
  staleness is repaid by the consistency layer at the next load. Since
  2026-08-31 it no longer creates `_dlt_*` bookkeeping tables, so a
  bootstrapped database looks genuinely fresh to DLT.

## The Consistency Layer (`utils/dlt_state.py`)

Runs inside `load_eyeon.main()` on every load:

1. **Instance identity** — every database carries `_meta.db_instance`
   (stamped by `db.init()` or first contact); the pipeline dir mirrors it in
   an `eyeon_db_instance` sidecar file. A mismatch means "database replaced
   under a live pipeline": pending packages are dropped (their source JSON
   batches remain re-loadable), the marker is updated, and the event is
   recorded.
2. **Pending-package drain** — surviving legitimate packages are loaded via
   a bare `pipeline.run()` *before* the bronze/silver dataset flip, so they
   land in the dataset they were extracted for.
3. **Physical drift heal** — before each phase, missing tables/columns are
   created from the DLT schema (add-only, idempotent). Tables are scoped per
   dataset by their root table (`raw_json` → bronze; everything else →
   silver); staging datasets are healed columns-only because DLT
   materializes staging tables itself.
4. **Event trail** — every action appends to `_meta.consistency_log
   (ts, event, detail JSON)`: `db_initialized`, `db_instance_marker_created`,
   `db_instance_changed`, `tables_healed`. Surface it anywhere with
   `select * from _meta.consistency_log order by ts desc`.

## Diagnosing

```bash
uv run python load_eyeon.py --doctor
```

Prints the three-store comparison: instance identity verdict, local schema
version/hash, pending packages, per-dataset destination version/hash and
physical drift, and recent consistency events. Run it whenever a load fails
strangely or after any manual database surgery.

In the Streamlit app (since [[wiki/work/db-health-surface/brief]]): every
page's sidebar has a **DB Health** expander with recent consistency events
and shows a warning when the database was replaced and no load has completed
since (dropped pending packages may mean batches need re-loading); the Debug
page's **DLT State Doctor** expander renders the full report on demand.

## Dev Reset Recipe

To truly start over on a dev machine:

```bash
rm <database>.duckdb                      # store 2 + 3
rm -rf ~/.dlt/pipelines/eyeon_metadata/   # store 1 (incl. pending packages)
```

Deleting only the database is self-healing since 2026-08-31 (the layer
detects the replacement and recovers), but the full reset is still the clean
option; dropped pending packages always mean re-loading those batches from
their source directories.

## History

Root-caused and fixed 2026-08-31 under
[[wiki/work/dlt-state-consistency/brief]] (lightweight LLM-assisted
workflow). The original `_ensure_destination_tables` guard in `load_eyeon.py`
was written for exactly this failure but silently no-oped for its entire
life: it looked up `pipeline.schemas[dataset_name]`, and that mapping is
keyed by schema name (`eyeon_metadata`), not dataset name. Regression test:
`tests/test_dlt_state_consistency.py`.

## Related

- [[wiki/pipeline/dlt_load]] — the load pipeline this protects
- [[wiki/pipeline/base_schema_derivation]] — the base-schema bootstrap design
- [[wiki/work/dlt-state-consistency/brief]] — feature record and verification
