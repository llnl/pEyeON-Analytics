---
title: "Pipeline: Base Schema Derivation"
type: pipeline
confidence: medium
grounded_by:
  - ../pEyeON-Analytics/schemas/schema.sql
  - ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml
  - ../pEyeON-Analytics/load_eyeon.py
  - ../pEyeON-Analytics/utils/schema_blame.py
  - ../pEyeON-Analytics/extras/Schema_Blame.md
  - ../pEyeON-Analytics/extras/MinamalRows.ipynb
  - /Users/johnson30/.duckdb_history
  - /Users/johnson30/.local/share/opencode/storage/session_diff/ses_273b9b28effeEn9H3N1zxXxv3u.json
policy: agent-editable
last_validated: 2026-06-30
repo_scope: pEyeON-Analytics
implementation_area: schema
format_domain: none
audience: mixed
status: draft
source_paths: wiki/pipeline/base_schema_derivation.md
tags: [dlt, duckdb, schema, schema-blame, corpus-selection]
---

# Pipeline: Base Schema Derivation

## Purpose

The analytics layer treats the DLT-discovered DuckDB schema as a derived
artifact of representative EyeON sample data. The intended base-schema workflow
is to scan a broad corpus with EyeON, load the resulting JSON observations
through `load_eyeon.py`, allow DLT to evolve the bronze and silver tables, then
export the resulting DuckDB schema as `schemas/schema.sql` for bootstrap and
repeatable local databases.

This is not just a convenience artifact. The recovered design notes frame the
base schema as a data-processing principle: build the fullest practical schema
from carefully selected sample observations, then use `schema_blame` to explain
which loads and rows caused schema growth.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/schema.sql lines 1-62 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/load_eyeon.py lines 87-319 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/utils/schema_blame.py lines 1-36 -->

## Current Artifacts

`schemas/schema.sql` is the exported baseline DDL. It creates bronze, silver,
silver staging, gold staging, and gold objects, including `silver.raw_obs`,
format-specific `silver.metadata_*` tables, nested DLT child tables, and the
`silver.schema_blame` and `silver.schema_blame_samples` tables.

`schemas/eyeon_metadata.schema.yaml` is the generated DLT schema file for the
`eyeon_metadata` pipeline. It shows a DLT schema version of `24`, keeps previous
hashes, and records `x-normalizer.seen-data: true` for many tables, which is
evidence that the schema reflects previously loaded observations rather than a
purely hand-authored model.

`load_eyeon.py` is the normal producer. It loads raw JSON into `bronze.raw_json`,
loads observation and metadata resources into `silver`, then calls
`schema_blame.materialize_schema_blame(conn)` after the load completes.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/schema.sql lines 6-40 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 1-5 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 1408-1418 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/load_eyeon.py lines 304-319 -->

## Recovered Methodology

The recovered TODO fragment describes a two-part workflow.

First, derive a base or maximum schema:

1. Load all data that is available, desired, or interesting.
2. Let DLT evolve the DuckDB database schema from those observations.
3. Use DuckDB `EXPORT DATABASE` to export the database; this creates a
   `schema.sql` file defining all tables.
4. Create a new file-based database and run `.read 'schema.sql'`.
5. Load data normally into the bootstrapped database.

Second, find a small carrier set for the schema:

1. Start with leaf or format-specific tables such as `metadata_pe_file`.
2. Use `summarize <table>` to identify sparse columns where
   `null_percentage > 0 and null_percentage <> 100`.
3. Find rows that carry values in the sparsest columns.
4. Encode row feature coverage as boolean signatures or bit masks.
5. Prefer rows that cover the most rare/non-null features, producing a small
   sample subset that recreates the most representative schema.

This is an optimization problem: approximate the fullest schema coverage with
the fewest binary source files. The local history shows manual PE `FileInfo`
experiments that grouped rows on non-null field patterns and selected example
UUIDs. The surviving `extras/MinamalRows.ipynb` notebook contains the more
general prototype: it reads DuckDB `summarize` output, selects nullable columns
by `null_percentage`, generates a per-row bit mask for those columns, chooses an
`example_uuid` for each distinct mask, joins those UUIDs back to
`silver.raw_obs.source_path` and `source_file`, and copies the selected JSON
files into `min_files_max_schema`.

<!-- GROUND_TRUTH: /Users/johnson30/.local/share/opencode/storage/session_diff/ses_273b9b28effeEn9H3N1zxXxv3u.json lines 192-193 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 1612-1803 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/extras/MinamalRows.ipynb lines 36-145 -->

## schema_blame Role

`schema_blame.py` tracks DLT schema evolution by diffing adjacent
`silver._dlt_version` JSON snapshots, filtering DLT internal tables and columns,
enriching changes with the responsible load using a DuckDB `ASOF JOIN`, and
sampling rows from the load that introduced each column-level change.

The materialized outputs are:

| Table | Role |
| --- | --- |
| `schema_blame` | One row per schema change event, with version range, timestamp, responsible load, change type, table, column, and JSON detail. |
| `schema_blame_samples` | Up to five sample rows for column-level changes, filtered to non-null values for `new_column`. |

The Streamlit `Schema_Blame.py` page exposes this as a changelog, change-type
filter, table heatmap, and column drill-down over sample rows.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/utils/schema_blame.py lines 172-245 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/utils/schema_blame.py lines 253-340 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/utils/schema_blame.py lines 348-447 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/pages/Schema_Blame.py lines 81-305 -->

## Sample-Source Clues

The original corpus was not found in the repository. These are best-effort clues
from DuckDB history and local session artifacts, not confirmed surviving sample
sets.

High-confidence clues:

| Clue | Evidence | Notes |
| --- | --- | --- |
| `~/git/LLNL/pEyeON/testfiles/*.json` | Early `read_json` experiments against pEyeON test files. | Likely initial scanner-output exploration. |
| `../results/*.json` and `partitioned/**/*.json` | Repeated `read_json(..., union_by_name=true)` and partition-by-filetype commands. | Likely local output from an early EyeON run, then partitioned by `filetype`. |
| `test_sample_*.json` | Read as JSON sample files after partitioning experiments. | Likely small ad hoc regression/sample files. |
| DLT schemas named `file_metadata_*`, `eyeon_*`, and `raw_*` | DuckDB `use`, `describe`, and `show tables` commands. | Indicates multiple schema-generation/load experiments before the current medallion names settled. |
| `~/data/eyeon/schneider-firmware-20260122-eyeon/*.json` | Raw JSON loaded and searched for firmware-related strings. | Strong candidate for one source batch, especially for firmware/industrial samples. |
| `/Users/johnson30/.dlt/pipelines/eyeon_metadata/schemas/eyeon_metadata.schema.json` | DLT schema file inspected directly from the local pipeline state. | Confirms DLT-generated schema introspection was part of the workflow. |

Medium-confidence clues:

| Clue | Evidence | Notes |
| --- | --- | --- |
| `spack-files-*.json.gz` | Queried for repeated library names such as `libmkl_avx.so`, size variation, paths, and MIME types. | May have been used to choose diverse source binaries rather than as direct EyeON JSON. |
| `~/data/fsi/johnson30.data.parquet` | Queried for file names, sizes, MIME types, and min group sizes. | Looks like a file-inventory source useful for selecting candidate binaries, but not proven to be an EyeON sample corpus. |

<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 2-7 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 191-320 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 511-519 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 1081-1116 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 1158-1168 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 1339-1350 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 1522-1528 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 1886-1923 -->
<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 1954-1987 -->

## Optimization Sketch

The recovered notebook implements a first-pass mask-based representative-row
selection, not a complete weighted set-cover solver. The implied next step is a
greedy set-cover style selection process:

1. Define schema features as columns or nested table/column pairs that have at
   least one observed non-null value.
2. For each observation UUID or source file, compute the set of schema features
   it carries.
3. Score rare features higher than common features, especially sparse columns
   found with `summarize`.
4. Iteratively select the source file that adds the most uncovered high-value
   features.
5. Stop when all target features are covered, or when adding more files only
   contributes low-value/common fields.

The local DuckDB history shows manual precursor queries: sparse-column
selection, boolean feature signatures, bit masks, feature counts, row counts,
and `first(uuid) as example_uuid` to identify carrier observations. The notebook
then generalizes that approach across tables with a `uuid` column and copies the
corresponding source JSON files.

<!-- GROUND_TRUTH: /Users/johnson30/.duckdb_history lines 1612-1803 -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/extras/MinamalRows.ipynb lines 56-145 -->

## Reproduction Guidance

For a new corpus, use the same principle rather than trying to recover the lost
files:

1. Build a broad corpus with deliberate diversity across PE, ELF, Mach-O,
   Java/JAR, OLE, UImage/firmware, archives, signed files, .NET assemblies, and
   malformed or low-information files.
2. Run EyeON over the full corpus and load each batch through `load_eyeon.py` or
   the Streamlit `Load Selected` path.
3. Refresh `schema_blame` and inspect new tables/columns plus sample rows.
4. Export the evolved database with DuckDB `EXPORT DATABASE` and preserve the
   resulting `schema.sql` as the bootstrap baseline.
5. Compute row/file feature coverage from the silver metadata tables and use a
   greedy set-cover pass to select a small representative corpus.
6. Rebuild a fresh database from `schemas/schema.sql`, load only the selected
   corpus, and compare resulting DLT schema coverage to the full corpus.

## Known Gaps

The original source files appear to be lost or at least not discoverable from
the repository and local DuckDB history alone. The best evidence is paths,
schema names, query fragments, and the `MinamalRows.ipynb` prototype. The exact
selected `min_files_max_schema` output directory was not found during this
recovery pass.

The current `schema_blame` implementation records schema changes and sample
rows, but it does not itself implement the minimum-subset optimizer. The
notebook prototype operates separately over loaded silver tables.

## Related

- [[wiki/pipeline/dlt_load]] - DLT load step that evolves the schema
- [[wiki/schema/silver_layer]] - DLT-loaded silver tables
- [[wiki/schema/gold_layer]] - downstream dbt models
- [[wiki/work/firmware-corpus/brief]] - current curated corpus work that can supply replacement samples
