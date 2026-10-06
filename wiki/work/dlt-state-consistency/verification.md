---
title: "Verification: DLT State Consistency"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/tests/test_dlt_state_consistency.py
  - ../pEyeON-Analytics/utils/dlt_state.py
  - ../pEyeON-Analytics/load_eyeon.py
policy: agent-editable
last_validated: 2026-08-31
repo_scope: pEyeON-Analytics
implementation_area: dlt-pipeline
format_domain: none
audience: developer
status: draft
source_paths: wiki/work/dlt-state-consistency/verification.md
tags: [feature-work, dlt, duckdb, schema, state-consistency, verification]
---

# Verification: DLT State Consistency

## Test Commands

```bash
# NOTE: the repo .venv was not executable by the verifying user (permission
# denied on .venv/bin symlinks), so an identical environment was built from
# uv.lock with:
UV_PROJECT_ENVIRONMENT=/home/ubuntu/.cache/eyeon-test-venv uv sync --frozen
uv pip install --python /home/ubuntu/.cache/eyeon-test-venv/bin/python pytest
# On a machine with a working .venv this is equivalent to: uv run pytest tests/ -v
/home/ubuntu/.cache/eyeon-test-venv/bin/python -m pytest tests/ -v
```

## Manual Checks

1. `load_eyeon.py --doctor` smoke test against an empty database with
   `DLT_DATA_DIR`, `EYEON_DUCKDB_PATH` pointed at a tmp dir — printed the
   full report (instance identity, local schema, pending packages, both
   datasets, consistency log) and exited 0.
2. `load_eyeon.py` with no arguments — exits with
   `error: --utility_id and --source are required unless --doctor is given`.
3. `import utils.db; import utils.dlt_state` — clean (no Streamlit import in
   `utils.dlt_state`).
4. `git status` after the test run — `schemas/eyeon_metadata.schema.yaml`
   untouched by tests (the fixture monkeypatches `resolve_dlt_path` so
   pipeline schema exports go to tmp, not the repo).

## Results

```text
============================= test session starts ==============================
platform linux -- Python 3.13.14, pytest-9.1.1, pluggy-1.6.0 -- /home/ubuntu/.cache/eyeon-test-venv/bin/python
cachedir: .pytest_cache
rootdir: /home/ubuntu/git/pEyeON-Analytics
configfile: pyproject.toml
plugins: anyio-4.14.1
collecting ... collected 18 items

tests/test_binwalk_cli.py::BinwalkCliTests::test_missing_json_raises_binwalk_error PASSED [  5%]
tests/test_binwalk_cli.py::BinwalkCliTests::test_missing_target_file_raises_file_not_found PASSED [ 11%]
tests/test_binwalk_cli.py::BinwalkCliTests::test_nonzero_exit_is_preserved_when_json_is_valid PASSED [ 16%]
tests/test_binwalk_cli.py::BinwalkCliTests::test_result_to_dict_includes_extraction_failures PASSED [ 22%]
tests/test_binwalk_cli.py::BinwalkCliTests::test_scan_file_parses_findings_and_extractions PASSED [ 27%]
tests/test_dlt_state_consistency.py::test_ensure_tables_heals_dropped_column PASSED [ 33%]
tests/test_dlt_state_consistency.py::test_db_replaced_under_pipeline_is_reconciled PASSED [ 38%]
tests/test_dlt_state_consistency.py::test_doctor_reports_drift_and_events PASSED [ 44%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_cli_list_json_uses_manifest_path PASSED [ 50%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_default_manifest_loads_and_has_expected_entries PASSED [ 55%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_demo_subset_includes_downloadable_and_manual_entries PASSED [ 61%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_entries_with_fetch_status_for_json_listing PASSED [ 66%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_fetch_file_url_and_verify_checksum PASSED [ 72%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_fetch_git_repo_artifacts_with_local_file_url PASSED [ 77%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_fetch_rejects_checksum_mismatch PASSED [ 83%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_fetch_rejects_non_direct_download_entries PASSED [ 88%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_invalid_manifest_rejects_duplicate_ids PASSED [ 94%]
tests/test_firmware_corpus.py::FirmwareCorpusTests::test_subset_filter_and_table_output PASSED [100%]

============================== 18 passed in 2.43s ==============================
```

Doctor smoke test output (empty DB):

```text
=== EyeON DLT state doctor ===
database: /tmp/doctor-smoke/eyeon.duckdb

-- instance identity --
db instance:      28469f46-75b7-44f9-b220-47246a577dcf
pipeline marker:  <absent>  [no marker (first contact)]
pipeline dir:     /tmp/doctor-smoke/dlt/pipelines/eyeon_metadata

-- local pipeline schema --
schema: <none — pipeline has not run yet>

-- pending load packages --
0 pending: <none>

-- dataset: bronze --
dataset not present in database

-- dataset: silver --
dataset not present in database

-- recent consistency events (_meta.consistency_log) --
<none>
```

## Deviations From Handoff

- The plan's "after a successful run, refresh the sidecar marker" is
  implemented as `dlt_state.refresh_instance_marker()` at the end of
  `main()`; it is normally a no-op because `reconcile_db_instance()` already
  wrote the marker at the start of the run.
- Two refinements discovered by the tests (design-consistent, not new
  decisions):
  - Root-table filtering (`BRONZE_ROOT_TABLES`): the single `eyeon_metadata`
    schema spans both datasets, so heal/drift is scoped per dataset by each
    table's root (bronze owns `raw_json`, silver owns the rest). Without
    this, the guard created bronze tables inside silver — a flaw the
    original dead guard also had.
  - Staging datasets are checked `columns_only`: DLT materializes staging
    tables itself and only for merge chains, so an absent staging table is
    normal; only stale columns on existing staging tables are drift.
- `main()` now closes its DuckDB connection in a `finally` (previously
  leaked; also required for the delete-the-DB regression test to be
  meaningful in-process).

## Known Gaps

- The regression test simulates the interrupted load with an extract-only
  pending package; a package interrupted mid-load (normalized, partially
  loaded) is dropped by the same `drop_pending_packages(with_partial_loads=
  True)` call but is not separately exercised.
- `db.init()`'s `db_initialized` event is not covered by an automated test
  (utils/db.py pulls in Streamlit caching; verified by import check only).
- Doctor output is CLI-only; Streamlit surface deferred per the brief
  (`_meta.consistency_log` is directly queryable in the meantime).

## Follow-Ups

- Streamlit sidebar/diagnostic page rendering `doctor_report()` and
  `_meta.consistency_log`.
- Strip `_dlt_*` tables from `schemas/schema.sql` at the next base-schema
  regeneration.
- Promote the three-store model into a canonical `wiki/diagnostic/` page.
