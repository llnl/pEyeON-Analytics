---
title: "Metadata Type Drift: Verification"
type: pipeline
confidence: medium
grounded_by:
  - dbt_eyeon_gold/models/gold/metadata_type_drift.sql
  - pages/Schema_Blame.py
policy: agent-editable
last_validated: 2026-07-07
repo_scope: pEyeON-Analytics
implementation_area: schema
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/metadata-type-drift/verification.md
tags: [dbt, streamlit, gold]
---

## Commands

1. `uv run python -m compileall pages utils`
1. `uv run python -c "from utils.utils import run_dbt; run_dbt()"`
1. `duckdb -readonly -init /dev/null database/eyeon.duckdb -c "select status, count(*) as n from gold.metadata_type_drift group by 1 order by 1;"`

## Results

- `uv run python -m compileall pages utils EyeOnData.py` (pass)
- `uv run python -c "from utils.utils import run_dbt; run_dbt()"` (PASS=39)
- Multi-batch load regression repro (fixed):
  - `EYEON_DUCKDB_PATH=/var/folders/.../eyeon_two_batch_dltfix2.duckdb uv run python -c "import utils.db as db; import load_eyeon; from utils.utils import run_dbt; db.init(); load_eyeon.main(...batch1...); run_dbt(); load_eyeon.main(...batch2...); run_dbt()"` (PASS)
- `duckdb -readonly -init /dev/null database/eyeon.duckdb -c "select status, count(*) as n from gold.metadata_type_drift group by 1 order by 1;"`
  - modeled: 17
  - unmodeled: 2
