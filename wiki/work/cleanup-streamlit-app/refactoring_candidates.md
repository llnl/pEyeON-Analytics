---
title: "Refactoring Candidates: Cleanup Streamlit App"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/pages/certs.py
  - ../pEyeON-Analytics/pages/EyeOnSummary.py
  - ../pEyeON-Analytics/pages/ObservationHierarchy.py
  - ../pEyeON-Analytics/pages/Schema_Blame.py
  - ../pEyeON-Analytics/utils/search_forms.py
  - ../pEyeON-Analytics/utils/batches.py
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/cleanup-streamlit-app/refactoring_candidates.md
tags: [feature-work, streamlit, refactoring, code-sharing]
---

# Refactoring Candidates: Cleanup Streamlit App

Engineer pass (2026-08-17, post-Phase 1) looking for code shared or
duplicated across pages, packaged as a small number of utility/library
classes per Architect preference. Ordered by evidence strength.

## Duplication inventory

### A. Metadata-type catalog — strongest candidate, 7 call sites

The notion of "what metadata types exist and what do we call them" is
re-implemented across the app:

1. `pages/ObservationHierarchy.py §_base_metadata_tables` and
   `utils/search_forms.py §_silver_metadata_base_tables` — **identical
   information_schema query** (`left(table_name, 9) = 'metadata_'` +
   `instr(table_name, '__') = 0`), one cached, one not.
2. `pages/EyeOnSummary.py` and `utils/search_forms.py §raw_obs_summary` —
   **verbatim duplicate block**: `list_sort(list(distinct
   _metadata_table_name)) from gold.all_metadata`, the
   `removeprefix("metadata_").removesuffix("_file")` dance, and the same
   `["_None_"]` fallback.
3. `pages/ObservationHierarchy.py §METADATA_LABELS/_metadata_label` —
   friendly display names.
4. `utils/search_forms.py §_curated_metadata_type_keys /
   _silver_table_for_type_key` — type-key ↔ silver-table mapping including
   the `native_lib` naming-quirk workaround, plus dbt `gold_staging`
   discovery with static fallback.
5. `pages/ObservationHierarchy.py §_metadata_union_sql` and
   `utils/search_forms.py` uuid-union building — same union-all-over-
   metadata-tables construction, different column sets.
6. `pages/Schema_Blame.py` — ad-hoc `str.replace(r"^metadata_", "")`.

**Proposal:** `utils/metadata_catalog.py` with a `MetadataCatalog` class
(cached discovery; properties/methods: `silver_tables()`,
`curated_type_keys()`, `label(table)`, `short_name(table)`,
`silver_table_for(type_key)`, `union_sql(columns)`, `loaded_type_names()`).
A class fits naturally here: it owns cached DB introspection state and the
naming quirks live in one place.

### B. Guarded query → DataFrame — every page

Same three-line patterns everywhere:
- `pages/certs.py §_query_df` — `db.get_conn().sql(sql).df()`.
- `pages/EyeOnSummary.py` and `pages/Schema_Blame.py` (drift block) — the
  same try/except → `st.warning(...not available yet...)` +
  `st.caption(f"{type(e).__name__}: {e}")` + empty DataFrame shape.
- `pages/BrowseDltData.py` — `duckdb.CatalogException` → info message.
- ~25 raw `db.get_conn()` call sites across pages/utils.

**Proposal:** extend `utils/db.py` (or new `utils/queries.py`) with a small
`Query` façade: `df(sql, params=None)`, `scalar(sql)`,
`try_df(sql, missing_msg=None) -> DataFrame` (guarded, renders the standard
warning/caption), `existing_tables(schema)`, `missing_tables(schema,
required)`. Kills the certs `_missing_models` preflight, the
EyeOnSummary/Schema_Blame guard blocks, and standardizes the "model not
materialized yet" UX.

### C. Streamlit widget helpers — 3–5 call sites each

- **Row-selection dataframe:** `utils/utils.py §batch_selector` and
  `pages/EyeOnSummary.py §All Batches` share the exact
  `event.selection.rows → iloc → replace({pd.NA: None}) →
  to_dict("records")` sequence; `pages/BrowseDltData.py` has the
  single-row + persistence + highlight variant.
- **Metric rows:** 6 hand-rolled `st.columns` + `.metric` blocks
  (EyeOnSummary ×2, certs, Schema_Blame ×2, search_forms).
- **Widget shadow-state:** `pages/debug_page.py` hand-rolls the
  cross-page-persistence hack twice; legacy `common/utils.py` already had
  generic `store_keys`/`load_keys`.

**Proposal:** `utils/st_widgets.py` — functions (no state to own):
`select_rows(df, key, multi=True) -> list[dict]`, `metric_row(mapping)`,
`persistent_state(key, default)`. The BrowseDltData single-row/highlight
variant can migrate later; it is entangled with the fragile-coupling area
that is out of scope this round.

### D. SQL text hygiene — 4+ call sites

- `pages/ObservationHierarchy.py §_sql_literal` (quoting).
- The `replace('*', '%')` wildcard→ilike dance in 3 places
  (search_forms uuid + filename, ObservationHierarchy filename filter).
- `utils/search_forms.py` interpolates user input directly into SQL
  (`uuid ilike '%{filter_uuid...}%'`) — injection-prone; local analytics
  app, but worth parameterizing whenever this module is touched.

**Proposal:** `utils/sqlutil.py` — `sql_literal(s)`,
`ilike_pattern(user_text)`, and a `conditions_to_where(list)` helper;
migrate search_forms to bound parameters opportunistically.

### E. `utils/utils.py` split — cohesion, not duplication

Post-Phase 1 it still mixes four concerns (~330 lines):
init/onboarding form, batch-dir domain logic, pipeline orchestration, and
sidebar rendering.

**Proposal:** split by concern, no behavior change:
- `utils/batches.py` — `BatchDir`, `parse_batch_dir*`, `list_dirs`,
  `list_all_batches` (and the known empty-frame `directory_path` bug fixed
  in passing, per verification.md finding, if approved).
- `utils/loader.py` — `load_me_some_data`, `load_data`, `run_dbt`.
- `utils/app_init.py` — `init_app_form`, `batch_selector`.
- `utils/sidebar.py` — `_db_settings`, `sidebar_db_chooser`.
- `utils/utils.py` retires (or keeps thin re-exports for one release).

## Deliberately excluded this pass

- Chart/color standardization (certs mixes `st.bar_chart` + Altair;
  Schema_Blame hardcodes a palette) — belongs to the Architect's UIX
  phase, not a mechanical refactor.
- BrowseDltData's recursive master-detail machinery — single-site code
  entangled with out-of-scope fragile coupling.
- `common/`/`extras/` dispositions — separate decision (see brief).

## Suggested packaging (Architect preference: a few classes)

| Module | Shape | Why |
|---|---|---|
| `utils/metadata_catalog.py` | **class** `MetadataCatalog` | Owns cached introspection state + naming quirks |
| `utils/queries.py` | **class** `Query` (thin façade over db.get_conn) | Groups guarded-query UX; mockable seam for future tests |
| `utils/st_widgets.py` | functions | Stateless render helpers; a class adds nothing |
| `utils/sqlutil.py` | functions | Pure string/parameter helpers |
| `utils/{batches,loader,app_init,sidebar}.py` | split of utils.py | Cohesion |

Estimated impact: ~150–200 duplicated lines removed; every future page
gets guarded queries, metadata naming, and selection widgets for free.

## Suggested sequencing

1. **Slice A:** `MetadataCatalog` + `Query` and migrate call sites — the
   two highest-evidence wins, independently verifiable page by page.
2. **Slice B:** `st_widgets` + `sqlutil` migrations.
3. **Slice C:** `utils/utils.py` split (mechanical; large diff, zero logic
   change) + optional `list_dirs` bug fix.

## Related

- [[wiki/work/cleanup-streamlit-app/current_state]] — Phase 0 analysis
- [[wiki/work/cleanup-streamlit-app/design]] — settled phases
- [[wiki/work/cleanup-streamlit-app/verification]] — Phase 1 results + known bugs
