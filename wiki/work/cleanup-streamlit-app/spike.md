---
title: "Feature Spike: Displaying the raw_obs Nested-Table Hierarchy"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/extras/spike_hierarchy_views.py
  - ../pEyeON-Analytics/pages/BrowseDltData.py
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/cleanup-streamlit-app/spike.md
tags: [feature-work, spike, streamlit, hierarchy, dlt, tree-table]
---

# Feature Spike: Displaying the raw_obs Nested-Table Hierarchy

## Question

What is the best way to display the deeply hierarchical structure of
`raw_obs` and its DLT-nested child tables, given that every table in the
tree has a different schema? The Architect's starting idea: a table with
expansion for children.

## Hypothesis

A tree-*table* (uniform columns, expandable rows) fights the data: sibling
tables are heterogeneous, so no single column set works. Re-nesting rows
back into documents may fit better than teaching a grid to walk tables.

## Experiment

Grounded findings from the local `eyeon.duckdb`:

- **The tree is shallow but wide.** Max depth is 3 tables
  (`raw_obs → raw_obs__signatures → raw_obs__signatures__certs`;
  `metadata_java_file → __java_classes → __java_imports/…`;
  `metadata_mach_o_file → __binaries → __dependencies/…`). Eight root
  tables have children; ~30 child tables total. The hard part is schema
  heterogeneity across siblings, not depth.
- **DLT flattening is reversible two ways:**
  1. `bronze.raw_json.json` holds the scanner's **original nested
     document** per uuid (verified: `metadata` dict, `signatures` list with
     nested `certs`). Display requires zero reconstruction.
  2. DuckDB re-nests silver rows cheaply: correlated subqueries with
     `list(to_json(child))` / `struct_pack(...)` on `_dlt_parent_id`
     (verified). `_dlt_root_id` on grandchildren allows whole-subtree
     fetches without recursive joins.
- **Native Streamlit (1.58) provides the needed widgets:** `st.json` is a
  free collapsible tree; `st.column_config.JsonColumn` renders expandable
  JSON cells inside `st.dataframe` (verified present).
- **Prototype** (`extras/spike_hierarchy_views.py`, run with
  `uv run streamlit run extras/spike_hierarchy_views.py`): pick an
  observation, compare three renderings — original bronze document,
  generic recursive re-nest from silver, and a grid with JsonColumn child
  cells. AppTest-verified including the selection path.

## Options Considered

| Option | What it looks like | Verdict |
|---|---|---|
| **A. Document view** — `st.json` of `bronze.raw_json` | Selected row expands into the original nested doc | **Recommend** as the primary detail view: authoritative, zero deps, heterogeneity is a non-issue |
| **B. Re-nest from silver** — DuckDB → `st.json` / `JsonColumn` cells | Same shape but reflects what DLT actually loaded; JsonColumn keeps a sortable grid with expandable child cells | **Recommend** as the grid-integrated variant and for silver-vs-bronze comparison (surfaces normalization) |
| **C. Improved master-detail** (evolve `BrowseDltData`) | Per-level dataframes, each with its own schema; add `@st.fragment` partial reruns + breadcrumbs | Keep — the only option where children stay sortable/filterable tables; fragment upgrade helps regardless |
| **D. AG Grid via streamlit-aggrid** — Tree Data / Master-Detail | The literal "expandable rows, per-detail schemas" | **Not recommended**: both features are AG Grid **Enterprise** (paid license; community fork doesn't unlock them); new heavyweight dependency |
| **E. Tree components** (streamlit-arborist 2025, streamlit-tree-select, antd) | Node/label trees | Wrong shape for row data — but arborist is a candidate to replace the sidebar Schema Info outline with a clickable navigator |
| **F. streamlit-pivot-table** (official component) | Hierarchical row layouts w/ collapse + drill-down panels | Aggregation-first; a candidate for hierarchy *summaries* (counts/bytes by path), not ragged detail |

## Prototype Location

`extras/spike_hierarchy_views.py` (standalone; not registered in app
navigation).

## Results

The document-view insight held: the table hierarchy is an artifact of DLT
flattening, and for *display* the cleanest move is to un-flatten. The
generic recursive re-nester is ~40 lines using only `information_schema`
naming conventions (`table__child`), so it works for every metadata family
without per-table code. JsonColumn keeps the "table" feel the Architect
asked about while absorbing heterogeneity into expandable cells.

## Recommendation

Hybrid, in this order:

1. **Detail view**: selected row → tabs for original document (bronze) and
   re-nested silver doc via `st.json`. Cheapest high-value change; could
   drop straight into BrowseDltData's "Selected Row Details" expander and
   the Observation Hierarchy drilldown.
2. **Grid integration**: for parent tables whose children are small lists
   (filetype, signatures, dependencies), add JsonColumn child columns so
   the main grid shows expandable children inline.
3. **Keep master-detail** for table-oriented exploration; upgrade it with
   `@st.fragment` and breadcrumbs when touched next.
4. Revisit AG Grid only if the lab acquires an AG Grid Enterprise license;
   consider streamlit-arborist later as a Schema Info navigator upgrade.

## Follow-Ups

- Architect to try the prototype and pick which surfaces (BrowseDltData,
  ObservationHierarchy, or a new page) get the document view first.
- Decide whether bronze (original) or silver (loaded) is the default
  detail source — showing both surfaced normalization differences, which
  is itself diagnostic.
- If adopted, promote the re-nester into `utils/` (it belongs next to
  `schema_ext`) with tests.
