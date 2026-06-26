# EyeON Wiki Operation Log

Append-only. Format: `## [YYYY-MM-DD] operation | description`
Query: `grep "^## \[" log.md | tail -10`

---

## [2026-06-26] init | Wiki scaffold from repo scan

Sources read: pEyeON/README.md, pEyeON/CONTRIBUTING.md, pEyeON/src/eyeon/observe.py,
  pEyeON/schema/observation.schema.json, pEyeON/schema/software.schema.json,
  pEyeON/schema/site_report.schema.json, pEyeON-Analytics/README.md,
  pEyeON-Analytics/dbt_eyeon_gold/README.md, pEyeON-Analytics/dbt_eyeon_gold/dbt_project.yml,
  pEyeON-Analytics/dbt_eyeon_gold/models/sources.yml,
  pEyeON-Analytics/dbt_eyeon_gold/models/gold/all_metadata.sql,
  pEyeON-Analytics/EyeOnData.toml-template

Pages created: index.md, log.md + all stubs (see index)
Contradictions flagged: none
Notes: Full scaffold grounded directly in live repo contents. Five genuine open
  tensions identified from schema gaps: parent field, archive recursion, RPM
  staging gap, Box vs local, multi-filetype array handling.

## [2026-06-26] ingest | ../pEyeON/README.md

Pages created: none
Pages updated: wiki/overview/project.md, wiki/overview/architecture.md,
  wiki/components/parse.md, wiki/components/checksum.md,
  wiki/components/box_integration.md, wiki/components/container.md,
  wiki/pipeline/eyeon_parse_sh.md, wiki/decisions/two_repo_split.md,
  wiki/index.md
Contradictions flagged: none
Notes: Replaced README-backed stubs with grounded content for the field
  quickstart, wrapper behavior, published container image, Docker/Podman runtime
  and ownership behavior, core parse/checksum commands, optional Box workflows,
  and the collection-vs-analytics repository boundary.

## [2026-06-26] ingest | ../pEyeON-Analytics/README.md

Pages created: none
Pages updated: wiki/overview/project.md, wiki/overview/architecture.md,
  wiki/overview/data_flow.md, wiki/components/load_eyeon.md,
  wiki/components/dbt_gold.md, wiki/components/streamlit_app.md,
  wiki/pipeline/dlt_load.md, wiki/pipeline/dbt_models.md,
  wiki/pipeline/eyeon_parse_sh.md, wiki/schemas/silver_layer.md,
  wiki/schemas/gold_layer.md, wiki/decisions/duckdb_dlt_dbt.md,
  wiki/decisions/bronze_silver_gold.md, wiki/index.md
Contradictions flagged: none
Notes: Populated analytics workflow pages from the local README: Python/uv
  quickstart, EyeOnData.toml dataset/database configuration, Streamlit-led
  `Load Selected` path, optional manual loader/dbt commands, repo layout, and
  the EyeOn JSON -> DLT bronze/silver -> dbt gold -> Streamlit data flow.

## [2026-06-26] process | LLM-assisted feature workflow

Pages created: wiki/concepts/llm_assisted_feature_workflow.md,
  wiki/concepts/feature_work_template.md,
  wiki/decisions/feature_work_artifacts.md
Pages updated: wiki/index.md
Contradictions flagged: none
Notes: Recorded the agreed lightweight feature workflow for LLM-assisted coding:
  feature brief, references, design, optional spike, implementation plan,
  verification, and closeout promotion into canonical wiki pages. Accepted
  `wiki/work/<feature-slug>/` as the location for future feature-work artifacts.
