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

## [2026-06-26] feature-start | Binwalk Support

Pages created: wiki/work/binwalk-support/brief.md,
  wiki/work/binwalk-support/references.md,
  wiki/work/binwalk-support/design.md,
  wiki/work/binwalk-support/spike.md,
  wiki/work/binwalk-support/implementation_plan.md,
  wiki/work/binwalk-support/verification.md
Pages updated: wiki/index.md
Contradictions flagged: none
Notes: Started the Binwalk Support feature using the LLM-assisted feature
  workflow. Initial open questions focus on whether support means detection,
  extraction, or both; where it belongs in the scanner/analytics workflow; and
  how extracted children should relate to existing parent/archive tensions.

## [2026-06-26] feature-update | Binwalk Support brief review

Pages created: none
Pages updated: wiki/work/binwalk-support/brief.md,
  wiki/work/binwalk-support/references.md,
  wiki/work/binwalk-support/design.md
Contradictions flagged: none
Notes: Reviewed human updates to Binwalk Support requirements. Marked detection
  plus extraction as the desired direction, clarified that extracted children
  should become normal EyeON observations, deferred full parent/child lineage,
  added draft acceptance criteria and test plan items, and recorded Binwalk v3 / 
  binwalk3 as candidate integration paths.

## [2026-06-26] feature-start | Firmware Corpus

Pages created: wiki/work/firmware-corpus/brief.md,
  wiki/work/firmware-corpus/references.md,
  wiki/work/firmware-corpus/design.md,
  wiki/work/firmware-corpus/spike.md,
  wiki/work/firmware-corpus/implementation_plan.md,
  wiki/work/firmware-corpus/verification.md
Pages updated: wiki/work/binwalk-support/brief.md,
  wiki/work/binwalk-support/references.md, wiki/index.md
Contradictions flagged: none
Notes: Split the firmware corpus work into its own feature slice. Scope covers a
  curated firmware URL manifest, local fetch/cache utility, EyeON parse workflow,
  demo corpus entries, and a small deterministic subset for Binwalk and unit or
  opt-in integration tests. Seeded candidate categories and URLs from
  raw/binwalk/binwalk.md.

## [2026-06-26] handoff | Firmware Corpus dev agent

Pages created: wiki/work/firmware-corpus/dev_handoff.md
Pages updated: wiki/work/firmware-corpus/brief.md,
  wiki/work/firmware-corpus/implementation_plan.md, wiki/index.md
Contradictions flagged: none
Notes: Created a copy/paste handoff prompt and development packet for the first
  Firmware Corpus implementation slice. The handoff focuses on manifest format,
  initial candidate entries, list/fetch utility behavior, non-network default
  tests, git hygiene for downloaded firmware, and closeout updates back into the
  feature-work wiki pages.

## [2026-06-26] process | AGENTS operating modes

Pages created: none
Pages updated: wiki/work/firmware-corpus/dev_handoff.md
Files updated: AGENTS.md
Contradictions flagged: none
Notes: Updated AGENTS.md to define explicit wiki-maintainer and code-development
  modes. Wiki-maintainer mode remains the default and writes only to wiki. Code
  development mode is activated by explicit user request and may modify source,
  tests, configs, scripts, documentation, and wiki files in this repository while
  keeping raw sources and ../pEyeON protected unless separately authorized.
