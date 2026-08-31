# EyeON Wiki Index

Master catalog of all pages. Updated by the agent on every ingest.

---

## Overview

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/overview/project]] | high | What EyeON is, motivation, two-repo structure |
| [[wiki/overview/architecture]] | medium | Core collection repo + analytics companion boundary; wrapper-driven collection path |
| [[wiki/overview/data_flow]] | high | EyeOn JSON → DLT bronze/silver → dbt gold → Streamlit pages |

## Components

| Page | Repo Scope | Confidence | Summary |
|------|-----------|------------|---------|
| [[wiki/component/observe]] | pEyeON | high | Observe class: single-file metadata extraction |
| [[wiki/component/parse]] | pEyeON | high | Recursive directory scanning via Parse |
| [[wiki/component/load_eyeon]] | pEyeON-Analytics | high | DLT loader: EyeOn JSON batches → DuckDB bronze/silver |
| [[wiki/component/dbt_gold]] | pEyeON-Analytics | high | dbt project: silver tables → gold reporting/exploration models |
| [[wiki/component/streamlit_app]] | pEyeON-Analytics | high | EyeOnData.py: preferred batch loading and exploration UI |
| [[wiki/component/container]] | pEyeON | high | Published image, local builds, Docker/Podman runtime matrix |
| [[wiki/component/surfactant_plugins]] | pEyeON | medium | pluggy-based metadata extraction architecture |
| [[wiki/component/checksum]] | pEyeON | high | Checksum verification (md5/sha1/sha256) |
| [[wiki/component/box_integration]] | pEyeON | high | Box auth, list, delete, upload, and parse --upload workflows |

## Schemas

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/schema/observation_schema]] | high | Full observation JSON schema (observation.schema.json) |
| [[wiki/schema/silver_layer]] | high | DLT-loaded silver tables: raw_obs and metadata tables |
| [[wiki/schema/gold_layer]] | high | dbt-built gold.* models for reporting and exploration |

## File Formats

| Page | Filetype Enum | dbt Staging Model | Confidence |
|------|--------------|-------------------|------------|
| [[wiki/file_format/pe]] | PE, Malformed PE, DOS | stg_metadata_pe_file | high |
| [[wiki/file_format/elf]] | ELF | stg_metadata_elf_file | high |
| [[wiki/file_format/macho]] | MACHO32, MACHO64, MACHOFAT, MACHOFAT64, EFIFAT | stg_metadata_mach_o_file | medium |
| [[wiki/file_format/coff]] | COFF, XCOFF32, XCOFF64, ECOFF | stg_metadata_coff_file | medium |
| [[wiki/file_format/java]] | JAVACLASS, JAR, WAR, EAR, APK, IPA, MSIX | stg_metadata_java_file | medium |
| [[wiki/file_format/javascript]] | (js detection via magic) | stg_metadata_js_file | low |
| [[wiki/file_format/ole]] | OLE, MSCAB, ISCAB | stg_metadata_ole_file | medium |
| [[wiki/file_format/uimage]] | UIMAGE | stg_metadata_uimage_file | medium |
| [[wiki/file_format/native_lib]] | AR_LIB, OMF_LIB | stg_metadata_native_lib | low |
| [[wiki/file_format/archives]] | ZIP, TAR, GZIP, BZIP2, XZ, RAR, ZSTANDARD, CPIO_* | `metadata.container_file` for first-slice extraction | high |
| [[wiki/file_format/rpm]] | RPM Package | (no staging model yet) | low |
| [[wiki/file_format/docker]] | DOCKER_GZIP, DOCKER_TAR | (SPDX output) | low |

## Pipeline

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/pipeline/eyeon_parse_sh]] | high | eyeon-parse.sh wrapper: batch output, image selection, runtime, ownership, and interactive progress behavior |
| [[wiki/pipeline/dlt_load]] | high | load_eyeon.py: DLT pipeline design and bronze/silver tables |
| [[wiki/pipeline/base_schema_derivation]] | medium | Recovered methodology for deriving `schemas/schema.sql` from representative EyeON samples and schema_blame evidence |
| [[wiki/pipeline/dbt_models]] | high | dbt_eyeon_gold: staging → intermediate → mart lineage |
| [[wiki/pipeline/cert_analysis]] | medium | Certificate extraction, deduplication, expiry/key marts |
| [[wiki/pipeline/metadata_curation]] | high | Silver discovers metadata types; gold curates known types via all_metadata; drift view highlights gaps |

## Diagnostics

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/diagnostic/dlt-three-store-consistency]] | high | The three stores of DLT state, the self-heal/reconcile layer (utils/dlt_state.py), `load_eyeon.py --doctor`, `_meta.consistency_log`, and the dev DB reset recipe |

## Decisions

| Page | Status | Summary |
|------|--------|---------|
| [[wiki/decision/two_repo_split]] | accepted | Core scanner vs. analytics in separate repos |
| [[wiki/decision/surfactant_plugins]] | accepted | pluggy for extensible format detection |
| [[wiki/decision/duckdb_dlt_dbt]] | accepted | DuckDB + DLT + dbt as local analytics stack |
| [[wiki/decision/bronze_silver_gold]] | accepted | Three-layer medallion architecture for EyeON data |
| [[wiki/decision/feature_work_artifacts]] | accepted | Standard wiki artifacts for LLM-assisted feature work |
| [[wiki/decision/2026-08-27-adopt-velocity-mini-lab]] | accepted | Adopt the interview stage and Velocity metrics overlay from the Wintap ecosystem |
| [[wiki/decision/2026-08-27-report-generation-typst]] | accepted | Typst (via typst pip bindings) is the EyeON report-generation engine; WeasyPrint runner-up, Jasper fallback |

## Tensions

| Page | Status | Summary |
|------|--------|---------|
| [[wiki/tension/parent_field]] | resolved | `parent` is the UUID of the containing file observation for extracted children |
| [[wiki/tension/archive_recursion]] | resolved | ZIP/TAR/GZIP/BZIP2/XZ, Docker tar/gzip, RAR, and ISO now use the container extraction pattern |
| [[wiki/tension/rpm_no_staging]] | open | RPM metadata extracted but no gold staging model exists |
| [[wiki/tension/box_vs_local]] | open | Box upload path vs. fully local operation |
| [[wiki/tension/filetype_multi]] | open | filetype is an array — multiple types per file; gold models handle this inconsistently |

## Feature Work

| Feature | Status | Summary |
|---------|--------|---------|
| [[wiki/work/binwalk-support/brief]] | started | Binwalk support requirements, references, design, spike, implementation, and verification skeleton |
| [[wiki/work/implement-a-report-generator-ability/brief]] | closed | Report-generation tool selected: Typst (ADR 2026-08-27) after 12-candidate matrix + WeasyPrint/Typst spikes on real data; first feature closed under the interview + Velocity workflow (11.2×, Low confidence) |
| [[wiki/work/report-generator-implementation/brief]] | handoff-approved | Implement the Typst report ability: reports/ package, eyeon-report CLI, Streamlit Reports page (batch changes + dossier, PDF, plotly charts); Developer session implements from approved handoff (2026-08-27) |
| [[wiki/work/firmware-corpus/brief]] | first-slice-implemented | JSON manifest, list/fetch/checksum utility, OpenWrt Binwalk smoke candidate, and expanded candidate catalog including utility/industrial sources |
| [[wiki/work/metadata-type-drift/implementation_plan]] | implemented | Add gold.metadata_type_drift and surface it in Schema Blame to show silver-only (unmodeled) metadata types |
| [[wiki/work/ovf-vm-image-build/brief]] | in-progress | Debian 12 aligned container+VM path: Dockerfile refactor into reusable provision scripts, plus Debian 12 qcow2 appliance VM build scaffold |
| [[wiki/work/vm-image-size-reduction/brief]] | proposed | Future task: reduce qcow2 size by stripping build toolchains, caches, and optional analytics payload rather than chasing nonexistent GUI/X11 weight |
| [[wiki/work/parse-multiprocessing-hang/implementation_plan]] | implemented | Fix core parse multiprocessing hang on large Mach-O binaries by using spawn-based workers and recycling each worker after one file |
| [[wiki/work/parse-terminal-output/brief]] | proposed | Future task: centralize parse worker output in the parent process and consider Rich for stable progress plus readable logs |
| [[wiki/work/update-loreforge/brief]] | implemented | Migrate AGENTS.md and wiki to the loreforge Engineer/Developer role model, singular directories, and new frontmatter schema (reverse-engineered workflow record) |
| [[wiki/work/dlt-state-consistency/brief]] | implemented | Heal/detect/explain/record dlt three-store state drift (deleted dev DB + surviving pending packages → Binder Error); lightweight workflow variant, in-session interview + approval; durable facts promoted to [[wiki/diagnostic/dlt-three-store-consistency]] (2026-08-31) |

## Concepts

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/concept/supply_chain_risk]] | medium | OT/ICS software supply chain threat landscape |
| [[wiki/concept/sbom]] | medium | Software Bill of Materials; EyeON as SBOM-adjacent tooling |
| [[wiki/concept/fuzzy_hashing]] | high | ssdeep, telfhash, imphash — purpose and use in EyeON |
| [[wiki/concept/authenticode]] | medium | Windows PE signing: authentihash, LIEF verification |
| [[wiki/concept/surfactant]] | medium | Upstream LLNL tool EyeON builds on for plugin management |
| [[wiki/concept/llm_assisted_feature_workflow]] | medium | Lightweight RFC/ADR/spike workflow for LLM-assisted code changes, with interview stage and Velocity metrics overlay |
| [[wiki/concept/feature_work_template]] | medium | Reusable skeleton for new feature work folders (incl. interview.md, metrics.md) |
| [[wiki/concept/velocity-metric]] | high | Velocity = solo-hours / (5.714 × days): one number for AI-assisted delivery speed |
| [[wiki/concept/metrics-template]] | high | Per-feature metrics.md format: Results scorecard + canonical YAML record |
| [[wiki/concept/build_glossary]] | medium | Builder glossary: containers, qcow2 appliance, multi-arch, and common gotchas |

## Metrics

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/metrics]] | high | Cross-feature Velocity rollup: one row per closed feature |

---

*Last updated: 2026-08-27 (adopted interview stage + Velocity metrics overlay from the Wintap ecosystem: new ADR, velocity-metric and metrics-template concepts, wiki/metrics rollup; workflow and template pages updated)*
