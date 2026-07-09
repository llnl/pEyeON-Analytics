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

| Page | Component | Confidence | Summary |
|------|-----------|------------|---------|
| [[wiki/components/observe]] | pEyeON-core | high | Observe class: single-file metadata extraction |
| [[wiki/components/parse]] | pEyeON-core | high | Recursive directory scanning via Parse |
| [[wiki/components/load_eyeon]] | pEyeON-analytics | high | DLT loader: EyeOn JSON batches → DuckDB bronze/silver |
| [[wiki/components/dbt_gold]] | pEyeON-analytics | high | dbt project: silver tables → gold reporting/exploration models |
| [[wiki/components/streamlit_app]] | pEyeON-analytics | high | EyeOnData.py: preferred batch loading and exploration UI |
| [[wiki/components/container]] | pEyeON-core | high | Published image, local builds, Docker/Podman runtime matrix |
| [[wiki/components/surfactant_plugins]] | pEyeON-core | medium | pluggy-based metadata extraction architecture |
| [[wiki/components/checksum]] | pEyeON-core | high | Checksum verification (md5/sha1/sha256) |
| [[wiki/components/box_integration]] | pEyeON-core | high | Box auth, list, delete, upload, and parse --upload workflows |

## Schemas

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/schemas/observation_schema]] | high | Full observation JSON schema (observation.schema.json) |
| [[wiki/schemas/silver_layer]] | high | DLT-loaded silver tables: raw_obs and metadata tables |
| [[wiki/schemas/gold_layer]] | high | dbt-built gold.* models for reporting and exploration |

## File Formats

| Page | Filetype Enum | dbt Staging Model | Confidence |
|------|--------------|-------------------|------------|
| [[wiki/file_formats/pe]] | PE, Malformed PE, DOS | stg_metadata_pe_file | high |
| [[wiki/file_formats/elf]] | ELF | stg_metadata_elf_file | high |
| [[wiki/file_formats/macho]] | MACHO32, MACHO64, MACHOFAT, MACHOFAT64, EFIFAT | stg_metadata_mach_o_file | medium |
| [[wiki/file_formats/coff]] | COFF, XCOFF32, XCOFF64, ECOFF | stg_metadata_coff_file | medium |
| [[wiki/file_formats/java]] | JAVACLASS, JAR, WAR, EAR, APK, IPA, MSIX | stg_metadata_java_file | medium |
| [[wiki/file_formats/javascript]] | (js detection via magic) | stg_metadata_js_file | low |
| [[wiki/file_formats/ole]] | OLE, MSCAB, ISCAB | stg_metadata_ole_file | medium |
| [[wiki/file_formats/uimage]] | UIMAGE | stg_metadata_uimage_file | medium |
| [[wiki/file_formats/native_lib]] | AR_LIB, OMF_LIB | stg_metadata_native_lib | low |
| [[wiki/file_formats/archives]] | ZIP, TAR, GZIP, BZIP2, XZ, RAR, ZSTANDARD, CPIO_* | `metadata.container_file` for first-slice extraction | high |
| [[wiki/file_formats/rpm]] | RPM Package | (no staging model yet) | low |
| [[wiki/file_formats/docker]] | DOCKER_GZIP, DOCKER_TAR | (SPDX output) | low |

## Pipeline

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/pipeline/eyeon_parse_sh]] | high | eyeon-parse.sh wrapper: batch output, image selection, runtime and ownership behavior |
| [[wiki/pipeline/dlt_load]] | high | load_eyeon.py: DLT pipeline design and bronze/silver tables |
| [[wiki/pipeline/base_schema_derivation]] | medium | Recovered methodology for deriving `schemas/schema.sql` from representative EyeON samples and schema_blame evidence |
| [[wiki/pipeline/dbt_models]] | high | dbt_eyeon_gold: staging → intermediate → mart lineage |
| [[wiki/pipeline/cert_analysis]] | medium | Certificate extraction, deduplication, expiry/key marts |
| [[wiki/pipeline/metadata_curation]] | high | Silver discovers metadata types; gold curates known types via all_metadata; drift view highlights gaps |

## Decisions

| Page | Status | Summary |
|------|--------|---------|
| [[wiki/decisions/two_repo_split]] | accepted | Core scanner vs. analytics in separate repos |
| [[wiki/decisions/surfactant_plugins]] | accepted | pluggy for extensible format detection |
| [[wiki/decisions/duckdb_dlt_dbt]] | accepted | DuckDB + DLT + dbt as local analytics stack |
| [[wiki/decisions/bronze_silver_gold]] | accepted | Three-layer medallion architecture for EyeON data |
| [[wiki/decisions/feature_work_artifacts]] | accepted | Standard wiki artifacts for LLM-assisted feature work |

## Tensions

| Page | Status | Summary |
|------|--------|---------|
| [[wiki/tensions/parent_field]] | resolved | `parent` is the UUID of the containing file observation for extracted children |
| [[wiki/tensions/archive_recursion]] | resolved | ZIP/TAR/GZIP/BZIP2/XZ, Docker tar/gzip, RAR, and ISO now use the container extraction pattern |
| [[wiki/tensions/rpm_no_staging]] | open | RPM metadata extracted but no gold staging model exists |
| [[wiki/tensions/box_vs_local]] | open | Box upload path vs. fully local operation |
| [[wiki/tensions/filetype_multi]] | open | filetype is an array — multiple types per file; gold models handle this inconsistently |

## Feature Work

| Feature | Status | Summary |
|---------|--------|---------|
| [[wiki/work/binwalk-support/brief]] | started | Binwalk support requirements, references, design, spike, implementation, and verification skeleton |
| [[wiki/work/firmware-corpus/brief]] | first-slice-implemented | JSON manifest, list/fetch/checksum utility, OpenWrt Binwalk smoke candidate, and expanded candidate catalog including utility/industrial sources |
| [[wiki/work/metadata-type-drift/implementation_plan]] | implemented | Add gold.metadata_type_drift and surface it in Schema Blame to show silver-only (unmodeled) metadata types |
| [[wiki/work/ovf-vm-image-build/brief]] | in-progress | Debian 12 aligned container+VM path: Dockerfile refactor into reusable provision scripts, plus Debian 12 qcow2 appliance VM build scaffold |

## Concepts

| Page | Confidence | Summary |
|------|------------|---------|
| [[wiki/concepts/supply_chain_risk]] | medium | OT/ICS software supply chain threat landscape |
| [[wiki/concepts/sbom]] | medium | Software Bill of Materials; EyeON as SBOM-adjacent tooling |
| [[wiki/concepts/fuzzy_hashing]] | high | ssdeep, telfhash, imphash — purpose and use in EyeON |
| [[wiki/concepts/authenticode]] | medium | Windows PE signing: authentihash, LIEF verification |
| [[wiki/concepts/surfactant]] | medium | Upstream LLNL tool EyeON builds on for plugin management |
| [[wiki/concepts/llm_assisted_feature_workflow]] | medium | Lightweight RFC/ADR/spike workflow for LLM-assisted code changes |
| [[wiki/concepts/feature_work_template]] | medium | Reusable skeleton for new feature work folders |

---

*Last updated: 2026-07-09 (updated qcow2 appliance VM notes; refreshed loader schema-bootstrap docs; documented DuckDB CLI policy and alpha VM access decisions)*
