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

## [2026-07-07] feature-impl | Metadata Type Drift

Pages created: wiki/pipeline/metadata_curation.md,
  wiki/work/metadata-type-drift/implementation_plan.md,
  wiki/work/metadata-type-drift/verification.md
Pages updated: wiki/index.md
Contradictions flagged: none
Notes: Added `gold.metadata_type_drift` (silver discovered vs gold curated) and
  surfaced it in the Streamlit Schema Blame page to make “unmodeled metadata
  types” obvious when new DLT metadata tables appear but dbt `all_metadata` has
  not been updated.
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

## [2026-06-26] implementation | Firmware Corpus first slice

Pages created: none
Pages updated: wiki/work/firmware-corpus/implementation_plan.md,
  wiki/work/firmware-corpus/verification.md,
  wiki/work/binwalk-support/brief.md, wiki/index.md
Files updated: AGENTS.md, .gitignore, data/firmware_corpus/manifest.json,
  utils/firmware_corpus.py, tests/test_firmware_corpus.py
Contradictions flagged: none
Notes: Added a JSON firmware corpus manifest, a dependency-free Python utility
  for listing entries, direct-download fetches, and SHA-256 verification, plus
  non-network unit tests. Selected the OpenWrt Carambola2 image as the first
  Binwalk smoke fixture candidate. Documented `uv` as the Python command runner;
  verification used `/opt/homebrew/bin/uv` because `uv` was not on PATH.

## [2026-06-26] spike | Binwalk Python API install options

Pages created: none
Pages updated: wiki/work/binwalk-support/spike.md
Contradictions flagged: none
Notes: Explored `binwalk3` as a Python API wrapper over Binwalk v3. The package
  installs and imports cleanly with `uv run --with binwalk3`, but it does not
  bundle a macOS binary and scan calls fail without `binwalk3` or `binwalk` on
  PATH. Homebrew has a bottled `binwalk` 3.1.0 formula with `sevenzip` as its
  required dependency, making that the likely macOS smoke-test path if approved.

## [2026-06-26] spike-update | Binwalk macOS CLI and Python API smoke test

Pages created: none
Pages updated: wiki/work/binwalk-support/spike.md
Contradictions flagged: none
Notes: After Homebrew `binwalk` was installed, direct CLI detection and JSON
  logging worked against the OpenWrt corpus fixture. The `binwalk3` Python API
  also detected `uimage` and `squashfs` once the CLI binary was on PATH. Extraction
  was partial: built-in `uimage` extraction succeeded, while `squashfs` extraction
  failed because Binwalk v3 expects `sasquatch`, which was not installed and was
  not available as a Homebrew formula. Direct CLI JSON recorded extraction success
  and failure; the Python wrapper did not surface the missing extractor in
  `module.errors`, so direct CLI JSON is currently the better integration target.

## [2026-06-26] implementation | Binwalk direct CLI wrapper spike

Pages created: none
Pages updated: wiki/work/binwalk-support/implementation_plan.md,
  wiki/work/binwalk-support/verification.md,
  wiki/work/binwalk-support/spike.md
Files updated: utils/binwalk_cli.py, tests/test_binwalk_cli.py
Contradictions flagged: none
Notes: Added a dependency-free Binwalk v3 CLI wrapper that runs `binwalk -l`,
  parses `Analysis.file_map` and `Analysis.extractions`, and preserves stdout,
  stderr, exit status, command, raw JSON, and extraction failures. Default tests
  use a fake Binwalk executable. Real smoke tests against the OpenWrt corpus
  fixture confirmed detection plus partial extraction diagnostics, including the
  missing `sasquatch` failure for SquashFS extraction.

## [2026-06-26] implementation | Firmware Corpus git-repo artifacts

Pages created: none
Pages updated: wiki/work/firmware-corpus/design.md,
  wiki/work/firmware-corpus/implementation_plan.md,
  wiki/work/firmware-corpus/verification.md
Files updated: data/firmware_corpus/manifest.json, utils/firmware_corpus.py,
  tests/test_firmware_corpus.py
Contradictions flagged: none
Notes: Extended the corpus manifest to support per-entry artifacts, including
  multiple files inside a `git-repo` source. Updated DVRF to point at
  `Firmware/DVRF_v03.bin` as a fetchable firmware artifact and the firmware
  license HTML as a referenced non-fetch artifact. Listing now shows whether an
  entry downloads and how many artifacts are fetchable; fetching logs each entry,
  artifact action, checksum, skipped entry, and final fetched count. Demo fetch
  now downloads OpenWrt and DVRF while explicitly skipping the D-Link manual page.

## [2026-06-29] implementation | Core container extraction pattern

Pages created: none
Pages updated: wiki/components/parse.md, wiki/file_formats/archives.md,
  wiki/tensions/parent_field.md, wiki/tensions/archive_recursion.md,
  wiki/index.md
Files updated in ../pEyeON: src/eyeon/container.py, src/eyeon/parse.py,
  src/eyeon/observe.py, schema/observation.schema.json, tests/testParse.py,
  tests/testObserve.py
Contradictions flagged: none
Notes: Implemented the first core archive recursion slice in pEyeON. Supported
  containers now receive `metadata.container_file`; `eyeon parse` extracts
  ZIP/TAR/GZIP/BZIP2/XZ containers to temporary directories, recursively observes
  extracted children, and sets child `parent` to the container observation UUID.
  Targeted tests passed. Full `tests.testObserve` still has existing macOS
  environment failures for `/etc/shadow` and `/root` permission assumptions.

## [2026-06-29] implementation | Core RAR ISO Docker container expansion

Pages created: none
Pages updated: wiki/components/parse.md, wiki/file_formats/archives.md,
  wiki/tensions/archive_recursion.md, wiki/index.md
Files updated in ../pEyeON: src/eyeon/container.py, tests/testParse.py
Contradictions flagged: none
Notes: Expanded the core container extraction helper to include Docker tar/gzip,
  RAR, and ISO_9660_CD. Docker archives are treated as tar-like containers and
  layer tars recurse through the existing tar path. RAR extraction uses `rarfile`
  and depends on external RAR tooling. ISO extraction uses `7zz`, `7z`, or an
  `EYEON_7Z_PATH` override. Added tests for Docker tar parent chains and ISO
  extraction through a fake 7z-compatible executable. `tests.testParse` passed.

## [2026-06-29] implementation | Core Binwalk CLI parse support

Pages created: none
Pages updated: wiki/work/binwalk-support/design.md,
  wiki/work/binwalk-support/implementation_plan.md,
  wiki/work/binwalk-support/verification.md
Files updated in ../pEyeON: src/eyeon/container.py, src/eyeon/parse.py,
  schema/observation.schema.json, tests/testParse.py, tests/testObserve.py
Contradictions flagged: none
Notes: Integrated direct Binwalk v3 CLI support into core parse flow. Binwalk is
  invoked for firmware-like inputs (`UIMAGE` or common firmware extensions) and

  can be controlled with `EYEON_BINWALK`, `EYEON_BINWALK_PATH`. The original
  observation records `metadata.binwalk_file`; extracted Binwalk children are
  recursively observed with `parent` set to the firmware observation UUID. Tests
  use a fake Binwalk executable, so default verification does not require
  Binwalk, firmware downloads, or network access. The Python API wrapper remains
  intentionally unused for core support.

## [2026-06-29] implementation | Binwalk container packaging

Pages created: none
Pages updated: wiki/components/container.md,
  wiki/work/binwalk-support/verification.md
Files updated in ../pEyeON: builds/Dockerfile, builds/podman.Dockerfile,
  .github/workflows/test-build-container.yaml, README.md
Contradictions flagged: none
Notes: Updated Docker and Podman image builds to compile Binwalk v3.1.0 in the
  builder stage, copy the CLI into the runtime image, and install runtime
  extractors including 7-Zip, sasquatch, unar, zstd, lz4, lzop, sleuthkit,
  cabextract, and device-tree-compiler. Local Docker builds succeeded for both
  Dockerfile paths; runtime smoke checks confirmed `eyeon`, `binwalk`,
  `sasquatch`, `7z`, and `unar`. A true local Podman run was not possible because
  the Lima Podman VM is not configured, but the Podman Dockerfile built
  successfully via Docker.

## [2026-06-29] verification | Binwalk container parse over Downloads test corpus

Pages created: none
Pages updated: wiki/work/binwalk-support/verification.md
Contradictions flagged: none
Notes: Ran the Binwalk-enabled Docker image against `/Users/johnson30/Downloads/test`
  with results at `/Users/johnson30/Downloads/eyeon-test-binwalk-results`. The
  run produced 2,129 JSON observations, including 2,124 child observations with
  `parent`, 9 Binwalk metadata observations, and 5 container metadata observations.
  Subdirectories were traversed, including DVRF and OpenWrt firmware folders.
  SquashFS extraction worked in the container via `sasquatch`. Follow-up issues:
  one extracted `mtab` observation recorded `[Errno 22] Invalid argument`, and
  large ISO-derived `install.img`/`rootfs.img` Binwalk scans exited `-9` with no
  findings.

## [2026-06-29] research | Firmware Corpus candidate expansion

Pages created: wiki/work/firmware-corpus/candidates.md
Pages updated: wiki/work/firmware-corpus/brief.md,
  wiki/work/firmware-corpus/references.md,
  wiki/work/firmware-corpus/design.md,
  wiki/work/firmware-corpus/dev_handoff.md, wiki/index.md
Contradictions flagged: none
Notes: Expanded the firmware corpus candidate pool from web research. Added
  deliberately vulnerable candidates (OWASP IoTGoat, DVRF variants), clean/open
  baselines (OpenWrt, Tasmota, OpenIPC, ESPHome), bulk indexes (iotwizz,
  FirmSecDataset, FIRMADYNE, FirmAE, Netgear GPL Archive), and vulnerability
  story references including CVE-2020-7982 and CVE-2018-1160. Suggested subsets:
  `unit-small`, `binwalk-smoke`, `demo-vulnerable`, `demo-baseline`, and
  `bulk-index`.

## [2026-06-29] research | Industrial Firmware Corpus candidates

Pages created: none
Pages updated: wiki/work/firmware-corpus/candidates.md,
  wiki/work/firmware-corpus/references.md,
  wiki/work/firmware-corpus/brief.md,
  wiki/work/firmware-corpus/dev_handoff.md, wiki/index.md
Contradictions flagged: none
Notes: Added industrial and utility-adjacent corpus candidates. Public or
  semi-public sources include Moxa Software & Documentation, Schneider Electric
  downloads, WAGO software, Beckhoff Download Finder, and HMS Networks General
  Downloads for Anybus/Ewon/Ixxat/N-Tron/Red Lion. Added manual-investigation
  candidates for Teltonika, AutomationDirect, SEL, Eaton, Phoenix Contact, Digi,
  and Belden/Hirschmann where automated fetches failed or access appears gated.
  Added a `demo-utility` subset concept for utility/industrial show-and-tell
  samples without requiring known vulnerabilities.

## [2026-06-30] implementation | Expanded Firmware Corpus download and parse

Pages created: none
Pages updated: wiki/work/firmware-corpus/verification.md,
  wiki/work/firmware-corpus/implementation_plan.md
Files updated: data/firmware_corpus/manifest.json,
  tests/test_firmware_corpus.py
Contradictions flagged: none
Notes: Expanded the source-controlled corpus manifest with Tasmota, ESPHome,
  OWASP IoTGoat, and OpenIPC release assets, increasing downloadable artifacts
  from 2 to 82. Downloaded all fetchable artifacts to
  `/Users/johnson30/data/eyeon-corpus-downloads-expanded` and parsed them with
  the Binwalk-enabled container image. JSON output is at
  `/Users/johnson30/data/eyeon-corpus-json-expanded/20260630T052134Z_CORPUSEXP`
  with 2,998 observations. Known follow-ups: extracted `mtab` still records one
  metadata error, and `.bin`/`.bin.gz` artifact pairs can collapse to the same
  `<filename>.<md5>.json` output name.

## [2026-06-30] implementation | Core generic metadata fallback

Pages created: none
Pages updated: wiki/components/observe.md, wiki/schemas/observation_schema.md,
  wiki/work/firmware-corpus/verification.md
Files updated in ../pEyeON: src/eyeon/observe.py,
  schema/observation.schema.json, tests/testObserve.py
Contradictions flagged: none
Notes: Added EyeON-owned generic metadata fallback logic without adding surfactant
  plugins. Files without deep format-specific metadata now classify into
  `opkg_file`, `text_file`, `web_asset`, `image_file`, `symlink_file`,
  `linux_kernel_image`, `device_tree_file`, or `generic_file` instead of
  `Unknown`. Rebuilt a local Docker image and reran the expanded firmware corpus:
  2,998 observations validated against the updated schema, `metadata.Unknown`
  dropped from 1,927 to 0, and the prior extracted `mtab` parse error became
  symlink fallback metadata with `identify_error` rather than `metadata.error`.

## [2026-06-30] refactor | Core generic metadata module

Pages created: none
Pages updated: wiki/components/observe.md,
  wiki/work/firmware-corpus/verification.md
Files updated in ../pEyeON: src/eyeon/generic_metadata.py,
  src/eyeon/observe.py
Contradictions flagged: none
Notes: Moved EyeON-owned generic metadata classification out of `Observe` and
  into a dedicated `src/eyeon/generic_metadata.py` module. `Observe` now delegates
  fallback classification through `_generic_metadata`. Targeted generic metadata
  tests, schema JSON syntax check, and Python compile checks passed after the
  refactor.

## [2026-06-30] fix | Generic metadata fallback log level

Pages created: none
Pages updated: wiki/work/firmware-corpus/verification.md
Files updated in ../pEyeON: src/eyeon/observe.py
Contradictions flagged: none
Notes: Downgraded handled filetype-identification fallback logging from ERROR to
  WARNING. This keeps expected recoverable symlink-like files such as extracted
  `mtab` from appearing as ERROR output when the updated generic metadata fallback
  is active. Targeted generic metadata tests and Python compile checks passed.

## [2026-06-30] fix | Collision-safe observation JSON names

## [2026-07-08] planning | OVF/VM appliance build (Nutanix-first)

Pages created: wiki/work/ovf-vm-image-build/brief.md,
  wiki/work/ovf-vm-image-build/design.md,
  wiki/work/ovf-vm-image-build/implementation_plan.md,
  wiki/work/ovf-vm-image-build/references.md,
  wiki/work/ovf-vm-image-build/verification.md
Pages updated: wiki/index.md
Contradictions flagged: none
Notes: Grounded the OVF/VM build plan in the actual pEyeON container build files
  under `../pEyeON/builds/`. Recorded decisions: appliance VM, no container-like
  bind-mount workflow, Nutanix (qcow2) first, and a strong preference to refactor
  Dockerfile install logic into shared scripts that are reused for VM provisioning.

## [2026-07-08] implementation | Debian provision scripts + VM scaffold

Pages created: none
Pages updated: wiki/work/ovf-vm-image-build/implementation_plan.md,
  wiki/work/ovf-vm-image-build/verification.md, wiki/index.md
Files updated in ../pEyeON: builds/Dockerfile, builds/podman.Dockerfile,
  builds/provision/*.sh, builds/vm/*, README.md
Contradictions flagged: none
Notes: Implemented Debian 12 alignment by pinning container base to
  `python:3.13-slim-bookworm` and refactoring Dockerfiles to call shared
  provision scripts for build deps, runtime deps, Binwalk, CMake, TLSH,
  sasquatch, EyeON venv install, and Surfactant DB warm. Added an initial
  Debian 12 qcow2 appliance VM build scaffold (Packer + cloud-init + wrapper),
  and documented the VM build in the pEyeON README. Verified the refactored
  container build locally with `eyeon`, `binwalk`, `sasquatch`, and TLSH checks.

Pages created: none
Pages updated: wiki/work/firmware-corpus/verification.md
Files updated in ../pEyeON: src/eyeon/observe.py, src/eyeon/parse.py,
  tests/testObserve.py
Contradictions flagged: none
Notes: Made core EyeON JSON output collision-safe. The default output filename
  remains `<filename>.<md5>.json`, but if that path already exists for a different
  observation UUID, EyeON writes `<filename>.<md5>.<uuid>.json`. This preserves
  both observations when a container and extracted child share the same basename
  and hash, while same-name/different-hash observations continue to use the
  existing filename shape. Added targeted tests for both cases; generic metadata
  tests and Python compile checks passed.

## [2026-06-30] implementation | Streamlit observation hierarchy page

Pages created: none
Pages updated: wiki/components/streamlit_app.md,
  wiki/work/firmware-corpus/verification.md
Files updated: pages/ObservationHierarchy.py, pages/pages.py
Contradictions flagged: none
Notes: Added a Streamlit page for browsing `silver.raw_obs` parent/child
  relationships. The page introduces a semantic summary layer grouped by metadata
  type plus fallback `kind`, which is more useful than raw MIME type for large
  firmware/container trees. Users can pick a root observation, choose max depth,
  inspect summary groups, drill down into a selected group, and view a limited
  hierarchy preview. Python compile and Ruff checks passed.

## [2026-06-30] research | Base schema derivation and schema_blame recovery

Pages created: wiki/pipeline/base_schema_derivation.md
Pages updated: wiki/index.md, wiki/pipeline/dlt_load.md,
  wiki/schemas/silver_layer.md
Contradictions flagged: none
Notes: Recovered and documented the prior DLT base-schema spike. Grounded the
  workflow in `schemas/schema.sql`, `load_eyeon.py`, `utils/schema_blame.py`,
  `extras/Schema_Blame.md`, local DuckDB history, and a local OpenCode session
  diff containing the deleted TODO for minimum-row schema reconstruction. Captured
  source-corpus clues including pEyeON test files, early result/partitioned JSON
  paths, Schneider firmware EyeON output, DLT schema paths, and possible file
  inventory sources. No original corpus files were found; recovery remains
  best-effort.

## [2026-06-30] research-update | Minimal-row schema prototype found

Pages created: none
Pages updated: wiki/pipeline/base_schema_derivation.md
Contradictions flagged: none
Notes: Found `extras/MinamalRows.ipynb`, which contains the remembered prototype
  for selecting minimal representative JSON files from maximal schema coverage.
  The notebook reads `summarize` output, selects columns by `null_percentage`,
  generates per-row bit masks, chooses `example_uuid` values, joins back to
  `silver.raw_obs.source_path` and `source_file`, and copies the selected source
  JSON files into `min_files_max_schema`. DuckDB history contains the manual PE
  precursor queries for sparse columns, boolean signatures, bit masks, and
  example UUID selection.

## [2026-07-09] docs | VM appliance + loader bootstrap notes

Pages updated: wiki/work/ovf-vm-image-build/brief.md,
  wiki/work/ovf-vm-image-build/design.md,
  wiki/work/ovf-vm-image-build/references.md,
  wiki/work/ovf-vm-image-build/implementation_plan.md,
  wiki/work/ovf-vm-image-build/verification.md,
  wiki/components/container.md,
  wiki/components/load_eyeon.md,
  wiki/pipeline/dlt_load.md,
  wiki/index.md
Contradictions flagged: none
Notes: Updated the qcow2 appliance VM work packet to reflect the current Debian 12 + Packer build implementation (including analytics provisioning, networking, and Apple Silicon amd64 emulation notes). Documented dlt first-run schema bootstrap behavior for `load_eyeon.py` and noted that DuckDB CLI is installed via shared provision scripts for containers/VM.

## [2026-07-09] docs | Alpha VM decisions

Pages updated: wiki/work/ovf-vm-image-build/brief.md,
  wiki/work/ovf-vm-image-build/design.md,
  wiki/components/container.md
Contradictions flagged: none
Notes: Recorded alpha/debug decisions: password login is acceptable, keep the default `debian` user, ship a single VM flavor that includes pEyeON-Analytics. Clarified DuckDB CLI policy: default to DuckDB "latest" asset (optionally pin via `DUCKDB_CLI_VERSION`).

## [2026-07-10] docs | Build Doc Consolidation

Pages updated: wiki/components/container.md,
  wiki/work/ovf-vm-image-build/verification.md
Contradictions flagged: none
Notes: Added pointers to the consolidated build documentation (`../pEyeON/BUILD.md`) and recorded additional libvirt/RHEL host troubleshooting notes (virsh console detach, SSH key override, DHCP recovery, libguestfs package naming).

## [2026-07-10] docs | Builder Glossary

Pages created: wiki/concepts/build_glossary.md
Pages updated: wiki/index.md
Contradictions flagged: none
Notes: Added a builder-focused glossary for container + qcow2 appliance build technologies, project-specific terms, and pin guidance for known problem areas.

## [2026-07-17] docs | VM size reduction future task

Pages created: wiki/work/vm-image-size-reduction/brief.md
Pages updated: wiki/index.md
Contradictions flagged: none
Notes: Added a future-work brief for reducing qcow2 appliance size. Corrected the initial intuition that X11 or a desktop stack is the likely size driver; the current Debian appliance is already cloud-image/headless, so likely reduction targets are retained build toolchains, Rust/cargo state, build trees, Python caches, and the optional analytics payload.

## [2026-07-17] docs | UTM qcow2 boot note

Pages updated: wiki/work/ovf-vm-image-build/verification.md
Contradictions flagged: none
Notes: Recorded a UTM 4.7.x caveat for macOS testing: some builds only expose `Import Existing Drive` under the `Emulate` flow, and `bootindex=0 in use` indicates UTM created multiple boot devices instead of using only the imported qcow2.

## [2026-07-17] docs | External VM deployment guide

Pages updated: wiki/components/container.md,
  wiki/work/ovf-vm-image-build/brief.md,
  wiki/work/ovf-vm-image-build/verification.md,
  wiki/index.md
Contradictions flagged: none
Notes: Added references to `../pEyeON/builds/README-Deploy.md` so external deployment/import instructions for Nutanix, libvirt, UTM, Hyper-V, VMware, and VirtualBox are discoverable from both user-facing docs and the VM work packet. Also refreshed the container component's CI workflow grounding to match the current `ci.yaml` and `publish-container.yaml` files.

## [2026-08-03] fix | Core parse multiprocessing hang

Pages created: wiki/work/parse-multiprocessing-hang/implementation_plan.md,
  wiki/work/parse-multiprocessing-hang/verification.md
Pages updated: wiki/components/parse.md, wiki/index.md
Files updated in ../pEyeON: src/eyeon/parse.py, tests/testParse.py,
  eyeon-parse.sh, builds/Dockerfile, builds/podman.Dockerfile,
  builds/provision/warm-surfactant-dbs.sh
Files updated in this repo: eyeon-parse.sh
Contradictions flagged: none
Notes: Diagnosed a parse hang where direct `eyeon observe` and serialized runs
  over large Mach-O Homebrew binaries completed, but multiprocessing stalled at
  4/5. Updated core parse multiprocessing to use a spawn context by default,
  recycle workers after one file, and sleep in the monitor loop when no workers
  are active. Patched both wrapper copies to allocate an interactive TTY for
  terminal runs, pass unbuffered Python/TERM settings, and run parse at WARNING
  log level by default so progress bars and monitor warnings appear without
  DEBUG-level plugin noise. Hardened container Surfactant database warmup by using
  image-global XDG data/config paths, caching `database_sources.toml` locally,
  and making warmed DB state readable by runtime users/workers. Targeted parse
  tests and a local five-file multiprocessing smoke test over `coder`, `helm`,
  `k9s`, `parqeye`, and `vcluster` passed. After rebuilt-image testing still
  stalled at 102/104 on `coder` and `k9s`, added a large-file split so files at
  or above `EYEON_SERIAL_LARGE_FILE_BYTES` (default 50 MiB) run serially after
  parallel small-file processing. A local five-file large-file smoke test routed
  all five problematic binaries through the serial path and completed 5/5.
  Container confirmation requires rebuilding `peyeon:latest` from the modified
  core repo.

Follow-up: Suppressed residual parse log noise from spawned workers by exporting
  `LOGURU_LEVEL` from the CLI logger configuration and wrapper container
  environment, and by explicitly configuring worker Loguru sinks from that value.
  Added a targeted `PYTHONWARNINGS`/warnings filter for Surfactant's known
  `FutureWarning: Possible nested set at position 81`, which comes from regex
  handling while loading native library pattern databases. Downgraded EyeON's
  plugin filtered-argument trace and expected generic fallback "no type" messages
  to DEBUG. Verified CLI, parse, and generic metadata targeted tests, wrapper
  syntax, and wrapper copy identity.

## [2026-08-04] planning | Parse terminal output future work

Pages created: wiki/work/parse-terminal-output/brief.md
Pages updated: wiki/work/parse-multiprocessing-hang/implementation_plan.md,
  wiki/index.md
Contradictions flagged: none
Notes: Captured future work for replacing fragile multiprocess terminal output
  with parent-owned worker events and potentially Rich-based progress/log
  rendering. Also documented fragile mitigations from the current fix: exact-text
  Surfactant warning suppression, threshold-based large-file serialization, and
  `LOGURU_LEVEL` coordination rather than a full centralized logging design.
