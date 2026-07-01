---
title: "Verification: Firmware Corpus"
type: concept
confidence: medium
grounded_by:
  - raw/binwalk/binwalk.md
  - data/firmware_corpus/manifest.json
  - utils/firmware_corpus.py
  - tests/test_firmware_corpus.py
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [feature-work, verification, firmware, corpus]
---

# Verification: Firmware Corpus

## Test Commands

- `/opt/homebrew/bin/uv run python -m unittest tests.test_firmware_corpus`
- `/opt/homebrew/bin/uv run python -m utils.firmware_corpus list --subset binwalk-smoke`
- `/opt/homebrew/bin/uv run ruff check utils/firmware_corpus.py tests/test_firmware_corpus.py`
- `/opt/homebrew/bin/uv run python -m unittest tests.test_firmware_corpus tests.test_binwalk_cli`
- `/opt/homebrew/bin/uv run ruff check utils/firmware_corpus.py tests/test_firmware_corpus.py utils/binwalk_cli.py tests/test_binwalk_cli.py`
- `/opt/homebrew/bin/uv run python -m utils.firmware_corpus list --subset demo`
- `/opt/homebrew/bin/uv run python -m utils.firmware_corpus fetch --subset demo --dest /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-demo-corpus-artifacts`
- `/opt/homebrew/bin/uv run python -m utils.firmware_corpus verify dvrf-praetorian-repo --dest /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-demo-corpus-artifacts`
- `/opt/homebrew/bin/uv run python -m unittest tests.test_firmware_corpus`
- `/opt/homebrew/bin/uv run ruff check utils/firmware_corpus.py tests/test_firmware_corpus.py`
- `/opt/homebrew/bin/uv run python -m utils.firmware_corpus list`
- `/opt/homebrew/bin/uv run python -m utils.firmware_corpus fetch --entry openwrt-ath79-carambola2-23-05-5 --entry dvrf-praetorian-repo --entry dlink-dir-605l-revb-legacy --entry firmsec-dataset-index --entry tasmota-v15-5-0-selected --entry esphome-26-5-1-selected --entry iotgoat-v1-0-release --entry openipc-nightly-nor-lite-selected --dest /Users/johnson30/data/eyeon-corpus-downloads-expanded --timeout 180`
- `EYEON_CONTAINER_RUNTIME=docker ./eyeon-parse.sh --util-cd CORPUSEXP --dir /Users/johnson30/data/eyeon-corpus-downloads-expanded --dataset-path /Users/johnson30/data/eyeon-corpus-json-expanded --threads 6 --image peyeon-binwalk-test:latest`
- In `../pEyeON`: `/opt/homebrew/bin/uv run python -m unittest tests.testObserve.GenericMetadataTestCase`
- In `../pEyeON`: `/opt/homebrew/bin/uv run python -m unittest tests.testParse`
- In `../pEyeON`: `docker build -f builds/Dockerfile -t peyeon-generic-metadata-test:latest .`
- `EYEON_CONTAINER_RUNTIME=docker ./eyeon-parse.sh --util-cd CORPUSGEN --dir /Users/johnson30/data/eyeon-corpus-downloads-expanded --dataset-path /Users/johnson30/data/eyeon-corpus-json-generic --threads 6 --image peyeon-generic-metadata-test:latest`
- `/opt/homebrew/bin/uv run python <schema-validation-snippet>` over `/Users/johnson30/data/eyeon-corpus-json-generic/20260630T145633Z_CORPUSGEN`
- In `../pEyeON`: `/opt/homebrew/bin/uv run python -m py_compile src/eyeon/observe.py src/eyeon/generic_metadata.py tests/testObserve.py`
- In `../pEyeON`: `/opt/homebrew/bin/uv run python -m py_compile src/eyeon/observe.py src/eyeon/parse.py src/eyeon/generic_metadata.py tests/testObserve.py`
- `/opt/homebrew/bin/uv run python -m py_compile pages/ObservationHierarchy.py pages/pages.py`
- `/opt/homebrew/bin/uv run ruff check pages/ObservationHierarchy.py pages/pages.py`

## Manual Checks

- Confirmed OpenWrt 23.05.5 ath79/generic `sha256sums` was reachable through the development fetch tool and recorded the checksum for `openwrt-23.05.5-ath79-generic-8dev_carambola2-squashfs-sysupgrade.bin`.
- Confirmed GitHub API metadata for `praetorian-inc/DVRF` was reachable; repository metadata reported no license and an archived repository, so it remains a non-default repository pointer.
- Confirmed the GitHub contents API for `praetorian-inc/DVRF/Firmware` lists `DVRF_v03.bin` and `FW_LICENSE_E1550_v1.0.03.002.html`.
- Attempted to reach the D-Link DIR-605L legacy firmware directory through the development fetch tool; the request failed with a transport error, so this remains a manual vendor-page candidate.
- Did not download firmware, run Binwalk, or run EyeON parse in this slice.

## Results

- Unit tests passed: 7 tests in `tests.test_firmware_corpus`.
- CLI smoke check listed the `binwalk-smoke` subset and selected the OpenWrt Carambola2 direct-download entry.
- Ruff check passed for the new utility and tests.
- `python` and `uv` were not on PATH in the shell used by the agent; `/opt/homebrew/bin/uv` was present and used for verification.
- Updated unit tests passed: 15 tests across `tests.test_firmware_corpus` and `tests.test_binwalk_cli`.
- Demo listing now shows OpenWrt as `DOWNLOAD yes` with `ARTIFACTS 1/1`, DVRF as `DOWNLOAD yes` with `ARTIFACTS 1/2`, and D-Link as `DOWNLOAD no` with `ARTIFACTS 0/0`.
- Demo fetch downloaded two artifacts: OpenWrt and DVRF. It logged D-Link as skipped because it has no downloadable artifacts.
- DVRF artifact checksum recorded and verified: `1a3442c85f589f85b922a2b7b22c46d2e3844a3f729cc4ed432138c0f7abc046`.
- Expanded manifest now has 8 entries and 82 downloadable artifacts: OpenWrt (1), DVRF (1), Tasmota selected binaries (20), ESPHome selected factory/OTA binaries (25), IoTGoat v1.0 release assets (5), and OpenIPC selected NOR-lite bundles (30). D-Link and FirmSec remain non-downloadable/manual entries.
- Expanded fetch downloaded and checksum-verified all 82 downloadable artifacts into `/Users/johnson30/data/eyeon-corpus-downloads-expanded`; cache size was about 320 MB.
- Expanded parse output directory: `/Users/johnson30/data/eyeon-corpus-json-expanded/20260630T052134Z_CORPUSEXP`.
- Expanded parse produced 2,998 JSON observations and no certificate files.
- Expanded parse summary: 80 top-level JSON observations, 2,918 child observations with `parent`, 115 observations with `metadata.binwalk_file`, 34 observations with `metadata.container_file`, and 1 observation with `metadata.error`.
- Top filetype counts from expanded parse: `ELF` 1005, `SHELL` 533, `HTML` 87, `JAVASCRIPT` 86, `GZIP` 34, `UIMAGE` 31, `CSS` 4, `SHEBANG` 4.
- Metadata counts from expanded parse: `Unknown` 1927, `elf_file` 1005, `binwalk_file` 115, `container_file` 34, `uimage_file` 31, `error` 1.
- `Unknown` metadata profile from expanded parse: 1,881 child observations and 46 top-level observations. Top extensions were `<none>` 372, `.control` 266, `.prerm` 266, `.list` 245, `.bin` 111, `.lua` 92, `.js` 86, `.htm` 85, `.sh` 71, `.md5sum` 60, `.png` 48, `.uc` 26, `.json` 24, `.conf` 21, `.gif` 20, `.conffiles` 20, `.dtb` 11, `.svg` 10. Top magic strings were `ASCII text` 952, `POSIX shell script` 436, `HTML document` 47, `data` 41, `PNG image data` 41, `JSON text data` 24, `JavaScript source` 22, `empty` 19, `GIF image data` 18, and Linux ARM `zImage` variants 26.
- Highest-impact `Unknown` reduction target appears to be package/text metadata from extracted OpenWrt/IoTGoat/OpenIPC files. OpenWrt package-manager files alone account for hundreds of Unknowns (`*.control`, `*.list`, `*.prerm`, `*.conffiles`).
- Two top-level source artifacts appear to collapse in the output because the `.gz` forms of `tasmota-4M.bin.gz` and `tasmota.bin.gz` extract to child files with the same filename and hash as the corresponding top-level `.bin` artifacts. The current observation filename scheme is `<filename>.<md5>.json`, so one observation can overwrite/collapse the other in this paired case.
- Core generic metadata fallback tests passed in `../pEyeON`: `tests.testObserve.GenericMetadataTestCase`.
- Core parse tests passed in `../pEyeON`: `tests.testParse`.
- Built `peyeon-generic-metadata-test:latest` from modified `../pEyeON` with EyeON-owned generic metadata fallback logic.
- Re-ran the expanded corpus through that image. Output directory: `/Users/johnson30/data/eyeon-corpus-json-generic/20260630T145633Z_CORPUSGEN`.
- Generic-metadata parse produced 2,998 JSON observations, 80 top-level observations, 2,918 child observations, `metadata.Unknown` count `0`, and `metadata.error` count `0`.
- Generic-metadata metadata counts: `elf_file` 1005, `opkg_file` 798, `text_file` 613, `web_asset` 291, `generic_file` 162, `binwalk_file` 115, `symlink_file` 61, `container_file` 34, `uimage_file` 31, `linux_kernel_image` 26, `device_tree_file` 11.
- All 2,998 JSON observations from `/Users/johnson30/data/eyeon-corpus-json-generic/20260630T145633Z_CORPUSGEN` validated against the updated `../pEyeON/schema/observation.schema.json`.
- Refactored generic metadata classification out of `src/eyeon/observe.py` into `src/eyeon/generic_metadata.py`. Re-ran `tests.testObserve.GenericMetadataTestCase`, schema JSON syntax check, and Python compile checks after the refactor; behavior was not changed, so the expanded corpus parse was not rerun.
- Added collision-safe JSON output naming in core EyeON. The normal output name remains `<filename>.<md5>.json`; if that file already exists for a different observation UUID, the writer uses `<filename>.<md5>.<uuid>.json`. This preserves both observations when a container and extracted child share the same basename and hash. Same-name/different-hash files already produce distinct `<filename>.<md5>.json` paths.
- Added tests for same-name/same-hash and same-name/different-hash observations. `tests.testObserve.GenericMetadataTestCase` passed with 6 tests after the output naming change.
- Added `pages/ObservationHierarchy.py` to display `silver.raw_obs` parent/child relationships with a semantic summary layer. The page groups descendants by metadata type plus fallback `kind`, then offers drilldown rows and a limited hierarchy preview so large containers remain navigable.
- Streamlit page syntax and Ruff checks passed for `pages/ObservationHierarchy.py` and `pages/pages.py`.

## Known Gaps

- Network-dependent firmware download tests are intentionally not part of the default test suite.
- Source terms and redistribution permissions still need human/legal review before any firmware images are redistributed or committed.
- DVRF includes a firmware-local license HTML file, but redistribution terms still need human/legal review before using DVRF as a distributed fixture.
- D-Link legacy URL reachability remains unresolved from this environment.
- Binwalk signatures for the selected OpenWrt fixture are expected but not yet verified by an actual Binwalk run.
- EyeON parse integration is deferred.
- The previous `[Errno 22] Invalid argument` metadata error for extracted `mtab` symlink-like files is handled by the generic fallback when using the updated core image. `Observe` records `symlink_file.identify_error`; parse no longer emits `metadata.error` for this case.
- The previous output filename collision/collapse for `.bin` and `.bin.gz` pairs is addressed in core EyeON by appending the UUID only when `<filename>.<md5>.json` already exists for another observation.
- Analytics/dbt models do not yet consume the new generic metadata keys (`opkg_file`, `text_file`, `web_asset`, `generic_file`, `symlink_file`, `linux_kernel_image`, `device_tree_file`, `image_file`).

## Follow-Ups

- Add an opt-in integration test that fetches the OpenWrt fixture, verifies its checksum, runs Binwalk, and records actual signatures.
- Add parse workflow integration after the corpus utility stabilizes.
- Re-check D-Link and other vendor URLs from a normal browser or network environment before using them in demos.
- Add more direct-download vendor/utility entries to the manifest once source URLs and terms are manually verified.
- Add analytics loader/dbt support for the new generic metadata keys after core behavior is accepted.
