---
title: "Verification: Binwalk Support"
type: concept
confidence: medium
grounded_by:
  - utils/binwalk_cli.py
  - tests/test_binwalk_cli.py
  - data/firmware_corpus/manifest.json
  - ../pEyeON/src/eyeon/container.py
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/schema/observation.schema.json
policy: agent-editable
last_validated: 2026-06-26
repo_scope: cross-repo
implementation_area: scanner
format_domain: firmware
audience: mixed
status: draft
source_paths: wiki/work/binwalk-support/verification.md
tags: [feature-work, verification, binwalk, firmware]
---

# Verification: Binwalk Support

## Test Commands

- `/opt/homebrew/bin/uv run python -m unittest tests.test_binwalk_cli tests.test_firmware_corpus`
- `/opt/homebrew/bin/uv run ruff check utils/binwalk_cli.py tests/test_binwalk_cli.py utils/firmware_corpus.py tests/test_firmware_corpus.py`
- `/opt/homebrew/bin/uv run python -m utils.binwalk_cli /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-binwalk-corpus/openwrt-ath79-carambola2-23-05-5/openwrt-23.05.5-ath79-generic-8dev_carambola2-squashfs-sysupgrade.bin`
- `/opt/homebrew/bin/uv run python -m utils.binwalk_cli --extract --output-dir /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/openwrt-wrapper-extract-20260626 /var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode/eyeon-binwalk-corpus/openwrt-ath79-carambola2-23-05-5/openwrt-23.05.5-ath79-generic-8dev_carambola2-squashfs-sysupgrade.bin`
- In `../pEyeON`: `/opt/homebrew/bin/uv run python -m unittest tests.testParse.TestContainerParse tests.testObserve.TestJSONSchema`
- In `../pEyeON`: `/opt/homebrew/bin/uv run python -m py_compile src/eyeon/container.py src/eyeon/parse.py src/eyeon/observe.py tests/testParse.py tests/testObserve.py`
- In `../pEyeON`: `/opt/homebrew/bin/uv run python -m json.tool schema/observation.schema.json`
- In `../pEyeON`: `docker build -f builds/Dockerfile -t peyeon-binwalk-test .`
- In `../pEyeON`: `docker run --rm peyeon-binwalk-test sh -c "eyeon --help >/dev/null && binwalk --version && command -v sasquatch && command -v 7z && command -v unar"`
- In `../pEyeON`: `docker build -f builds/podman.Dockerfile -t peyeon-binwalk-podmanfile-test .`
- In `../pEyeON`: `docker run --rm peyeon-binwalk-podmanfile-test sh -c "eyeon --help >/dev/null && binwalk --version && command -v sasquatch && command -v 7z && command -v unar"`
- In `../pEyeON`: `docker run --rm -e EYEON_UID="$(id -u)" -e EYEON_GID="$(id -g)" -v "/Users/johnson30/Downloads/test:/source:ro" -v "/Users/johnson30/Downloads/eyeon-test-binwalk-results:/results" peyeon-binwalk-test eyeon parse /source -o /results --threads 1 -v INFO`

## Manual Checks

- Confirmed Homebrew Binwalk v3 was available on PATH before real smoke tests.
- Confirmed the OpenWrt corpus fixture had already been fetched to temp storage and verified by the firmware corpus utility.
- Confirmed extraction output and downloaded firmware remained under `/var/folders/gj/mckt0dsj0xv2vwr1jd1wv8nc0004ys/T/opencode`, not in the repository.

## Results

- Unit tests passed: 12 tests across `tests.test_binwalk_cli` and `tests.test_firmware_corpus`.
- Ruff passed for the new Binwalk wrapper, new tests, firmware corpus utility, and firmware corpus tests.
- Real wrapper detection smoke test found two findings: `uimage` at offset `0` and `squashfs` at offset `2359296`.
- Real wrapper extraction smoke test captured two extraction records: `uimage_built_in` succeeded and `squashfs` failed.
- The wrapper captured Binwalk `stderr` containing the missing `sasquatch` extractor errors while preserving `returncode: 0`.
- `extraction_failures` correctly included the failed `squashfs` extraction record.
- Core fake-Binwalk test passed: firmware-like `.bin` input received `metadata.binwalk_file`, produced an extracted child observation, and linked the child `parent` to the firmware observation UUID.
- Core schema test for `metadata.binwalk_file` passed.
- Core schema JSON parsed successfully.
- Docker image build with `builds/Dockerfile` succeeded locally and reported `binwalk 3.1.0`, `/usr/bin/sasquatch`, `/usr/bin/7z`, and `/usr/bin/unar` in the runtime image.
- The Podman Dockerfile path was validated by building `builds/podman.Dockerfile` with Docker because the local `podman.lima` VM is not configured. The built image reported `binwalk 3.1.0`, `/usr/bin/sasquatch`, `/usr/bin/7z`, and `/usr/bin/unar`.
- Real containerized parse against `/Users/johnson30/Downloads/test` completed and wrote results to `/Users/johnson30/Downloads/eyeon-test-binwalk-results`.
- That run produced 2,129 JSON observations, 2,124 child observations with `parent`, 9 observations with `metadata.binwalk_file`, and 5 observations with `metadata.container_file`.
- Input subdirectories were handled: observations include `DVRF_v03.bin` from `dvrf-praetorian-repo/` and the OpenWrt sysupgrade image from `openwrt-ath79-carambola2-23-05-5/`.
- Binwalk extraction succeeded for the OpenWrt and DVRF firmware samples, including SquashFS extraction through container-provided `sasquatch`.
- One extracted OpenWrt child observation (`mtab`) recorded an error fallback with `[Errno 22] Invalid argument`.
- Large ISO-derived `install.img` and `rootfs.img` child scans recorded `metadata.binwalk_file.returncode: -9` with `extraction_status: no_findings`; this needs follow-up if those child disk images are in scope.

## Known Gaps

- Analytics/dbt models do not yet consume `metadata.binwalk_file`.
- Local macOS SquashFS extraction can still be blocked without `sasquatch`, but the updated Docker and Podman image builds include it.
- Real Binwalk integration tests remain opt-in; default tests use a fake Binwalk executable.

## Follow-Ups

- Add opt-in integration tests for real Binwalk scans and extraction.
