---
title: "Implementation Plan: Firmware Corpus"
type: concept
confidence: medium
grounded_by:
  - raw/binwalk/binwalk.md
  - data/firmware_corpus/manifest.json
  - utils/firmware_corpus.py
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [feature-work, implementation, firmware, corpus]
---

# Implementation Plan: Firmware Corpus

## Scope

Build a small, manifest-driven firmware corpus workflow that can support Binwalk
development before full Binwalk integration is implemented.

For the code-development handoff, see [[wiki/work/firmware-corpus/dev_handoff]].

## Steps

1. Choose manifest location and format.
2. Populate initial manifest with a small set of candidate entries from `raw/binwalk/binwalk.md`.
3. Verify source URLs, redistribution terms, and checksums where possible.
4. Select the first Binwalk spike fixture.
5. Design the fetch utility interface and cache layout.
6. Implement listing and fetching for direct-download entries.
7. Add checksum verification.
8. Add parse workflow integration with EyeON.
9. Define unit, smoke, demo, and research subsets.
10. Update Binwalk Support design based on corpus spike results.

## Files Likely To Change

- `data/firmware_corpus/manifest.json` - source-controlled corpus manifest.
- `utils/firmware_corpus.py` - listing, direct-download fetching, and checksum utility.
- `tests/test_firmware_corpus.py` - non-network unit tests for manifest/list/fetch/checksum behavior.
- `.gitignore` - local firmware corpus cache paths.
- `wiki/work/binwalk-support/*` after fixture selection informs Binwalk spike work.

## Tests To Add Or Update

- Manifest schema/structure validation.
- Fetch utility dry-run/list behavior.
- Direct-download fetch of a small approved fixture.
- Checksum verification success and failure paths.
- Opt-in integration test for EyeON parse over fetched sample.

## Migration Or Compatibility Notes

- Do not assume network access in default test runs.
- Do not commit large firmware binaries unless explicitly approved.
- Keep local corpus cache paths configurable.

## Rollback Plan

- Remove or ignore the corpus manifest and utility without affecting core EyeON parsing.
- Keep wiki notes as historical context if implementation is abandoned.

## Done Checklist

- [x] Dev agent has read [[wiki/work/firmware-corpus/dev_handoff]].
- [x] Manifest location and format selected.
- [x] Initial candidate entries verified.
- [x] First Binwalk spike fixture selected.
- [x] Fetch/list utility implemented or explicitly deferred.
- [x] Test subset policy documented.
- [x] Binwalk Support feature docs updated with selected fixture.

## Implementation Notes

- Manifest format: JSON, versioned at `version: 1`, with entries under `entries`.
- Manifest location: `data/firmware_corpus/manifest.json`.
- Utility: `utils/firmware_corpus.py`, runnable with `uv run python -m utils.firmware_corpus ...`.
- Local cache defaults to `.firmware-corpus/`; `.firmware-corpus/`, `firmware-corpus/`, and `data/firmware-corpus/` are ignored by git.
- Default tests are non-network only. Direct-download behavior is covered with local `file://` fixtures and checksum success/failure paths.
- Initial candidate verification is partial: OpenWrt checksum metadata and DVRF GitHub repository metadata were reachable on 2026-06-26; D-Link legacy directory access failed through the development fetch tool and remains manual; FirmSecDataset remains a dataset-index pointer.
- Manifest entries now support per-entry `artifacts` for specific files inside a source, including multiple firmware binaries in one git repository. DVRF uses this to reference `Firmware/DVRF_v03.bin` as a fetchable firmware artifact and `Firmware/FW_LICENSE_E1550_v1.0.03.002.html` as a non-fetched license document.
- Listing includes `DOWNLOAD` and `ARTIFACTS` columns. Fetching logs per-entry progress, artifact cache/fetch actions, checksums, skipped entries, and a final fetched count.
- Expanded manifest entries added Tasmota v15.5.0 selected binaries, ESPHome 26.5.1 selected factory/OTA binaries, OWASP IoTGoat v1.0 release assets, and a bounded OpenIPC nightly NOR-lite subset. This increased downloadable artifacts to 82.
- Expanded fetch and parse completed on 2026-06-30. JSON output was written to `/Users/johnson30/data/eyeon-corpus-json-expanded/20260630T052134Z_CORPUSEXP`.
