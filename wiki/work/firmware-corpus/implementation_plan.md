---
title: "Implementation Plan: Firmware Corpus"
type: concept
confidence: medium
grounded_by:
  - raw/binwalk/binwalk.md
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

- TBD: manifest path.
- TBD: utility/script path.
- TBD: tests for manifest validation and fetch behavior.
- TBD: `.gitignore` or docs for local corpus caches.
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

- [ ] Dev agent has read [[wiki/work/firmware-corpus/dev_handoff]].
- [ ] Manifest location and format selected.
- [ ] Initial candidate entries verified.
- [ ] First Binwalk spike fixture selected.
- [ ] Fetch/list utility implemented or explicitly deferred.
- [ ] Test subset policy documented.
- [ ] Binwalk Support feature docs updated with selected fixture.
