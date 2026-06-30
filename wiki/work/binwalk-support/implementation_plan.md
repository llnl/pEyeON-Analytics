---
title: "Implementation Plan: Binwalk Support"
type: concept
confidence: medium
grounded_by:
  - utils/binwalk_cli.py
  - tests/test_binwalk_cli.py
  - wiki/work/binwalk-support/spike.md
  - ../pEyeON/src/eyeon/container.py
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/schema/observation.schema.json
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [feature-work, implementation, binwalk, firmware]
---

# Implementation Plan: Binwalk Support

## Scope

Current implementation uses direct Binwalk v3 CLI invocation in core `pEyeON`
parse flow. The earlier analytics wrapper remains a reusable spike artifact.

## Steps

1. Research Binwalk integration options and record references.
2. Decide metadata-only vs extraction behavior.
3. Decide where the feature belongs in the EyeON workflow.
4. Define schema and analytics implications.
5. Build a spike if dependency or extraction behavior is uncertain.
6. Implement the chosen design.
7. Add or update tests.
8. Update canonical wiki pages and close feature-work artifacts.

## Files Likely To Change

- `utils/binwalk_cli.py` - direct Binwalk CLI JSON wrapper.
- `tests/test_binwalk_cli.py` - default non-network tests using a fake Binwalk executable.
- `../pEyeON/src/eyeon/container.py` - direct Binwalk invocation, JSON parsing, extraction child discovery.
- `../pEyeON/src/eyeon/parse.py` - Binwalk extraction integrated into parse recursion.
- `../pEyeON/schema/observation.schema.json` - `metadata.binwalk_file` shape.
- `../pEyeON/tests/testParse.py`, `../pEyeON/tests/testObserve.py` - fake-Binwalk and schema tests.
- TBD: analytics loader/schema/dbt/UI files, if Binwalk output is loaded or surfaced.
- TBD: container/build files, if Binwalk is packaged with the scanner image.

## Tests To Add Or Update

- Unit tests for parsing `Analysis.file_map` and `Analysis.extractions`.
- Unit tests that preserve non-zero Binwalk exit status when JSON is valid.
- Unit tests for missing JSON and missing target file failure paths.
- Optional real Binwalk smoke test against the OpenWrt corpus fixture.
- Core fake-Binwalk test for firmware-like `.bin` extraction and parent linkage.
- Core schema test for `metadata.binwalk_file`.

## Migration Or Compatibility Notes

- Binwalk remains optional; no Python package dependency is added in this slice.
- Core integration uses the Binwalk CLI, not `binwalk3`.
- Default tests do not require Binwalk, network access, or firmware downloads.
- The wrapper is designed around Binwalk v3 JSON logging via `-l`.
- Extraction failures may occur even when Binwalk exits `0`; callers must inspect `extraction_failures` and `stderr`.
- `EYEON_BINWALK=0` disables Binwalk, `EYEON_BINWALK=1` forces it, and `EYEON_BINWALK_PATH` selects the executable.

## Rollback Plan

- Remove the Binwalk branch from `../pEyeON/src/eyeon/parse.py`, remove `metadata.binwalk_file` from the schema, and keep or remove the analytics wrapper depending on whether it remains useful for manual experiments.

## Done Checklist

- [ ] Brief acceptance criteria finalized.
- [x] References documented.
- [x] Design accepted or tensions recorded.
- [x] Implementation completed for the CLI-wrapper spike slice.
- [x] Tests pass for the CLI-wrapper spike slice.
- [x] Implementation completed for the core Binwalk CLI parse slice.
- [x] Tests pass for the core Binwalk CLI parse slice.
- [ ] Canonical wiki pages updated.
- [x] `wiki/log.md` updated with closeout entry.
