---
title: "Feature Design: Binwalk Support"
type: concept
confidence: medium
grounded_by:
  - ../pEyeON/src/eyeon/container.py
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/schema/observation.schema.json
  - wiki/work/binwalk-support/spike.md
policy: agent-editable
last_validated: 2026-06-26
repo_scope: cross-repo
implementation_area: scanner
format_domain: firmware
audience: mixed
status: draft
source_paths: wiki/work/binwalk-support/design.md
tags: [feature-work, design, binwalk, firmware]
---

# Feature Design: Binwalk Support

## Summary

Draft direction: Binwalk support should detect and extract firmware/container
structures during the normal EyeON parsing flow, then feed extracted children
back through existing EyeON parsing. The original container file should still
receive its own EyeON JSON observation.

## Implemented Approach

Core `pEyeON` uses direct Binwalk v3 CLI invocation during `eyeon parse`. The
Python API wrapper is intentionally not used.

- `Parse` invokes Binwalk through `src/eyeon/container.py` for firmware-like inputs.
- Default trigger set: `UIMAGE` filetype or file extensions `.bin`, `.chk`, `.firmware`, `.fw`, `.img`, `.trx`.
- `EYEON_BINWALK=0` disables Binwalk.
- `EYEON_BINWALK=1` forces Binwalk for parsed files.
- `EYEON_BINWALK_PATH` can point to a specific Binwalk executable.
- Binwalk runs with `-q -e -C <temp-output> -l <json-log> <file>`.
- Extracted files are recursively observed like other container children.
- Child observations set `parent` to the original firmware/container observation UUID.

## Data Model Or Schema Changes

Original firmware/container observations can now include `metadata.binwalk_file`.
The schema records availability, scan/extraction status, findings, extraction
records, extraction failures, stderr/stdout, return code, command, and errors.

Child lineage uses the now-defined observation `parent` field as the parent
observation UUID.

## Interfaces And User Experience

Binwalk is automatic for firmware-like inputs during `eyeon parse`, subject to
the environment controls above. If Binwalk is unavailable, parsing still emits
the original observation with `metadata.binwalk_file.available: false`.

## Edge Cases

- Large firmware images.
- Recursive extraction depth and cycles.
- Extraction failures and partial results.
- Filesystems, compressed streams, and embedded archives found inside firmware.
- Duplicate extracted files.
- Parent/child relationship preservation.

## Error Handling

- Missing Binwalk does not fail the parse; it records unavailable Binwalk metadata.
- Non-zero Binwalk status is captured in metadata and stderr.
- Partial extraction is represented through `extraction_status` and `extraction_failures`.
- Extracted children are recursively observed only when Binwalk produced files.

## Risks

- Binwalk dependency availability may vary by platform and container runtime.
- Recursive extraction can be expensive and may produce large output trees.
- Extracted child observations may require schema or analytics changes.
- Running Binwalk automatically for extension-based firmware guesses can add runtime cost.
- Parent/child modeling now has a first concrete shape, but analytics still needs to consume it.

## Alternatives Considered

- Metadata-only Binwalk signatures without extraction.
- Full extraction with recursive EyeON observation of child files.
- Separate Binwalk command outside normal `observe`/`parse`.
- Analytics-only ingestion of precomputed Binwalk results.
- Use the `binwalk3` Python API as a compatibility layer over Binwalk v3. Rejected for core integration because CLI JSON exposes extraction success/failure more directly and avoids depending on a small compatibility wrapper.

## Open Questions

- See [[wiki/work/binwalk-support/brief]] for the current requirements-level open questions.
