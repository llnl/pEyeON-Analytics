---
title: "Feature Design: Binwalk Support"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [feature-work, design, binwalk, firmware]
---

# Feature Design: Binwalk Support

## Summary

Draft direction: Binwalk support should detect and extract firmware/container
structures during the normal EyeON parsing flow, then feed extracted children
back through existing EyeON parsing. The original container file should still
receive its own EyeON JSON observation.

## Proposed Approach

TBD after reference review and spike. Current candidate approaches:

- Invoke Binwalk from `parse` before or around normal `observe` processing for selected container-like inputs.
- Implement a surfactant plugin or routing hook that uses Binwalk detection to decide whether additional extraction is needed.
- Use the `binwalk3` Python API as a compatibility layer over Binwalk v3, if it behaves reliably in the EyeON runtime.
- Fall back to direct Binwalk v3 CLI invocation if the Python package is not suitable.

## Data Model Or Schema Changes

Known need: the original container observation likely needs a Binwalk metadata
section that records scan findings, extraction status, and any errors. Parent
and child lineage is intentionally deferred unless a minimal field is required
for the first implementation.

## Interfaces And User Experience

Open. The likely user-facing choice is between automatic behavior during
`eyeon parse`, an opt-in flag, or automatic behavior for selected file types.

## Edge Cases

- Large firmware images.
- Recursive extraction depth and cycles.
- Extraction failures and partial results.
- Filesystems, compressed streams, and embedded archives found inside firmware.
- Duplicate extracted files.
- Parent/child relationship preservation.

## Error Handling

TBD.

## Risks

- Binwalk dependency availability may vary by platform and container runtime.
- Recursive extraction can be expensive and may produce large output trees.
- Extracted child observations may require schema or analytics changes.
- Parent/child modeling may overlap with unresolved schema tensions.

## Alternatives Considered

- Metadata-only Binwalk signatures without extraction.
- Full extraction with recursive EyeON observation of child files.
- Separate Binwalk command outside normal `observe`/`parse`.
- Analytics-only ingestion of precomputed Binwalk results.

## Open Questions

- See [[wiki/work/binwalk-support/brief]] for the current requirements-level open questions.
