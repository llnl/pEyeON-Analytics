---
title: "Feature Brief: Binwalk Support"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [feature-work, binwalk, firmware]
---

# Feature Brief: Binwalk Support

## Problem

EyeON targets software and firmware inventory, but firmware is often distributed
as nested containers, compressed streams, filesystems, and vendor-specific image
formats. EyeON needs Binwalk support so it can identify these container-like
firmware structures, extract their contents, and run normal EyeON parsing over
the extracted files.

## Goals

- Use Binwalk to detect firmware/container structures that EyeON cannot otherwise route or unpack well.
- Integrate Binwalk into the normal processing flow rather than requiring a completely separate analytics-only workflow.
- Extract files from known container structures and run those extracted files through the normal EyeON parser.
- Emit an EyeON JSON observation for the original container file itself.
- Treat extracted files as separate EyeON observations.
- Support recursive extraction in principle, because firmware can contain containers within containers.
- Define testable acceptance criteria using known firmware/container sample fixtures.
- Identify required dependencies, licensing considerations, installation method, and runtime constraints.
- Use the Firmware Corpus feature slice to choose the first Binwalk spike and test fixtures.

## Non-Goals

- Fully resolving parent/child lineage for recursive extraction in the first implementation. That relationship is important, but can be handled as a later or final task.
- Adding Streamlit reporting before the scanner output and loader behavior are stable.
- Treating every Binwalk signature match as a supported EyeON file format. Initial support should focus on extraction/routing value.

## User-Facing Behavior

- Intended behavior: Binwalk support participates in normal parsing for firmware/container-like inputs.
- Open: whether the first implementation is automatic, opt-in via a flag, or both.
- Open: exact JSON shape for Binwalk scan results on the original container observation.
- Open: exact output directory layout for extracted children.
- Open: analytics/database behavior after extracted observations are produced.
- Deferred: Streamlit/UI behavior.

## Acceptance Criteria

- Given a known firmware/container sample, EyeON records a JSON observation for the original input file.
- Given a known firmware/container sample with extractable embedded content, EyeON extracts child files using Binwalk and runs the normal EyeON parser on those extracted files.
- Extracted child files produce normal EyeON JSON observations with hashes, magic, filetype, and metadata where available.
- Recursive extraction behavior is bounded by a documented depth, size, or execution limit.
- Binwalk failures are recorded or reported without crashing the whole parse batch.
- The feature works in the containerized workflow or has a documented installation/runtime blocker.
- At least one deterministic fixture test asserts expected Binwalk findings or extracted child observations.

## Affected Areas

- `../pEyeON/` core scanner behavior may be affected if Binwalk extraction runs during observe or parse.
- `pEyeON-Analytics` loader, schemas, dbt models, or Streamlit pages may be affected if Binwalk output is persisted and reported.
- Wiki file-format, component, schema, and pipeline pages may need updates after implementation.

## References

- [[wiki/work/binwalk-support/references]]
- [[wiki/work/firmware-corpus/brief]]
- [[wiki/concepts/llm_assisted_feature_workflow]]
- [[wiki/concepts/feature_work_template]]

## Open Questions

- Should the first implementation be automatic during `eyeon parse`, opt-in with a flag, or automatic only for selected file types?
- What exact routing layer should invoke Binwalk: before `observe`, inside `parse`, inside a surfactant plugin, or as a dedicated pre/post-processing step?
- What JSON shape should represent Binwalk scan results on the container observation?
- What temporary and final directory layout should hold extracted files during parsing?
- What recursion, size, timeout, and safety limits are required for extraction?
- What minimal parent/child information, if any, must be emitted before the larger lineage design is resolved?
- How should this feature update or depend on [[wiki/tensions/parent_field]] and [[wiki/tensions/archive_recursion]]?
- Should the implementation target Binwalk v3 directly, the `binwalk3` Python compatibility package, or both?
- How should Binwalk v3 and/or `binwalk3` be installed in the published container image and local development environments?
- Which fixture from [[wiki/work/firmware-corpus/brief]] should be used as the first Binwalk spike sample?

## Test Plan

- Identify small, redistributable firmware/container fixtures with deterministic Binwalk output.
- Add a fixture that has at least one extractable embedded file.
- Assert the original container observation is emitted.
- Assert extracted child observations are emitted and parseable by the existing analytics loader.
- Add failure-path coverage for missing Binwalk, extraction failure, or timeout.

## Done When

- Requirements and acceptance criteria are complete.
- References and dependency constraints are documented.
- Design is accepted or open tensions are recorded.
- Implementation is complete and tested.
- Durable behavior is promoted into canonical wiki pages.
