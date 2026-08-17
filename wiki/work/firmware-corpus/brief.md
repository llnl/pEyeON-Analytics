---
title: "Feature Brief: Firmware Corpus"
type: concept
confidence: medium
grounded_by:
  - raw/binwalk/binwalk.md
policy: agent-editable
last_validated: 2026-06-26
repo_scope: cross-repo
implementation_area: scanner
format_domain: firmware
audience: mixed
status: draft
source_paths: wiki/work/firmware-corpus/brief.md
tags: [feature-work, firmware, corpus, binwalk, testing]
---

# Feature Brief: Firmware Corpus

## Problem

EyeON needs realistic firmware samples to test and demonstrate Binwalk support,
archive/container handling, and supply-chain analysis workflows. The project
currently has candidate firmware sources in notes, but not a maintained corpus
manifest, fetching workflow, or test subset strategy.

<!-- GROUND_TRUTH: raw/binwalk/binwalk.md §firmware-sample-corpus -->

## Goals

- Maintain a curated list of firmware sample pointers with source URLs, license/redistribution notes, expected structure, and intended use.
- Create a utility design for fetching selected corpus files into a local dataset directory without committing large firmware binaries to the repo.
- Define a workflow for parsing fetched corpus files with EyeON.
- Identify a small deterministic subset suitable for automated tests.
- Support Binwalk development by prioritizing samples with known nested firmware/container structures.
- Support demos and "show and tell" stories using known firmware samples and expected discoveries.

## Non-Goals

- Commit full firmware images to this repository by default.
- Build a comprehensive public firmware mirror.
- Perform exploit development or emulation as part of the corpus fetch/parse utility.
- Treat vendor vulnerability claims as verified until checked against primary sources.

## User-Facing Behavior

- Users should be able to list available corpus entries.
- Users should be able to fetch selected entries into a local directory.
- Users should be able to run EyeON parsing over the fetched entries.
- Users should be able to select a small test/demo subset rather than downloading a bulk corpus.

## Acceptance Criteria

- A corpus manifest format is defined and includes URL, name, category, expected file type, expected Binwalk-relevant structure, size/checksum when known, license/redistribution status, and intended use.
- At least three candidate entries are documented across clean baseline, vulnerable/demo, and real-world vendor categories.
- At least one candidate is selected for Binwalk extraction testing, subject to licensing and size constraints.
- The proposed fetch/parse utility behavior is documented with command examples.
- Test-fixture policy is documented: what can be committed, what must be downloaded, and how tests skip or fetch external data.

## Affected Areas

- [[wiki/work/binwalk-support/brief]] - Binwalk implementation needs fixture samples.
- `../pEyeON/` may eventually gain tests or helper scripts that consume the corpus.
- `pEyeON-Analytics` may eventually gain demo workflows over corpus-derived EyeON output.
- `raw/binwalk/binwalk.md` remains the current seed note for candidate corpus sources.

## References

- [[wiki/work/firmware-corpus/dev_handoff]]
- [[wiki/work/firmware-corpus/candidates]]
- [[wiki/work/firmware-corpus/references]]
- [[wiki/work/binwalk-support/brief]]
- `raw/binwalk/binwalk.md`

## Open Questions

- Where should the corpus manifest live: `wiki/`, `raw/`, source-controlled code/data directories, or a generated artifact?
- What is the first implementation target for the fetch/parse utility: shell script, Python CLI, Make target, or notebook?
- Should corpus entries be fetched by direct URL, by vendor page plus manual selection, or by dataset-specific adapters?
- Which firmware files are legally safe to use in committed unit tests?
- What size limit should apply to test fixtures committed to the repo?
- Should tests download external firmware on demand, or should external corpus tests be opt-in only?
- What checksum policy is required for fetched files?
- Which sample should be the first Binwalk spike fixture?
- Which candidates from [[wiki/work/firmware-corpus/candidates]] should seed the first manifest: `unit-small`, `binwalk-smoke`, `demo-vulnerable`, or `demo-baseline`?
- Which industrial/utility candidates should seed a `demo-utility` subset, and which must remain manual/gated entries?

## Test Plan

- Validate the corpus manifest schema or structure.
- Test that the fetch utility downloads one small allowed fixture and verifies checksum when available.
- Test that the parse workflow produces EyeON JSON for downloaded corpus files.
- Mark large/network-dependent corpus tests as opt-in unless a small redistributable fixture is identified.

## Done When

- Corpus manifest design is accepted.
- Initial candidate entries are documented with provenance and redistribution notes.
- Fetch/parse utility plan is ready for implementation.
- A first Binwalk spike fixture is selected.
- Automated-test subset policy is documented.
