---
title: "Dev Handoff: Firmware Corpus"
type: concept
confidence: medium
grounded_by:
  - wiki/work/firmware-corpus/brief.md
  - wiki/work/firmware-corpus/design.md
  - wiki/work/firmware-corpus/implementation_plan.md
  - wiki/work/firmware-corpus/references.md
  - raw/binwalk/binwalk.md
policy: agent-editable
last_validated: 2026-06-26
repo_scope: cross-repo
implementation_area: scanner
format_domain: firmware
audience: mixed
status: draft
source_paths: wiki/work/firmware-corpus/dev_handoff.md
tags: [feature-work, handoff, firmware, corpus]
---

# Dev Handoff: Firmware Corpus

**Status:** Approved
**Architect Approval:** Grandfathered (handoff pre-dates the approval header convention introduced 2026-08-17)

## Copy/Paste Prompt

Use this prompt to hand the work to a Developer session:

```text
As the Developer, implement the feature: firmware-corpus.

Use these wiki files as the handoff context:

- wiki/work/firmware-corpus/brief.md
- wiki/work/firmware-corpus/references.md
- wiki/work/firmware-corpus/candidates.md
- wiki/work/firmware-corpus/design.md
- wiki/work/firmware-corpus/implementation_plan.md
- wiki/work/firmware-corpus/verification.md
- wiki/work/binwalk-support/brief.md
- raw/binwalk/binwalk.md

Goal: implement the first Firmware Corpus slice. Do not implement full Binwalk support yet.

First slice:

- Define a source-controlled firmware corpus manifest format.
- Add a small initial manifest with candidate entries from raw/binwalk/binwalk.md.
- Use wiki/work/firmware-corpus/candidates.md to select entries for unit-small, binwalk-smoke, demo-vulnerable, demo-utility, and bulk-index subsets.
- Add a utility or script to list corpus entries.
- Add fetch support for direct-download entries if it can be implemented cleanly and tested without relying on network access in default tests.
- Include checksum fields and verification behavior where checksums are available.
- Keep downloaded firmware and generated outputs out of git.
- Add tests for manifest loading/listing and non-network fetch/checksum logic.
- Update wiki/work/firmware-corpus/verification.md with commands run and results.
- Update wiki/work/firmware-corpus/implementation_plan.md checkboxes as work completes.

Before editing code, read AGENTS.md and confirm this handoff is Approved.
```

## Handoff Summary

The Firmware Corpus feature is the first implementation slice that supports
later Binwalk integration. It should create a repeatable way to describe,
fetch, and parse firmware samples without committing large firmware binaries to
the repository.

This work should produce infrastructure useful for:

- Binwalk spike fixtures.
- Future Binwalk extraction tests.
- New-user demos.
- Show-and-tell stories over known firmware samples.
- Opt-in integration tests over real firmware.

## Primary Sources For The Dev Agent

Read these first:

- [[wiki/work/firmware-corpus/brief]] - requirements and acceptance criteria.
- [[wiki/work/firmware-corpus/design]] - manifest-driven design and utility behavior.
- [[wiki/work/firmware-corpus/implementation_plan]] - step list and done checklist.
- [[wiki/work/firmware-corpus/references]] - candidate source URLs and evaluation checklist.
- [[wiki/work/firmware-corpus/candidates]] - expanded candidate catalog and suggested subsets.
- [[wiki/work/binwalk-support/brief]] - why the corpus matters for Binwalk support.
- `raw/binwalk/binwalk.md` - seed source for candidate firmware corpus entries.

## Recommended First Implementation Slice

Prefer the smallest useful code change:

1. Choose a manifest location and format after inspecting existing repo layout.
2. Add a manifest with a few candidate entries, but do not download firmware into git.
3. Add a list command or script that reads and displays entries from the manifest.
4. Add manifest validation tests.
5. Add fetch/checksum logic only if it can be cleanly unit-tested without network access.
6. Keep parse integration as a follow-up unless the repo already has a natural hook.

## Suggested Manifest Semantics

The manifest should support, at minimum:

| Field | Purpose |
| --- | --- |
| `id` | Stable short identifier |
| `name` | Human-readable sample name |
| `category` | `vulnerable-demo`, `vendor-real-world`, `open-source-baseline`, or `research-dataset` |
| `source_url` | Direct download URL or landing page |
| `source_type` | `direct-download`, `vendor-page`, `git-repo`, `archive`, or `dataset-index` |
| `license_or_terms` | Redistribution/use notes |
| `redistributable` | `yes`, `no`, `unknown`, or `user-must-download` |
| `expected_size` | Optional byte size or rough size class |
| `sha256` | Optional checksum |
| `expected_binwalk` | Expected high-level Binwalk signatures, if known |
| `use_cases` | `unit-test`, `integration-test`, `demo`, `spike`, or `research` |
| `notes` | Caveats, CVE story, or manual download instructions |

The exact manifest format is open. YAML or TOML are both reasonable if they fit
the repo's existing conventions.

## Candidate Initial Entries

Seed candidates come from `raw/binwalk/binwalk.md` and [[wiki/work/firmware-corpus/candidates]]:

- Tasmota: likely best first small fetch/checksum candidate.
- OpenWrt: likely best first clean embedded-Linux baseline candidate.
- IoTGoat: likely best first deliberately vulnerable demo candidate.
- DVRF: likely useful vulnerable-demo candidate if licensing and size are acceptable.
- OpenIPC: useful camera firmware corpus candidate with many release assets.
- D-Link DIR-605L: useful real-world vendor candidate, likely opt-in/manual-download.
- Moxa/HMS/Schneider/Siemens/WAGO/Beckhoff: useful industrial and utility-adjacent demo candidates, likely starting as `vendor-page` or `manual` entries.
- FirmSecDataset, FirmAE, FIRMADYNE, iotwizz, and Netgear GPL Archive: useful bulk/index candidates, not default test inputs.

The dev agent should verify URLs, terms, and direct-download feasibility before
marking entries as testable or fetchable.

## Non-Goals For This Slice

- Do not implement full Binwalk scanning or extraction.
- Do not commit full firmware images by default.
- Do not add Streamlit reporting for corpus data.
- Do not make network-dependent tests part of the default test suite.
- Do not verify or promote vulnerability claims without primary sources.

## Testing Expectations

Default tests should not require network access. Good first tests include:

- Manifest loads successfully.
- Required fields are present.
- Categories and redistributable values are valid.
- List command/script returns expected entries.
- Checksum verification can be tested against small local bytes or mocked data.
- Fetch behavior uses mocks or local temporary files unless explicitly marked as opt-in integration.

Network or large-file tests should be opt-in and clearly documented.

## Git Hygiene

The dev agent should ensure local corpus caches and downloaded firmware files are
ignored by git. Suggested ignored path names include:

- `firmware-corpus/`
- `.firmware-corpus/`
- `data/firmware-corpus/`
- generated EyeON output directories from corpus runs

Final ignored paths should match the implementation's chosen cache layout.

## Closeout Instructions

When the Developer finishes the first slice:

- Update [[wiki/work/firmware-corpus/verification]] with commands run and results.
- Update [[wiki/work/firmware-corpus/implementation_plan]] done checklist.
- Update [[wiki/work/binwalk-support/brief]] if a first Binwalk spike fixture is selected.
- Append a concise entry to `wiki/log.md`.
- If durable behavior is established, update canonical wiki pages such as pipeline/component docs.

## Role Note

`AGENTS.md` defines the Architect / Engineer / Developer roles. This handoff
is intended to be used with the explicit trigger:

```text
As the Developer, implement the feature: firmware-corpus.
```

In the Developer role, the agent may modify source code, tests, configs,
scripts, and documentation in this repository as needed, plus this feature's
`verification.md`, `implementation_plan.md` checklist, and `wiki/log.md`.
`raw/` and `../pEyeON` remain protected unless separately authorized.
