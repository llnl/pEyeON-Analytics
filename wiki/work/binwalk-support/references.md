---
title: "Feature References: Binwalk Support"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
last_validated: 2026-06-26
repo_scope: cross-repo
implementation_area: scanner
format_domain: firmware
audience: mixed
status: draft
source_paths: wiki/work/binwalk-support/references.md
tags: [feature-work, references, binwalk, firmware]
---

# Feature References: Binwalk Support

## Live Repo Sources

- TBD: core scanner files in `../pEyeON/`.
- TBD: schemas in `../pEyeON/schema/`.
- TBD: analytics loader/schema/dbt files in this repository.

## Internal Notes

- `raw/binwalk/binwalk.md` - brainstorming notes, Binwalk status summary, firmware corpus candidates, and quick-start commands.

## External Sources

- Binwalk v3 Rust implementation: https://github.com/ReFirmLabs/binwalk
- Binwalk3 Python compatibility package repository: https://github.com/ZachFlint/Binwalk3
- Binwalk3 PyPI package: https://pypi.org/project/binwalk3/
- TBD: dependency packaging and licensing references.

## Related Wiki Pages

- [[wiki/work/firmware-corpus/brief]]
- [[wiki/work/firmware-corpus/references]]
- [[wiki/component/observe]]
- [[wiki/component/parse]]
- [[wiki/component/container]]
- [[wiki/component/surfactant_plugins]]
- [[wiki/schema/observation_schema]]
- [[wiki/overview/data_flow]]
- [[wiki/tension/parent_field]]
- [[wiki/tension/archive_recursion]]

## Libraries And APIs

- Binwalk v3 CLI. Raw notes show install paths via `cargo install binwalk` or Docker, and common commands such as `binwalk firmware.bin`, `binwalk -eM firmware.bin`, and `binwalk -E firmware.bin`.
- `binwalk3` Python package. The package presents a Binwalk v2-compatible Python API backed by the Binwalk v3 Rust binary.
- `binwalk.scan(*files, extract=True, matryoshka=True)` and `Modules().execute(..., extract=True, matryoshka=True)` are candidate APIs for the spike.

## Notes

- Binwalk support may overlap with archive recursion and parent/child modeling.
- Current desired direction is both detection and extraction, with extracted children parsed by normal EyeON logic.
- Avoid calling sample fixtures "Binwalk files"; use "firmware fixtures", "container fixtures", or "Binwalk fixture corpus" to avoid confusing the tool with the input file type.
- The raw Binwalk notes mention candidate fixture sources such as DVRF, OpenWrt, DD-WRT, and vendor firmware archives. Fixture licensing, size, and redistribution need explicit review before tests are committed.
