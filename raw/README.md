# raw/

**External documents only.** This directory holds source material that has no
stable live path in a git repo — format specs, papers, and freeform notes.

Do not copy files here from `../pEyeON/` or from this repo. Those are cited
directly by live path. See the Source Policy section in `../AGENTS.md`.

## What belongs here

- `specs/` — vendor format specs (PE, ELF ABI, U-Boot image format, OLE compound doc, etc.)
- `papers/` — academic papers on supply chain security, binary analysis, fuzzy hashing
- `notes/` — your own freeform notes, meeting notes, design sketches

## Required header for every file in raw/

Every file added to this directory must include a provenance header at the top:

```markdown
<!-- SOURCE: <URL or citation where this document came from> -->
<!-- RETRIEVED: YYYY-MM-DD -->
```

Files missing this header will be flagged by lint as unprovenanced sources.

## What is cited in place (not here)

The following are read directly from their live locations and never copied:

| What | Live path |
|------|-----------|
| pEyeON README | `../pEyeON/README.md` |
| pEyeON CONTRIBUTING | `../pEyeON/CONTRIBUTING.md` |
| Observation schema | `../pEyeON/schema/observation.schema.json` |
| Software schema | `../pEyeON/schema/software.schema.json` |
| observe.py | `../pEyeON/src/eyeon/observe.py` |
| dbt models | `../pEyeON-Analytics/dbt_eyeon_gold/models/` |
| Analytics README | `../pEyeON-Analytics/README.md` (this repo) |

## Suggested specs to add

- PE/COFF specification — https://learn.microsoft.com/en-us/windows/win32/debug/pe-format
- ELF specification — System V ABI, https://refspecs.linuxfoundation.org/elf/elf.pdf
- U-Boot image format — https://u-boot.readthedocs.io/en/latest/usage/fit/
- Mach-O ABI reference — Apple developer docs
- OLE Compound Document spec — Microsoft Open Specifications
- telfhash paper — TrendMicro, https://github.com/trendmicro/telfhash
- imphash blog post — Mandiant, https://www.mandiant.com/resources/blog/tracking-malware-import-hashing
