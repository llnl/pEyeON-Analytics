---
title: "Feature Brief: Update to Loreforge Roles and Wiki Features"
type: concept
confidence: high
grounded_by:
  - AGENTS.md
  - ../Wintap-Analytics/AGENTS.md
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: dev-environment
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/update-loreforge/brief.md
tags: [feature-work, loreforge, workflow, roles, wiki-infrastructure]
---

# Feature Brief: Update to Loreforge Roles and Wiki Features

> Reverse-engineered artifact: this feature was executed conversationally on
> 2026-08-17 and documented retroactively in the workflow format it introduces.

## Problem

The pEyeON-Analytics `AGENTS.md` predates the latest loreforge/llm-wiki
conventions. It uses the older wiki-maintainer / code-development operating
modes, an older frontmatter schema, plural wiki directory names, and lacks the
formalized LLM-assisted feature workflow, `SYNTHESIS` marker, domain-context
section, and domain-specific lint rules that the Wintap Ecosystem wiki
(`../Wintap-Analytics/AGENTS.md`) already uses. Meanwhile the global opencode
role prompts (`~/.config/opencode/prompts/engineer.md` and `developer.md`)
define an Engineer/Developer/Architect model that has eclipsed the two-mode
model, and their default paths (`.wiki/`, `/developer_docs/`) do not exist in
this repository.

## Goals

- Replace wiki-maintainer / code-development modes with the Architect /
  Engineer / Developer role model. Roles only; no dual mode/role language.
- Merge in all newer Wintap AGENTS.md features: richer frontmatter, invocable
  feature workflow, singular directory names and new page types, `SYNTHESIS`
  marker, domain-specific reasoning lint rules, domain-context section, and the
  scaffolding-vs-canonical promotion rule.
- Reconcile the global role prompts' paths onto this repo's `wiki/` + `raw/`
  layout via an explicit path-mapping table (AGENTS.md wins).
- Migrate the existing wiki (~78 pages) without breaking links: singular
  directory renames, frontmatter backfill, updated workflow concept pages.

## Non-Goals

- No restructuring to `.wiki/` + `developer_docs/` directories.
- No renaming of individual page filenames (directories only); existing
  underscore filenames remain, new pages use kebab-case.
- No renaming of existing ADR files; date-prefixed kebab names apply to new
  ADRs only.
- No changes to `../pEyeON`, source code, dbt models, or the Streamlit app.
- No changes to the global role prompts in `~/.config/opencode/`.

## User-Facing Behavior

- Sessions start in the Engineer role by default; the Developer role activates
  on an approved `dev_handoff.md`.
- `Start a new feature using the LLM-assisted feature workflow: <name>`
  creates a skeleton under `wiki/work/<feature-slug>/`.
- LINT enforces the new frontmatter schema and source policy.

## Acceptance Criteria

- AGENTS.md contains no wiki-maintainer / code-development mode language and
  defines the three roles with a path-mapping table.
- All wiki directories use singular names; zero broken wikilinks or
  `grounded_by` paths.
- Every wiki page carries the new frontmatter fields with valid enum values.
- The two workflow concept pages match the Wintap versions adapted to EyeON.
- `wiki/index.md` and `wiki/log.md` reflect the migration.

## Affected Areas

`AGENTS.md`; every page under `wiki/`; no code paths.

## References

See [[wiki/work/update-loreforge/references]]. The approved instruction
document is [[wiki/work/update-loreforge/dev_handoff]]; the executed plan is
[[wiki/work/update-loreforge/implementation_plan]].

## Open Questions

None at close; all design questions were settled by the Architect on
2026-08-17 (see [[wiki/work/update-loreforge/design]]).

## Test Plan

Deterministic lint pass after migration: broken-wikilink scan, frontmatter
field/enum validation, stale-path scan for old plural directory references.
Results recorded in [[wiki/work/update-loreforge/verification]].

## Done When

Acceptance criteria pass lint, the migration is committed on the
`grantj-update-loreforge` branch, and the log entry is appended.
