---
title: "Feature References: Update to Loreforge Roles and Wiki Features"
type: concept
confidence: high
grounded_by:
  - ../Wintap-Analytics/AGENTS.md
  - AGENTS.md
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: dev-environment
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/update-loreforge/references.md
tags: [feature-work, loreforge, workflow, roles]
---

# Feature References: Update to Loreforge Roles and Wiki Features

> Reverse-engineered artifact: sources reviewed during the 2026-08-17 session.

## Live Repo Sources

- `AGENTS.md` (this repo, pre-migration) — the older contract being replaced.
- `../Wintap-Analytics/AGENTS.md` — reference implementation of the latest
  loreforge/llm-wiki conventions; source of the merged features.
- `../Wintap-Analytics/wiki/concept/llm-assisted-feature-workflow.md` — the
  formalized feature workflow (stages, invocation phrase, promotion rule).
- `../Wintap-Analytics/wiki/concept/feature-work-template.md` — artifact
  skeletons and work-artifact frontmatter conventions.
- `../Wintap-Analytics/opencode.json` — minimal per-repo opencode config
  (`"instructions": ["AGENTS.md"]`).

## External Sources

- `~/.config/opencode/prompts/engineer.md` (v1.0, 2026-06-18) — global
  Engineer role: design collaborator, sole wiki keeper, instruction-document
  author; never writes code. Defers to repo AGENTS.md.
- `~/.config/opencode/prompts/developer.md` (v1.0, 2026-06-18) — global
  Developer role: implements one approved instruction at a time, audit
  artifact per unit, full test output, source-of-truth order. Defers to repo
  AGENTS.md.
- `~/.config/opencode/opencode.jsonc` — wires both prompts as primary agents.

These live outside any project repo, so they are cited by path here rather
than copied into `raw/` (they are user-maintained config, re-readable live).

## Related Wiki Pages

- [[wiki/concept/llm_assisted_feature_workflow]] — updated by this feature.
- [[wiki/concept/feature_work_template]] — updated by this feature.
- [[wiki/decision/feature_work_artifacts]] — prior decision on artifact
  organization; remains valid under the new contract.

## Libraries And APIs

None — documentation/process change only.

## Notes

Key findings from the review rounds:

- The role prompts reference `.wiki/wiki/`, `.wiki/sources/`, and
  `/developer_docs/{instructions,design,features,audits}/`, none of which
  exist in either repo. Both prompts state the repo AGENTS.md wins, so the
  new AGENTS.md carries an explicit path-mapping table.
- The engineer.md ADR body format (Options Considered, Tradeoffs) differs
  from the wiki ADR frontmatter format; the merged format keeps the YAML
  frontmatter and adopts the richer body sections.
- Wintap's `event_domain` frontmatter field has no direct EyeON analog;
  `format_domain` was defined in its place.
