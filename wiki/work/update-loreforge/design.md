---
title: "Feature Design: Update to Loreforge Roles and Wiki Features"
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
source_paths: wiki/work/update-loreforge/design.md
tags: [feature-work, loreforge, workflow, roles]
---

# Feature Design: Update to Loreforge Roles and Wiki Features

> Reverse-engineered artifact: captures the design settled across two review
> rounds with the Architect on 2026-08-17.

## Summary

Rewrite AGENTS.md around the Architect/Engineer/Developer role model, merge
all newer Wintap wiki features, and migrate the existing wiki in one pass on
branch `grantj-update-loreforge`.

## Proposed Approach

### Roles (replace operating modes entirely)

- **Architect** — the human; decision-maker and approval gate.
- **Engineer** — default role. Wiki keeper and design collaborator; absorbs
  all former wiki-maintainer duties plus the engineer.md workflow (2–3
  options with tradeoffs, ADRs on settled decisions, Architect-approved
  handoffs, session-close checklist). Writes only to `wiki/`.
- **Developer** — implements one approved `dev_handoff.md` at a time; may
  modify code/tests/configs in this repo; `wiki/` is read-only except the
  active `wiki/work/<slug>/` folder and `wiki/log.md` (documented deviation
  from developer.md, since AGENTS.md wins).

### Path mapping (role prompts → this repo)

| Role-prompt path | This repo |
|---|---|
| `.wiki/wiki/` | `wiki/` |
| `.wiki/sources/` | `raw/notes/` |
| `.wiki/wiki/decisions/` | `wiki/decision/` |
| `/developer_docs/instructions/` | `wiki/work/<slug>/dev_handoff.md` |
| `/developer_docs/audits/` | `wiki/work/<slug>/verification.md` |
| `/developer_docs/design/`, `/developer_docs/features/` | `wiki/work/<slug>/design.md`, `brief.md` |

## Data Model Or Schema Changes

New frontmatter schema (all wiki pages):

- `repo_scope: pEyeON | pEyeON-Analytics | cross-repo` — replaces `component:`
  (mapping: pEyeON-core→pEyeON, pEyeON-analytics→pEyeON-Analytics,
  both→cross-repo).
- `implementation_area: scanner | schema | dlt-pipeline | dbt-gold |
  streamlit | container | surfactant-plugins | analytics | dev-environment`
- `format_domain: executable | firmware | archive | script | document |
  package | container-image | cross-domain | none` — EyeON analog of Wintap's
  `event_domain`.
- `audience: llm-agent | developer | researcher | mixed`
- `status: stub | draft | reviewed | stable`
- `source_paths: <repo-relative page path>`

Directory renames (plural → singular): `components/ concepts/ decisions/
tensions/ schemas/ file_formats/` → `component/ concept/ decision/ tension/
schema/ file_format/`. `overview/ pipeline/ work/` unchanged. New page types
available on demand: `data_model`, `tool`, `workflow`, `repo`, `diagnostic`.

## Interfaces And User Experience

- Invocation phrase: `Start a new feature using the LLM-assisted feature
  workflow: <name>` creates `wiki/work/<feature-slug>/`.
- `dev_handoff.md` gains `Status: Draft | Approved` and `Architect Approval:`
  header lines and serves as the instruction document.
- `verification.md` gains full-test-output and Deviations-From-Handoff
  sections and serves as the audit artifact.
- `SYNTHESIS` marker joins `GROUND_TRUTH` and `SPECULATIVE`.
- ADR body merges engineer.md sections: Context, Decision, Options
  Considered, Tradeoffs, Consequences, Supersedes/Superseded-By.

## Edge Cases

- Existing filenames keep underscores; only directories rename. New pages use
  kebab-case.
- Existing ADR filenames unchanged; new ADRs use `YYYY-MM-DD-kebab-title.md`.
- Three files carried old mode language (firmware-corpus `dev_handoff.md`,
  `implementation_plan.md`, `wiki/log.md`); handoff prompts are updated to
  invoke the Developer role. Historical log entries are left verbatim.

## Error Handling

LINT is the safety net: broken-wikilink scan, frontmatter enum validation,
and a stale-path scan for lingering plural-directory references run after
migration and on every future lint pass.

## Risks

- Link breakage across ~78 pages during renames — mitigated by scripted
  rewrite plus deterministic lint.
- Backfilled `implementation_area`/`format_domain` values are inferred and
  may need refinement as pages are next touched (values chosen from page type
  and directory; recorded as reviewed only where confidence was high).

## Alternatives Considered

- **Restructure to `.wiki/` + `developer_docs/`** for exact role-prompt
  fidelity — rejected: bigger move, breaks existing layout, and the prompts
  themselves defer to AGENTS.md.
- **Keep plural directory names** — rejected: chose full consistency with the
  loreforge convention; link churn handled once, mechanically.
- **Lazy frontmatter backfill** — rejected: bulk backfill keeps LINT clean
  from day one.
- **Dedicated `instruction.md`/`audit.md` artifacts** — rejected: reusing
  `dev_handoff.md`/`verification.md` avoids duplicate artifact types.

## Open Questions

None — all four design questions were put to the Architect and settled on
2026-08-17 (singular renames: yes; path mapping over restructure; bulk
backfill; reuse existing artifacts).
