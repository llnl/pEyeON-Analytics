---
title: "Dev Handoff: Update to Loreforge Roles and Wiki Features"
type: concept
confidence: high
grounded_by:
  - AGENTS.md
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: dev-environment
format_domain: none
audience: llm-agent
status: reviewed
source_paths: wiki/work/update-loreforge/dev_handoff.md
tags: [feature-work, loreforge, workflow, roles]
---

# Dev Handoff: Update to Loreforge Roles and Wiki Features

**Status:** Approved
**Architect Approval:** Approved 2026-08-17

> Reverse-engineered artifact: this handoff was executed in the same session
> it was designed in; it is recorded in the instruction-document format the
> migration introduces.

## Copy/Paste Prompt

Use this prompt to hand the work to a Developer session:

    As the Developer, implement the feature: update-loreforge.

    Use these wiki files as the handoff context:

    - wiki/work/update-loreforge/brief.md
    - wiki/work/update-loreforge/design.md
    - wiki/work/update-loreforge/implementation_plan.md

    Goal: migrate AGENTS.md and the wiki to the loreforge
    Engineer/Developer role model and newer wiki conventions on branch
    grantj-update-loreforge.

    Before editing, read AGENTS.md and confirm this handoff is Approved.

## Handoff Summary

Rewrite AGENTS.md around Architect/Engineer/Developer roles with a
path-mapping table for the global role prompts; rename wiki directories to
singular; backfill the new frontmatter schema on all pages; update the two
workflow concept pages; fix all links; lint; commit.

## Primary Sources For The Dev Agent

- `../Wintap-Analytics/AGENTS.md` — reference for merged features
- `~/.config/opencode/prompts/engineer.md`, `developer.md` — role contracts
- [[wiki/work/update-loreforge/design]] — settled decisions

## Recommended First Implementation Slice

AGENTS.md rewrite first (it defines the target schema), then the mechanical
wiki migration against it.

## Non-Goals For This Slice

See the Non-Goals section of [[wiki/work/update-loreforge/brief]] — no repo
restructure, no page-filename renames, no code or `../pEyeON` changes.

## Testing Expectations

Deterministic lint pass with zero broken wikilinks, zero stale plural paths,
and full frontmatter enum compliance; full scan output pasted (not
summarized) into `verification.md`.

## Closeout Instructions

- Update wiki/work/update-loreforge/verification.md with commands run and results.
- Update the wiki/work/update-loreforge/implementation_plan.md done checklist.
- Append a concise entry to wiki/log.md.
- Promote durable facts into canonical wiki pages once behavior stabilizes.
