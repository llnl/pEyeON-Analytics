---
title: "Implementation Plan: Update to Loreforge Roles and Wiki Features"
type: concept
confidence: high
grounded_by:
  - AGENTS.md
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: dev-environment
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/update-loreforge/implementation_plan.md
tags: [feature-work, loreforge, workflow, roles]
---

# Implementation Plan: Update to Loreforge Roles and Wiki Features

> Reverse-engineered artifact: plan as executed on 2026-08-17.

## Scope

`AGENTS.md` and everything under `wiki/`. No code, no `raw/` content changes,
no `../pEyeON` changes.

## Steps

1. Create branch `grantj-update-loreforge`.
2. Create `wiki/work/update-loreforge/` artifacts (this folder).
3. Rewrite `AGENTS.md`: roles, path-mapping table, new frontmatter schema,
   singular layout, `SYNTHESIS` marker, feature workflow, domain context,
   EyeON reasoning lint rules, merged ADR format, role-aware session startup.
4. `git mv` the six plural wiki directories to singular names.
5. Rewrite all wikilinks, `grounded_by`, and prose path references from
   plural to singular across `wiki/` (scripted).
6. Bulk-backfill frontmatter on every wiki page (scripted): `component:` →
   `repo_scope:`; add `implementation_area`, `format_domain`, `audience`,
   `status`, `source_paths` per the inference rules in
   [[wiki/work/update-loreforge/design]].
7. Update `wiki/concept/llm_assisted_feature_workflow.md` and
   `wiki/concept/feature_work_template.md` to the Wintap versions adapted to
   EyeON paths and Developer-role invocation.
8. Update `wiki/work/firmware-corpus/dev_handoff.md` and
   `implementation_plan.md` handoff prompts to Developer-role invocation.
9. Update `wiki/index.md`; append migration entry to `wiki/log.md`.
10. Run deterministic lint; record results in
    [[wiki/work/update-loreforge/verification]].
11. Commit on `grantj-update-loreforge`.

## Files Likely To Change

`AGENTS.md`; all ~78 pages under `wiki/`; `wiki/index.md`; `wiki/log.md`.

## Tests To Add Or Update

No code tests. Verification is the deterministic lint pass (broken links,
frontmatter enums, stale plural paths) recorded in `verification.md`.

## Migration Or Compatibility Notes

- Old `component:` values map: `pEyeON-core`→`pEyeON`,
  `pEyeON-analytics`→`pEyeON-Analytics`, `both`→`cross-repo`.
- `status:` backfilled as `reviewed` where `confidence: high`, else `draft`.
- `audience:` backfilled as `mixed`.
- Historical `wiki/log.md` entries are not rewritten; they may reference the
  old mode names and plural paths as historical record.

## Rollback Plan

All changes are on branch `grantj-update-loreforge`; rollback is discarding
the branch. `main` is untouched.

## Done Checklist

- [x] Branch created
- [x] Feature artifacts created
- [x] AGENTS.md rewritten (roles only, all Wintap features merged)
- [x] Directories renamed to singular
- [x] All links and path references fixed
- [x] Frontmatter backfilled on all pages
- [x] Workflow concept pages updated
- [x] Old mode-language handoffs updated
- [x] index.md and log.md updated
- [x] Lint pass clean; verification.md recorded
- [x] Committed on grantj-update-loreforge
