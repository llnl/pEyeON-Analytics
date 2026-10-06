---
title: "Implementation Plan: AssemblyLine Plugin Using eyeon-parse.sh"
type: concept
confidence: low
grounded_by:
  - wiki/work/assemblyline-eyeon-parse-plugin/brief.md
  - wiki/work/assemblyline-eyeon-parse-plugin/design.md
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: scanner
format_domain: cross-domain
audience: developer
status: draft
source_paths: wiki/work/assemblyline-eyeon-parse-plugin/implementation_plan.md
tags: [feature-work, assemblyline, plan, wrapper]
---

# Implementation Plan: AssemblyLine Plugin Using eyeon-parse.sh

## Scope

This plan targets a new standalone AssemblyLine 4 service repository. It is
ready to become a developer handoff after the pinned versions and data-handling
policy are approved.

## Steps

1. Create `assemblyline-service-eyeon` using the selected AssemblyLine 4 service
   skeleton and pin its compatible SDK version.
2. Build a service image embedding a pinned EyeON release and record the version
   in build metadata and result output.
3. Stage one submission in a unique per-job source directory and create a
   unique output directory; never share workspaces between jobs.
4. Validate configured input, timeout, thread, archive-size, and observation
   count limits. Default to the approved values: 1 GiB, 15 minutes, two threads,
   512 MiB, and 10,000 JSON files.
5. Invoke `eyeon parse` directly with an argument array, a `WARNING` log level,
   bounded output capture, timeout/cancellation handling, and no shell.
6. Discover all generated JSON, derive identity/type/count summary data, and
   create one deterministic compressed archive with stable relative paths.
7. Attach the archive and emit a visible nonfatal AssemblyLine result for each
   successful, unsupported, partial, timeout, limit, or scanner-failure state.
8. Clean up per-job source/output data after AssemblyLine has accepted the
   attachment, according to the approved retention policy.
9. Add unit tests, AssemblyLine service-contract tests, opt-in service-container
   integration tests, and a real AssemblyLine worker smoke test before release.

## Files Likely To Change

- `assemblyline-service-eyeon`: service implementation, configuration schema,
  Dockerfile, pinned dependency metadata, fixtures, tests, CI, and release docs.
- Canonical wiki pages documenting the shipped integration.

## Tests To Add Or Update

- Process-argument and environment construction tests.
- Temporary-workspace isolation and cleanup tests.
- Scanner nonzero exit, timeout, cancellation, missing-output, partial-output,
  oversized-output, and observation-count-limit tests.
- AssemblyLine SDK contract tests.
- Opt-in integration test against the approved EyeON image and AssemblyLine
  worker runtime.

## Migration Or Compatibility Notes

No migration is proposed. Existing host users of `eyeon-parse.sh` are unaffected
because the service invokes the pinned scanner directly.

## Rollback Plan

Disable or roll back the AssemblyLine service image/configuration to the prior
tested release. The scanner and host wrapper are unchanged.

## Done Checklist

- [x] Architect selected AssemblyLine 4, a standalone service repository, and a
  dedicated pinned EyeON service image.
- [x] Architect selected one submitted file, raw JSON archive attachment, and
  identity/type/count summary results.
- [x] Architect selected visible nonfatal service outcomes and unit plus
  service-container integration validation.
- [ ] Pin the AssemblyLine 4/service SDK and EyeON versions.
- [x] Architect approved defaults: 1 GiB input, 15 minutes, two threads, 512
  MiB archive, and 10,000 observation JSON files per job.
- [ ] Decide raw-result classification and retention policy.
- [ ] Architect approves a complete `dev_handoff.md`.
- [ ] Developer implementation is complete.
- [ ] Automated tests pass.
- [ ] Representative integration validation is recorded in `verification.md`.
- [ ] Durable facts are promoted into canonical wiki pages.
