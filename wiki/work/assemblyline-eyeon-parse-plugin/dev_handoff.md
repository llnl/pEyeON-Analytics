---
title: "Dev Handoff: AssemblyLine Plugin Using eyeon-parse.sh"
type: concept
confidence: medium
grounded_by:
  - wiki/work/assemblyline-eyeon-parse-plugin/brief.md
  - wiki/work/assemblyline-eyeon-parse-plugin/design.md
  - wiki/work/assemblyline-eyeon-parse-plugin/implementation_plan.md
policy: human-review-required
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: scanner
format_domain: cross-domain
audience: developer
status: draft
source_paths: wiki/work/assemblyline-eyeon-parse-plugin/dev_handoff.md
tags: [feature-work, assemblyline, handoff, wrapper]
---

# Dev Handoff: AssemblyLine Plugin Using eyeon-parse.sh

**Status:** Draft
**Architect Approval:** Pending

## Copy/Paste Prompt

Use this prompt after this handoff is approved:

    As the Developer, implement the feature: assemblyline-eyeon-parse-plugin.

    Use these wiki files as the handoff context:

    - wiki/work/assemblyline-eyeon-parse-plugin/brief.md
    - wiki/work/assemblyline-eyeon-parse-plugin/references.md
    - wiki/work/assemblyline-eyeon-parse-plugin/design.md
    - wiki/work/assemblyline-eyeon-parse-plugin/implementation_plan.md
    - wiki/work/assemblyline-eyeon-parse-plugin/dev_handoff.md

    Goal: create the approved standalone AssemblyLine 4 EyeON service.

    Before editing, read AGENTS.md and confirm this handoff is Approved.

## Handoff Summary

Create a new standalone `assemblyline-service-eyeon` repository containing an
AssemblyLine 4 analysis service. The service image embeds a pinned EyeON
release, stages one submitted file per isolated job workspace, and invokes
`eyeon parse` directly. It must not invoke Docker, Podman, or the host-oriented
`eyeon-parse.sh` from a worker.

The service attaches one deterministic compressed archive containing all JSON
observations generated for the job. Its concise result exposes identity,
detected file types, observation/metadata counts, pinned EyeON version, and
scan status. Unsupported, partial, timeout, resource-limit, and scanner-failure
outcomes must be visible nonfatal results.

## Primary Sources For The Dev Agent

- `eyeon-parse.sh`: behavior to preserve where relevant: source validation,
  explicit output placement, thread control, `WARNING` default logging, and
  actionable diagnostics.
- `wiki/pipeline/eyeon_parse_sh.md`: wrapper contract and host-only behavior
  intentionally excluded from the service.
- Version-pinned AssemblyLine 4 service SDK documentation, added before work
  begins.
- Version-pinned EyeON source/image reference, added before work begins.

## Required Decisions Before Approval

- Exact AssemblyLine 4 release and compatible service SDK version.
- Exact pinned EyeON release/image digest.
- Classification and retention policy for raw JSON archive and service logs.

## Approved Defaults

- Maximum input: 1 GiB.
- Scan timeout: 15 minutes.
- EyeON threads: 2.
- Maximum compressed JSON archive: 512 MiB.
- Maximum generated observation JSON files: 10,000.

These values must be configurable and deployment configuration may lower them.

## Recommended First Implementation Slice

1. Scaffold the standalone AssemblyLine 4 service and service image.
2. Implement per-job workspace staging and direct argument-array invocation of
   the pinned EyeON binary.
3. Implement configuration validation and the approved resource limits.
4. Collect JSON output, create a deterministic archive, and derive the concise
   summary result.
5. Add mocked unit tests and an opt-in service-container integration fixture.

## Non-Goals For This Slice

- Nested Docker/Podman execution.
- Modifying `eyeon-parse.sh` or `../pEyeON`.
- AssemblyLine-extracted-child submission orchestration.
- Detailed EyeON metadata-to-tag mapping beyond identity/type/count summary.
- DLT/dbt/Streamlit integration.

## Testing Expectations

- Unit tests cover config validation, process arguments, timeout/cancellation,
  output discovery, deterministic archive contents, summary derivation, and all
  visible nonfatal outcomes.
- Service-contract tests use the pinned AssemblyLine 4 SDK harness.
- An opt-in container integration test runs the pinned EyeON binary on a small
  representative fixture.
- A release-blocking smoke test runs the image in a real AssemblyLine 4 worker.

## Closeout Instructions

- Update `wiki/work/assemblyline-eyeon-parse-plugin/verification.md` with every
  command and its full output.
- Update this feature's `implementation_plan.md` done checklist.
- Append a concise entry to `wiki/log.md`.
- Promote durable shipped behavior into canonical wiki pages.
