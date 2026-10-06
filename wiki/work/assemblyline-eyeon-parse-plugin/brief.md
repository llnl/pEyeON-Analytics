---
title: "Feature Brief: AssemblyLine Plugin Using eyeon-parse.sh"
type: concept
confidence: medium
grounded_by:
  - eyeon-parse.sh
  - wiki/pipeline/eyeon_parse_sh.md
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: scanner
format_domain: cross-domain
audience: mixed
status: draft
source_paths: wiki/work/assemblyline-eyeon-parse-plugin/brief.md
tags: [feature-work, assemblyline, plugin, wrapper, orchestration]
---

# Feature Brief: AssemblyLine Plugin Using eyeon-parse.sh

## Problem

EyeON batch parsing is currently initiated through the host-oriented
`eyeon-parse.sh` wrapper. An AssemblyLine integration is needed to make that
scan capability available from an AssemblyLine analysis workflow without
requiring analysts to manually construct a wrapper invocation.

## Goals

- Define a new standalone AssemblyLine 4 service repository named
  `assemblyline-service-eyeon`.
- Build a dedicated service image with a pinned, tested EyeON version; do not
  run Docker or Podman from inside an AssemblyLine worker.
- Invoke EyeON directly using the behavior of `eyeon-parse.sh` that is relevant
  to noninteractive per-job scanning: an isolated source directory, explicit
  output directory, thread limit, and deterministic batch collection.
- Attach a deterministic archive containing every JSON observation generated for
  the submitted file.
- Present submission identity, detected file types, observation/metadata counts,
  and status in a concise AssemblyLine result.
- Define service configuration, resource limits, logging, and failure behavior.
- Add automated tests that do not require production AssemblyLine or container
  infrastructure by default.

## Non-Goals

- Replacing the EyeON scanner, its container image, or the existing host
  wrapper.
- Redesigning EyeON observation schemas.
- Loading results into this repository's DLT/dbt/Streamlit analytics pipeline.
- Bundling or operating an AssemblyLine deployment as part of this repository.
- Calling Docker or Podman, or `eyeon-parse.sh`, from inside the service worker.
- Scanning AssemblyLine-extracted child artifacts as separate service jobs.

## User-Facing Behavior

- The service receives one AssemblyLine submission, stages it in a unique
  per-job source directory, and scans that directory with a direct `eyeon parse`
  invocation in the service image.
- The service returns a visible, nonfatal result for unsupported files,
  scanner failures, partial output, or resource-limit rejection so other
  AssemblyLine services can continue.
- A successful result includes identity/type/count summary fields and one
  deterministic archive attachment containing all generated observation JSON.
- Service diagnostics identify the pinned EyeON version and actionable status
  without exposing worker host paths, credentials, or unbounded scanner output.

## Acceptance Criteria

- The new `assemblyline-service-eyeon` repository implements an AssemblyLine 4
  service using the selected AL4 SDK and conventions.
- Its service image embeds a pinned EyeON release and invokes `eyeon parse`
  directly without nested container runtime access.
- The service scans one submitted file per job in isolated workspaces.
- Output collection produces exactly one deterministic archive containing all
  generated JSON files, or a visible nonfatal error result.
- The result exposes submission hashes/identity, detected file types,
  observation/metadata counts, EyeON version, and scan status.
- Configurable defaults bound input size, scan duration, threads, output size,
  and generated observation count.
- Scanner failures, timeout, partial output, and resource-limit rejection are
  explicit visible results, not silent or partial successes.
- Tests cover argument construction, output collection, error translation, and
  the selected plugin/service contract.
- An integration validation path is documented for a real AssemblyLine worker
  and EyeON image.

## Affected Areas

- New standalone `assemblyline-service-eyeon` repository: service source,
  Dockerfile, AssemblyLine configuration, fixtures, tests, and CI.
- This repository: canonical integration documentation after release.
- `../pEyeON`: read-only source of the pinned EyeON version; no change is
  currently proposed.

## References

- [[wiki/pipeline/eyeon_parse_sh]]
- [[wiki/component/parse]]
- [[wiki/component/container]]
- [[wiki/decision/two_repo_split]]
- [[wiki/work/assemblyline-eyeon-parse-plugin/references]]

## Open Questions

- Which supported AssemblyLine 4 release and service SDK version will be pinned
  in the new repository?
- Which tested EyeON release/image digest is the first pinned service-image
  dependency?
- Resource defaults are approved: 1 GiB input, 15-minute wall-clock scan, two
  EyeON threads, 512 MiB compressed result archive, and 10,000 generated JSON
  files per job.
- What AssemblyLine classification and retention policy applies to the raw JSON
  archive and service logs? Deferred pending deployment policy.
- Should EyeON-extracted child observations be summarized separately in the
  first result, or only included in the raw archive/counts?

## Test Plan

- Unit-test configuration validation, direct `eyeon parse` argument generation,
  output discovery/archive creation, summary derivation, and error translation
  with a mocked process runner.
- Contract-test the selected AssemblyLine 4 service interface using its
  supported test harness.
- Run an opt-in service-container integration test against a pinned EyeON build
  and representative fixtures.
- Run a deployment smoke test in a real AssemblyLine 4 worker before release.

## Done When

- The Architect approves a complete developer handoff resolving the remaining
  version, limit, and data-handling questions.
- The Developer implements and verifies the approved service in its owning
  repository and records the audit trail.
- Durable integration behavior is promoted into canonical wiki documentation.
