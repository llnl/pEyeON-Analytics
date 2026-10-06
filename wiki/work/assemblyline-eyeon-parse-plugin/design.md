---
title: "Feature Design: AssemblyLine Plugin Using eyeon-parse.sh"
type: concept
confidence: low
grounded_by:
  - eyeon-parse.sh
  - wiki/pipeline/eyeon_parse_sh.md
policy: agent-editable
last_validated: 2026-09-01
repo_scope: cross-repo
implementation_area: scanner
format_domain: cross-domain
audience: developer
status: draft
source_paths: wiki/work/assemblyline-eyeon-parse-plugin/design.md
tags: [feature-work, assemblyline, design, wrapper]
---

# Feature Design: AssemblyLine Plugin Using eyeon-parse.sh

## Summary

Build an AssemblyLine extension that stages a submitted artifact into a
worker-local source directory, runs EyeON through an approved wrapper execution
model, collects the timestamped observation batch, and translates the outcome
into the selected AssemblyLine result contract.

## Proposed Approach

The service is a standard AssemblyLine 4 analysis service in a new standalone
`assemblyline-service-eyeon` repository. Its service image embeds a pinned
EyeON release; it never starts Docker or Podman and never invokes the
host-oriented `eyeon-parse.sh` from a worker. The service should:

1. Receive an AssemblyLine file submission through the selected SDK.
2. Create per-job source and dataset workspaces owned by the service worker.
3. Validate configurable image, timeout, threads, and output-size limits.
4. Invoke `eyeon parse -o <job-output> -t <threads> -v WARNING <job-source>`
   through an argument-array process API, with no shell interpolation.
5. Collect all JSON produced beneath the job output directory and create one
   deterministic compressed archive with stable relative paths.
6. Attach the archive and emit an AssemblyLine result summarizing identity,
   detected file types, observation count, metadata count, EyeON version, and
   status.
7. Emit structured service logs and clean up job workspaces subject to the
   approved retention policy.

## Data Model Or Schema Changes

No EyeON schema change is proposed. The first release does not map detailed
metadata into AssemblyLine tags or sections; it reports only identity/type/count
summary data and preserves all observation JSON in the archive attachment.

## Interfaces And User Experience

The service image pins the EyeON release. Service configuration must expose the
maximum accepted input bytes, wall-clock timeout, EyeON thread count, maximum
archive bytes, and maximum generated JSON files. Approved conservative defaults
are 1 GiB input, 15 minutes, two threads, 512 MiB archive, and 10,000 JSON
files. All limits must be validated before or during collection, and deployment
configuration may lower them.

User-visible results must identify the EyeON version and distinguish scan,
configuration, timeout, resource-limit, and output-collection outcomes.

## Edge Cases

- A submission is not a directory, while the wrapper requires a directory.
- The scanner produces zero, one, or many JSON observations, including
  observations for scanner-extracted children.
- The output batch cannot be located, is incomplete, or exceeds retention caps.
- The pinned EyeON binary is missing or incompatible with the service image.
- User-supplied names contain shell-significant characters.
- Multiple jobs execute concurrently and must never share a dataset workspace.

## Error Handling

Use an argument array or equivalent process API, never shell interpolation.
Capture bounded stdout/stderr and process exit status. Treat validation errors,
runtime failures, timeouts, resource-limit enforcement, partial output, and
output-collection failures as visible nonfatal AssemblyLine results with
diagnostic context that excludes sensitive host paths. Preserve a distinction
between service misconfiguration and an artifact EyeON cannot scan.

## Risks

- The pinned service image can lag scanner fixes or security updates; upgrading
  EyeON requires a tested service release.
- Scanner extraction can expand one submitted file into many observations and
  exceed bounded workspace/output resources.
- Large artifacts can exceed worker disk, time, output, or file-count limits.
- Attachment of raw observations may expose metadata that requires a retention
  and access-control policy.

## Alternatives Considered

1. Run EyeON directly in a dedicated AssemblyLine service image. Selected:
   avoids nested container access and makes scanner versioning part of service
   release management.
2. Invoke `eyeon-parse.sh` unchanged from the service worker. Rejected for the
   first release because it assumes host mounts and Docker/Podman access.
3. Run EyeON externally and have AssemblyLine submit work to a queue/API.
   Deferred: it adds a distributed integration and result-correlation contract.

## Open Questions

- Which AssemblyLine 4 and service SDK versions are supported first?
- Which EyeON release/image digest is pinned in the first service image?
- What classification and retention policy applies to the attached raw JSON
  archive and bounded service diagnostics? Deferred pending deployment policy.
