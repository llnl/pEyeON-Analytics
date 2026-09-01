---
title: "Feature References: AssemblyLine Plugin Using eyeon-parse.sh"
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
audience: developer
status: draft
source_paths: wiki/work/assemblyline-eyeon-parse-plugin/references.md
tags: [feature-work, assemblyline, references, wrapper]
---

# Feature References: AssemblyLine Plugin Using eyeon-parse.sh

## Live Repo Sources

- `eyeon-parse.sh`: current host wrapper contract. It requires a nonempty source
  directory, writes `<timestamp>_<UTIL_CD>` beneath the dataset path, and runs
  either the EyeON container or a VM-local `eyeon` command.
- `wiki/pipeline/eyeon_parse_sh.md`: canonical wrapper behavior and runtime
  notes.
- `../pEyeON/README.md`: scanner container and wrapper usage once the target
  repository and revision are confirmed.

## External Sources

- AssemblyLine 4 service-development documentation for the selected pinned
  AssemblyLine release.
- AssemblyLine 4 service SDK/API reference for the selected pinned SDK version.

The service will live in a new standalone `assemblyline-service-eyeon`
repository. Add its live repository path and the version-pinned upstream URLs
before implementation begins.

## Related Wiki Pages

- [[wiki/pipeline/eyeon_parse_sh]]
- [[wiki/component/parse]]
- [[wiki/component/container]]
- [[wiki/overview/architecture]]

## Libraries And APIs

Use the AssemblyLine 4 service SDK version compatible with the selected service
deployment. Pin it in the standalone service repository; do not add it to this
analytics repository.

## Notes

`eyeon-parse.sh` is designed to create and mount host paths, select Docker or
Podman, and allocate a terminal when interactive. The approved direction avoids
those worker-incompatible semantics: a dedicated AssemblyLine service image
will contain a pinned EyeON release and invoke `eyeon parse` directly. The
service must preserve the relevant wrapper guarantees without copying host
runtime selection or ownership behavior into the service.
