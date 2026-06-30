---
title: "Tension: Archive Recursion — scan into archives or not?"
type: tension
status: resolved
poles:
  - "Archives should only be detected as files to avoid hidden recursion, extraction costs, and path traversal risks."
  - "Archives should be extracted and their children should become normal EyeON observations so nested software is visible."
resolution: "Resolved for the current supported container set: pEyeON now emits a container metadata object, safely extracts ZIP/TAR/GZIP/BZIP2/XZ, Docker tar/gzip, RAR with external tooling, and ISO via 7-Zip-compatible tooling during parse, recursively observes extracted children, and sets each child `parent` to the container observation UUID. Unsupported container-like formats remain follow-up work."
confidence: high
grounded_by:
  - ../pEyeON/src/eyeon/container.py
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/src/eyeon/observe.py
  - ../pEyeON/schema/observation.schema.json
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [archives, recursion, firmware]
---

# Tension: Archive Recursion — scan into archives or not?

## Resolution

Core `pEyeON` now implements the first archive recursion slice. During parse,
supported containers are observed as files, enriched with
`metadata.container_file`, extracted to temporary directories, and their
extracted child files are observed recursively. Child observations set `parent`
to the container observation UUID.

Supported extraction formats are `ZIP`, `TAR`, `GZIP`, `BZIP2`, `XZ`,
`DOCKER_TAR`, `DOCKER_GZIP`, `RAR`, and `ISO_9660_CD`. `GZIP`, `BZIP2`, and `XZ`
support both compressed tar archives and single compressed files. Docker archives
are treated as tar-like containers, so layer tars can be recursively observed.
RAR and ISO extraction are conditional on external tooling.

## Remaining Gaps

- `MACOS_DMG`, `ZLIB`, `CPIO_*`, `ZSTANDARD`, and Java/app ZIP-like packages are
  still detected but not extracted by this pattern.
- RAR and ISO extraction can fail on systems without the required external tools.
- Binwalk firmware extraction remains separate follow-up work.
- Analytics models still need to consume `parent` and `metadata.container_file`.
