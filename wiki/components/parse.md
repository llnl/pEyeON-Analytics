---
title: "Component: Parse"
type: component
confidence: high
grounded_by:
  - ../pEyeON/README.md
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/src/eyeon/container.py
  - ../pEyeON/schema/observation.schema.json
policy: agent-editable
component: pEyeON-core
last_validated: 2026-08-03
tags: [parse, directory, batch]
---

# Component: Parse

## Purpose

`parse.py` performs directory-level scanning by calling `observe` recursively and
returning an observation for each file in a directory. It is the batch-scale
counterpart to single-file `Observe`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §parse -->

## CLI Use

The core CLI exposes parse help through:

```bash
eyeon parse --help
```

For normal containerized batch use, the README recommends `eyeon-parse.sh`,
which creates a timestamped batch directory and runs `eyeon parse` in the
container.

<!-- GROUND_TRUTH: ../pEyeON/README.md §core-cli -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §eyeon-parse.sh -->

## Library Use

The README shows direct library usage as:

```python
obs = eyeon.parse.Parse(args.dir)
```

<!-- GROUND_TRUTH: ../pEyeON/README.md §parse -->

## Container Extraction

`Parse` now owns the first container extraction pattern. For supported container
filetypes, it observes the container, extracts children into a temporary
directory, observes extracted children recursively, and writes child observations
with `parent` set to the container observation UUID.

Supported extraction formats are `ZIP`, `TAR`, `GZIP`, `BZIP2`, `XZ`,
`DOCKER_TAR`, `DOCKER_GZIP`, `RAR`, and `ISO_9660_CD`. RAR support depends on
external RAR tooling usable by `rarfile`; ISO support depends on `7zz`, `7z`, or
`EYEON_7Z_PATH`. The container observation receives
`metadata.container_file` so the outer container has its own metadata in addition
to child observations.

## Multiprocessing

When `threads > 1`, `Parse` uses a Python multiprocessing pool and a daemon
monitor thread that records active worker PIDs and file paths in a shared manager
dictionary. The pool uses the `spawn` start method by default and recycles worker
processes after each file with `maxtasksperchild=1`; this avoids inherited native
library/plugin state from the default `fork` behavior when parsing large Mach-O
binaries concurrently. The start method can be overridden with
`EYEON_MULTIPROCESS_START_METHOD` for diagnostics.

Files at or above `EYEON_SERIAL_LARGE_FILE_BYTES` are processed serially even
when `threads > 1`; the default threshold is 50 MiB. This avoids observed
multiprocessing tail hangs on large Mach-O binaries while preserving concurrent
processing for smaller files. Set `EYEON_SERIAL_LARGE_FILE_BYTES=0` to disable
the split for diagnostics.

<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/parse.py §__call__ -->

## Optional Upload

The core CLI can parse, compress, and upload results to Box in a single command:

```bash
eyeon parse <dir> --upload
```

This connects `parse` to the optional Box-sharing workflow documented in
[[wiki/components/box_integration]].

<!-- GROUND_TRUTH: ../pEyeON/README.md §checksum-check -->

## Related

- [[wiki/components/observe]] — single-file observation
- [[wiki/pipeline/eyeon_parse_sh]] — recommended container wrapper
- [[wiki/components/box_integration]] — upload workflow
