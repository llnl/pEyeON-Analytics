---
title: "Parse Multiprocessing Hang: Implementation Plan"
type: component
confidence: high
grounded_by:
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/tests/testParse.py
policy: agent-editable
last_validated: 2026-08-03
component: pEyeON-core
tags: [parse, multiprocessing, mach-o, hang]
---

# Parse Multiprocessing Hang: Implementation Plan

## Goal

Prevent `eyeon parse -t N` from hanging near completion when multiple large
Mach-O binaries are parsed concurrently, while preserving single-thread parse
behavior.

## Checklist

1. Reproduce and isolate the failure to multiprocessing rather than single-file metadata extraction. (Completed)
2. Switch parse worker creation away from implicit `fork` state inheritance. (Completed)
3. Recycle worker processes after each file to avoid accumulated plugin/native-library state. (Completed)
4. Fix the monitor loop so it sleeps when no workers are active. (Completed)
5. Update parse unit tests for the context-based pool. (Completed)
6. Run targeted tests and a five-file smoke test over the problematic Mach-O binaries. (Completed)
7. Harden container Surfactant database warmup so spawned workers reuse image-global database/source metadata state. (Completed)
8. Serialize files at or above the large-file threshold instead of sending them through the worker pool. (Completed)

## Notes

The direct `eyeon observe` path completed `coder` in roughly 23 seconds, and a
serialized run over the five suspect binaries completed. The failure appeared
only under multiprocessing, where the five-file parse stalled at 4/5 with
`coder` missing from the output directory.

The container images already ran `surfactant plugin update-db --all`, but that
warmup happened as root and did not guarantee runtime workers would use the same
`XDG_DATA_HOME` or avoid ReadTheDocs `database_sources.toml` lookups. The build
now pins Surfactant data/config paths under `/opt/eyeon` and caches
`database_sources.toml` locally before warming databases.

After the first multiprocessing fix and database warmup hardening, rebuilt-image
parses still stalled at 102/104 with only `coder` and `k9s` missing. Since the
same large Mach-O files complete through direct serialized `observe`, parse now
routes files at or above `EYEON_SERIAL_LARGE_FILE_BYTES` through the serial path
after parallel small-file processing. The default threshold is 50 MiB and can be
disabled with `EYEON_SERIAL_LARGE_FILE_BYTES=0` for diagnostics.

## Fragile Mitigations To Revisit

- `PYTHONWARNINGS` and CLI warning filters suppress the exact Surfactant message
  `Possible nested set at position 81`. This is intentionally narrow, but brittle:
  if Surfactant or the EMBA-derived regex database changes the warning text, the
  warning may return. A durable upstream fix would identify and repair or skip
  the specific problematic regex pattern.
- The large-file split is threshold based rather than format/plugin aware. It is
  pragmatic for the observed large Mach-O hang, but it may serialize unrelated
  large files unnecessarily. Future work could use per-plugin isolation,
  supervised futures with timeouts, or parent-owned worker event queues.
- The wrapper and CLI now coordinate `LOGURU_LEVEL`, but this is still a logging
  convention rather than a fully centralized multiprocessing logging design. See
  [[wiki/work/parse-terminal-output/brief]] for a possible Rich/parent-owned
  output redesign.
