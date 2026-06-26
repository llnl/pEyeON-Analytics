---
title: "Component: Observe"
type: component
confidence: high
grounded_by:
  - ../pEyeON/src/eyeon/observe.py
  - ../pEyeON/schema/observation.schema.json
policy: agent-editable
component: pEyeON-core
last_validated: 2026-06-26
tags: [observe, hashing, pe, elf, surfactant, lief, ssdeep]
---

# Component: Observe

## Purpose

The `Observe` class (in `src/eyeon/observe.py`) makes a single-file observation,
producing a JSON document with identifying metadata. It is the atomic unit of
EyeON scanning.

<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/observe.py §__init__ -->

## What It Collects (Always)

| Field | Source |
|-------|--------|
| `uuid` | `uuid4()` — generated fresh per observation |
| `bytecount` | `os.stat().st_size` |
| `filename` | `os.path.basename(file)` |
| `md5`, `sha1`, `sha256` | `hashlib` |
| `ssdeep` | subprocess call to `ssdeep -b` |
| `magic` | `libmagic` via `python-magic` |
| `modtime` | `os.stat().st_mtime` (UTC) |
| `observation_ts` | `datetime.now()` at scan time |
| `permissions` | `oct(os.stat().st_mode)` |
| `eyeon_version` | `importlib.metadata.version("peyeon")` |
| `filetype` | surfactant plugin hook `identify_file_type` |
| `metadata` | surfactant plugin hook `extract_file_info` |

## Format-Conditional Fields

| Field | Condition |
|-------|-----------|
| `imphash` | `PE` in filetype — via `pefile` |
| `signatures` | `PE` in filetype — via `lief` |
| `authentihash` | `PE` in filetype with at least one signature |
| `authenticode_integrity` | `PE` in filetype — LIEF `verify_signature()` |
| `telfhash` | `ELF` in filetype — via `telfhash`; 30s timeout; may be absent if timed out |

## Plugin Architecture

<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/observe.py §set_metadata -->
Format detection and metadata extraction both use pluggy hooks from surfactant.
The two hooks are `identify_file_type` and `extract_file_info`. Each plugin
receives only the kwargs it declares in its signature (filtered via `argnames`).
Plugin errors are caught and recorded in `metadata.error` rather than crashing
the observation. See [[wiki/components/surfactant_plugins]].

## telfhash Threading Note

<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/observe.py §set_telfhash -->
`telfhash` is run in a daemon thread with a 30-second timeout. If it hangs
(which happens on select ELF files), the thread is abandoned and `telfhash`
is simply absent from the output. This is a known reliability issue.

## Output

`write_json(outdir)` produces:
- `<outdir>/<filename>.<md5>.json` — full observation
- `<outdir>/certs/<sha256>.crt` — raw DER bytes for each certificate (PE only)

## Related

- [[wiki/schemas/observation_schema]] — full field reference
- [[wiki/file_formats/pe]] — PE-specific metadata
- [[wiki/file_formats/elf]] — ELF-specific metadata
- [[wiki/components/surfactant_plugins]] — plugin system
