---
title: "Component: Observe"
type: component
confidence: high
grounded_by:
  - ../pEyeON/src/eyeon/observe.py
  - ../pEyeON/src/eyeon/generic_metadata.py
  - ../pEyeON/schema/observation.schema.json
policy: agent-editable
last_validated: 2026-06-30
repo_scope: pEyeON
implementation_area: scanner
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/component/observe.md
tags: [observe, hashing, pe, elf, surfactant, lief, ssdeep, generic-metadata]
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
| `metadata` | surfactant plugin hook `extract_file_info`, or EyeON generic fallback metadata when no format plugin produces metadata |

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
the observation. See [[wiki/component/surfactant_plugins]].

## Generic Metadata Fallback

<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/observe.py §_generic_metadata -->
<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/generic_metadata.py §generic_metadata -->
When surfactant does not identify a format-specific metadata extractor, EyeON now
uses an internal generic metadata fallback rather than leaving most firmware
filesystem artifacts as `metadata.Unknown`. This is not implemented as a new
surfactant plugin; it is EyeON-owned behavior delegated from `Observe` to
`src/eyeon/generic_metadata.py`.

The fallback classifies common extracted firmware artifacts into metadata keys
such as:

- `opkg_file` — OpenWrt/opkg package metadata and package scripts such as `.control`, `.list`, `.prerm`, `.postrm`, `.preinst`, `.postinst`, and `.conffiles`
- `text_file` — shell/config/plain text, Lua, ucode, JSON, certificates, rules, and other generic text-like files
- `web_asset` — HTML, JavaScript, CSS, SVG, PNG/GIF/ICO web-interface assets
- `image_file` — generic image files not otherwise classified
- `symlink_file` — symbolic links, including the symlink target
- `linux_kernel_image` — Linux zImage-style kernel images identified from magic output
- `device_tree_file` — device tree blobs (`.dtb`) and magic-identified device tree files
- `generic_file` — remaining binary blobs or unclassified artifacts

If filetype identification itself fails, `Observe` records fallback metadata with
`identify_error` rather than failing the entire observation. This keeps extracted
filesystem edge cases, such as symlink-like `mtab` files, from becoming parse-level
observation errors.

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

- [[wiki/schema/observation_schema]] — full field reference
- [[wiki/file_format/pe]] — PE-specific metadata
- [[wiki/file_format/elf]] — ELF-specific metadata
- [[wiki/component/surfactant_plugins]] — plugin system
