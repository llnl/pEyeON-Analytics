---
title: "File Format: Archives"
type: file_format
confidence: high
grounded_by:
  - ../pEyeON/.venv/lib/python3.14/site-packages/surfactant/filetypeid/id_magic.py
  - ../pEyeON/src/eyeon/container.py
  - ../pEyeON/src/eyeon/parse.py
  - ../pEyeON/src/eyeon/observe.py
  - ../pEyeON/schema/observation.schema.json
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [zip, tar, gzip, archives]
---

# File Format: Archives

## What EyeON Detects

The observation schema and Surfactant magic detector cover these archive or
container-like filetypes:

- `DOCKER_GZIP`, `GZIP`, `BZIP2`, `XZ`, `DOCKER_TAR`, `TAR`, `RAR`, `ZIP`
- Java/app ZIP-family containers: `JAR`, `WAR`, `EAR`, `APK`, `IPA`, `MSIX`
- Compression/container signatures: `ZLIB`, `CPIO_BIN big`, `CPIO_BIN little`,
  `CPIO_ASCII_OLD`, `CPIO_ASCII_NEW`, `CPIO_ASCII_NEW_CRC`, `ZSTANDARD`,
  `ZSTANDARD_DICTIONARY`, `ISO_9660_CD`, `MACOS_DMG`

## Extracted During Parse

The current core extraction path supports:

- `ZIP`
- `TAR`
- `GZIP`
- `BZIP2`
- `XZ`
- `DOCKER_TAR`
- `DOCKER_GZIP`
- `RAR`, when the `rarfile` backend can use an installed external tool such as `unrar` or `unar`
- `ISO_9660_CD`, when `7zz` or `7z` is available, or `EYEON_7Z_PATH` points to a compatible executable

For these formats, `eyeon parse` emits an observation for the container itself,
adds `metadata.container_file`, extracts children to a temporary directory, and
emits normal child observations with `parent` set to the container observation
UUID. Docker archives are treated as tar-like containers; their layer tar files
can then be recursively extracted by the same pattern.

## Container Metadata

Container observations include `metadata.container_file` with fields such as:

- `formats`
- `extractable`
- `extracted`
- `extraction_status`
- `child_count`
- `extracted_child_count`
- `members`
- `errors`

## Known Gaps

- ZIP-family application formats (`JAR`, `WAR`, `EAR`, `APK`, `IPA`, `MSIX`) are
  still routed as their own filetypes and are not extracted by this first slice.
- `MACOS_DMG`, `ZLIB`, `CPIO_*`, and `ZSTANDARD` are detected but not extracted.
- `ISO_9660_CD` extraction depends on an external 7-Zip-compatible executable.
- `RAR` extraction depends on external RAR tooling available through `rarfile`.
- Firmware-specific embedded filesystem extraction remains Binwalk follow-up
  work.
