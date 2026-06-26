---
title: "Schema: Observation JSON"
type: schema
confidence: high
grounded_by:
  - ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [schema, observation, json, fields]
---

# Schema: Observation JSON

## Purpose

The observation JSON schema defines the structure of metadata records produced by
`eyeon parse` and loaded into the DLT silver layer. Each observation represents a
single file's metadata extracted through the surfactant plugin architecture.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml -->

## Core Tables

### `raw_obs`

The primary observation table with one row per file. Core fields include:

**Identity:**
- `uuid` — Primary key, unique identifier for the observation
- `filename` — File name
- `source_path` — Path relative to the scan root
- `source_file` — Full source path

**File Properties:**
- `bytecount` — File size in bytes
- `modtime` — File modification timestamp
- `permissions` — File permissions string
- `magic` — File magic bytes/type detection
- `observation_ts` — Timestamp when the observation was created
- `eyeon_version` — Version of EyeON that created the observation

**Cryptographic Hashes:**
- `md5` — MD5 hash
- `sha1` — SHA1 hash
- `sha256` — SHA256 hash

**Fuzzy Hashes:**
- `ssdeep` — Context-triggered piecewise hash
- `imphash` — PE import hash (when applicable)
- `telfhash` — TLSH fuzzy hash

**Authenticode (PE Files):**
- `authentihash` — Windows Authenticode hash
- `authenticode_integrity` — LIEF verification status

**Nested Arrays:**
- `filetype` — Array of detected file types (e.g., `["PE", "ELF"]`)
- `signatures` — Array of signature objects (for signed binaries)

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 142-207 -->

### `raw_obs__filetype`

Child table storing the array of detected file types for each observation. Multiple
file types may be detected for a single file (e.g., an ELF binary that is also a
shared library).

**Fields:**
- `value` — The file type enum string
- `_dlt_root_id` — Links back to parent `raw_obs` row
- `_dlt_list_idx` — Array position

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 385-408 -->

### `raw_obs__signatures`

Child table for code signing signatures extracted from binaries (primarily PE files
with Authenticode signatures).

**Fields:**
- `signers` — Comma-separated list of signer names
- `digest_algorithm` — Signature digest algorithm
- `verification` — Verification status (e.g., "OK", "NO_SIGNATURE")
- `sha1` — Signature SHA1 hash
- `certs` — Nested array of X.509 certificates (see below)

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 409-441 -->

### `raw_obs__signatures__certs`

Nested child of signatures containing X.509 certificate details. See
[[wiki/pipeline/cert_analysis]] for the complete field list and downstream processing.

Key fields: `cert_sha256`, `issuer_name`, `subject_name`, `issued_on`, `expires_on`,
`rsa_key_size`, `basic_constraints`, `key_usage`, `ext_key_usage`.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 442-516 -->

## Format-Specific Metadata Tables

EyeON extracts format-specific metadata into separate tables keyed by the observation
UUID. Each metadata table links back to `raw_obs` via the `uuid` field.

### `metadata_pe_file`

Windows PE (Portable Executable) file metadata.

**Core PE Fields:**
- `os` — Operating system target
- `pe_machine` — Target architecture (e.g., "x86", "x64")
- `pe_subsystem` — Subsystem (e.g., "WINDOWS_GUI", "WINDOWS_CUI")
- `pe_is_exe` — Boolean: is executable
- `pe_is_dll` — Boolean: is DLL
- `pe_is_clr` — Boolean: is .NET Common Language Runtime binary

**Version Info:**
- `file_info__company_name`
- `file_info__file_description`
- `file_info__file_version`
- `file_info__product_name`
- `file_info__product_version`
- `file_info__legal_copyright`
- `file_info__original_filename`

**.NET Metadata:**
- `dotnet_flags__ilonly` — IL-only flag
- `dotnet_flags__strongnamesigned` — Strong-name signed flag
- `file_info__assembly_version`

**Child Arrays:**
- `pe_import` — Imported DLL names
- `pe_delay_import` — Delay-loaded DLL names
- `dotnet_assembly` — .NET assembly manifest
- `dotnet_assembly_ref` — .NET assembly references
- `dotnet_impl_map` — P/Invoke mapping

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 212-336 -->

### `metadata_elf_file`

ELF (Executable and Linkable Format) file metadata for Linux/Unix binaries.

**Core ELF Fields:**
- `os` — Operating system target
- `elf_architecture` — Target architecture
- `elf_os_abi` — OS ABI identifier
- `elf_is_exe` — Boolean: is executable
- `elf_is_lib` — Boolean: is shared library
- `elf_gnu_relro` — Boolean: GNU RELRO enabled

**Security Flags:**
- `elf_dynamic_flags` → `df_bind_now` — BIND_NOW flag
- `elf_dynamic_flags1` → `df_1_pie` — Position-independent executable
- `elf_dynamic_flags1` → `df_1_now` — Immediate binding

**Child Arrays:**
- `elf_dependencies` — Shared library dependencies
- `elf_interpreter` — Dynamic linker path
- `elf_runpath` — Runtime library search path
- `elf_note` — ELF note sections (build ID, ABI tags)

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 517-761 -->

### `metadata_native_lib_file`

Native library archive metadata (AR_LIB, OMF_LIB).

**Child Arrays:**
- `native_libraries__contains_library` — Array of library member names

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 762-1001 -->

### `metadata_unknown`

Fallback metadata table for files that could not be parsed into a recognized format.

**Fields:**
- `description` — Human-readable description of why parsing failed

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 341-364 -->

## Supporting Tables

### `batch_info`

Metadata about each parse batch run.

**Fields:**
- `run_ts` — Batch run timestamp
- `utility_id` — User-provided batch identifier (UTIL_CD)
- `source` — Source directory path
- `depth` — Scan depth
- `hostname` — Host where batch was run

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 110-141 -->

### `raw_json`

Raw observation JSON blobs as originally loaded, before normalization.

**Fields:**
- `json` — Full observation JSON
- `uuid` — Observation UUID
- `source_path` — Source path
- `source_file` — Source file

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 50-75 -->

## DLT Infrastructure Tables

- `_dlt_version` — Schema version tracking
- `_dlt_loads` — Load tracking
- `_dlt_pipeline_state` — Pipeline state

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml lines 6-109 -->

## File Type Coverage

The schema supports these file type enums detected via surfactant plugins:

**Executables:**
- PE, Malformed PE, DOS (Windows)
- ELF (Linux/Unix)
- MACHO32, MACHO64, MACHOFAT, MACHOFAT64, EFIFAT (macOS)
- COFF, XCOFF32, XCOFF64, ECOFF (legacy Unix)

**Java/JVM:**
- JAVACLASS, JAR, WAR, EAR, APK, IPA, MSIX

**Archives:**
- ZIP, TAR, GZIP, BZIP2, XZ, RAR, ZSTANDARD, CPIO_*

**Libraries:**
- AR_LIB, OMF_LIB

**Others:**
- OLE, MSCAB, ISCAB (Microsoft formats)
- UIMAGE (U-Boot images)
- RPM Package
- DOCKER_GZIP, DOCKER_TAR

<!-- GROUND_TRUTH: ../pEyeON-Analytics/wiki/index.md §File Formats -->

## Normalization Notes

DLT normalizes nested JSON arrays into separate child tables with link columns:
- `_dlt_root_id` — Links to root observation
- `_dlt_parent_id` — Links to immediate parent
- `_dlt_list_idx` — Array index
- `_dlt_id` — Unique row identifier
- `_dlt_load_id` — Links to the DLT load batch

The `raw_obs` table uses merge write disposition for upsert behavior, while metadata
tables use append disposition.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml §settings -->

## Related

- [[wiki/schemas/silver_layer]] — DLT-loaded silver table structure
- [[wiki/schemas/gold_layer]] — dbt-built gold models
- [[wiki/components/observe]] — observation extraction
- [[wiki/components/surfactant_plugins]] — plugin architecture
