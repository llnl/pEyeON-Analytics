---
title: "File Format: PE (Portable Executable)"
type: file_format
confidence: high
grounded_by:
  - ../pEyeON/schema/observation.schema.json
  - ../pEyeON/src/eyeon/observe.py
  - ../pEyeON-Analytics/dbt_eyeon_gold/models/staging/stg_metadata_pe_file.sql
policy: agent-editable
last_validated: 2026-06-26
repo_scope: cross-repo
implementation_area: scanner
format_domain: executable
audience: mixed
status: reviewed
source_paths: wiki/file_format/pe.md
tags: [pe, windows, imphash, authenticode, supply-chain]
---

# File Format: PE (Portable Executable)

## Filetype Enum Values

`PE`, `Malformed PE`, `DOS`

## What EyeON Extracts

### Base observation fields (all files)
`md5`, `sha1`, `sha256`, `ssdeep`, `magic`, `filetype`, `bytecount`, `uuid`

### PE-specific fields
<!-- GROUND_TRUTH: ../pEyeON/schema/observation.schema.json §PEMetadata -->

| Field | Required | Description |
|-------|----------|-------------|
| `peMachine` | **yes** | Machine type string |
| `peOperatingSystemVersion` | no | Min OS version |
| `peSubsystemVersion` | no | Subsystem version |
| `peSubsystem` | no | Subsystem (GUI, console, etc.) |
| `peLinkerVersion` | no | Linker version string |
| `peImport` | no | Array of imported DLL/function names |
| `peIsExe` | no | Boolean |
| `peIsDll` | no | Boolean |
| `peIsClr` | no | Boolean (.NET managed) |
| `FileInfo` | no | Windows version resource properties |
| `dllRedirectionLocal` | no | Boolean |
| `OS` | no | Target OS string |

### Signature / certificate fields (PE with signatures)
<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/observe.py §set_signatures -->

| Field | Description |
|-------|-------------|
| `imphash` | Import hash (pefile); Mandiant tracking technique |
| `authentihash` | Hash of code section per PE signature algorithm |
| `authenticode_integrity` | LIEF `verify_signature()` result string |
| `signatures[].certs[]` | Full certificate chain (issuer, subject, key size, expiry, etc.) |
| `signatures[].signers` | Signer string |
| `signatures[].digest_algorithm` | Hash algorithm used |
| `signatures[].verification` | Per-signature verification flags |
| `signatures[].sha1` | Content info digest |

Certificate SHA256 is computed over raw DER bytes. Issuer linkage is built by
matching `issuer_name` → `subject_name` (case-insensitive per RFC 5280 §7.1),
resulting in `issuer_sha256` on each cert.

## Key Identifiers

- Magic bytes: `MZ` header
- imphash enables clustering of binaries with identical import patterns
- ssdeep enables similarity matching against known-malicious samples

## dbt Staging Model

<!-- GROUND_TRUTH: ../pEyeON-Analytics/dbt_eyeon_gold/models/staging/stg_metadata_pe_file.sql -->
`stg_metadata_pe_file` is a thin pass-through view of `silver.metadata_pe_file`.
No transformations currently applied at staging. Gold-level PE analysis happens
in the cert marts (see [[wiki/pipeline/cert_analysis]]).

## Supply Chain Relevance

PE files are the dominant binary format in Windows-based OT/ICS environments
(HMIs, engineering workstations, SCADA servers). Key threat signals:
- Unsigned binaries or broken Authenticode chains
- Import patterns matching known malware families (imphash clustering)
- .NET CLR flag (`peIsClr`) — managed code; different attack surface
- Expired or self-signed certificates in `signatures`

## Known Gaps

- `peImport` is a flat array of strings — DLL name and function name are not
  separated, making structured import analysis harder
- `FileInfo` is typed as `object` in the schema with no defined sub-fields
- `Malformed PE` detection exists but malformation type is not recorded
- No exports extracted (only imports)

## Related

- [[wiki/component/observe]] — extraction implementation
- [[wiki/schema/observation_schema]] — full schema
- [[wiki/concept/authenticode]] — signature validation background
- [[wiki/concept/fuzzy_hashing]] — imphash and ssdeep
