---
title: "Concept: Fuzzy Hashing (ssdeep, imphash, telfhash)"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml
  - ../pEyeON-Analytics/wiki/file_format/pe.md
  - ../pEyeON-Analytics/raw/README.md
policy: agent-editable
last_validated: 2026-06-26
repo_scope: pEyeON
implementation_area: analytics
format_domain: cross-domain
audience: mixed
status: reviewed
source_paths: wiki/concept/fuzzy_hashing.md
tags: [ssdeep, imphash, telfhash, fuzzy-hash]
---

# Concept: Fuzzy Hashing (ssdeep, imphash, telfhash)

## Purpose

Fuzzy hashing provides similarity-based file identification that survives minor binary
changes, unlike cryptographic hashes (MD5, SHA1, SHA256) which produce completely
different outputs for even single-byte modifications. EyeON extracts three types of
fuzzy hashes to support malware clustering and variant detection.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/schemas/eyeon_metadata.schema.yaml §raw_obs -->

## ssdeep (Context-Triggered Piecewise Hash)

**What it is:**
ssdeep computes a rolling hash over the file contents using context-triggered
piecewise hashing. It produces a signature that can be compared against other
signatures to detect similarity even after code obfuscation, recompilation, or
partial modification.

**Stored in:**
`raw_obs.ssdeep` field (text)

**Use cases:**
- Detect malware variants that share code with known samples
- Identify firmware images that differ only in configuration data
- Cluster binaries with similar structure but different embedded resources

**Limitations:**
- Not format-aware — treats all files as byte streams
- Sensitive to large insertions or deletions
- Requires external comparison logic (not indexed for similarity queries in DuckDB)

**References:**
- ssdeep project: https://ssdeep-project.github.io/ssdeep/

<!-- GROUND_TRUTH: ../pEyeON-Analytics/wiki/component/container.md §dependencies -->

## imphash (Import Hash)

**What it is:**
imphash is a hash of the Windows PE import table (imported DLL and function names).
Binaries with identical imports produce identical imphashes, regardless of code
content. Developed by Mandiant for malware tracking.

**Stored in:**
`raw_obs.imphash` field (text)

**Computed for:**
PE files only (Windows executables and DLLs)

**Use cases:**
- Cluster malware families with identical import patterns
- Track malware using the same APIs across variants (e.g., all samples calling
  `CreateRemoteThread`, `WriteProcessMemory`, `VirtualAllocEx`)
- Identify repackaged or slightly modified binaries that preserve their API surface

**Limitations:**
- Does not capture code behavior — only API dependencies
- Easily bypassed by adding decoy imports or dynamic API resolution (GetProcAddress)
- EyeON's `peImport` array does not separate DLL from function names, complicating
  manual import analysis

**References:**
- Mandiant blog post: https://www.mandiant.com/resources/blog/tracking-malware-import-hashing

<!-- GROUND_TRUTH: ../pEyeON-Analytics/wiki/file_format/pe.md §imphash -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/raw/README.md §imphash -->

## telfhash (TLSH for ELF)

**What it is:**
telfhash is a locality-sensitive hash adapted from TLSH (Trend Micro Locality
Sensitive Hash) specifically for ELF binaries. It hashes structural features of
the ELF format to enable similarity matching.

**Stored in:**
`raw_obs.telfhash` field (text)

**Computed for:**
ELF files (Linux/Unix executables and shared libraries)

**Use cases:**
- Detect ELF malware variants in IoT/embedded firmware
- Cluster similar firmware images from different vendors
- Identify recompiled or slightly patched binaries in Linux-based OT/ICS systems

**Limitations:**
- ELF-specific — not applicable to PE or other formats
- Requires external comparison logic (not indexed for similarity queries in DuckDB)

**References:**
- telfhash paper & implementation: https://github.com/trendmicro/telfhash

<!-- GROUND_TRUTH: ../pEyeON-Analytics/raw/README.md §telfhash -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/wiki/file_format/elf.md §telfhash -->

## Gold Layer Usage

All three fuzzy hashes are passed through to the `gold_files` mart unchanged:

```sql
l.ssdeep,
l.telfhash,
l.imphash,
```

Similarity matching and clustering queries must be implemented by consuming
applications (e.g., Streamlit dashboards, external threat intel pipelines).

<!-- GROUND_TRUTH: ../pEyeON-Analytics/dbt_eyeon_gold/models/gold/gold_files.sql -->

## Comparison to Cryptographic Hashes

| Hash Type | Survives Minor Changes | Format-Aware | Use Case |
|-----------|------------------------|--------------|----------|
| **MD5/SHA1/SHA256** | No | No | Exact file deduplication, integrity verification |
| **ssdeep** | Yes | No | Generic binary similarity |
| **imphash** | Yes | Yes (PE imports) | Malware family clustering (Windows) |
| **telfhash** | Yes | Yes (ELF structure) | Firmware similarity (Linux/embedded) |

## Supply Chain Relevance

In OT/ICS software supply chain analysis:
- **imphash** identifies vendor binaries sharing suspicious API patterns (e.g.,
  remote access tools bundled in engineering software)
- **ssdeep** detects firmware variants across different equipment versions
- **telfhash** clusters embedded Linux binaries in industrial IoT devices

## Related

- [[wiki/file_format/pe]] — PE-specific imphash extraction
- [[wiki/file_format/elf]] — ELF-specific telfhash extraction
- [[wiki/schema/observation_schema]] — fuzzy hash storage in `raw_obs`
- [[wiki/concept/supply_chain_risk]] — threat landscape context
