---
title: "Feature Design: Firmware Corpus"
type: concept
confidence: medium
grounded_by:
  - raw/binwalk/binwalk.md
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [feature-work, design, firmware, corpus]
---

# Feature Design: Firmware Corpus

## Summary

Create a curated firmware corpus workflow that starts with a manifest of sample
pointers, then adds a utility to fetch selected samples and parse them with
EyeON. The corpus should serve three use cases: Binwalk development fixtures,
new-user demos, and repeatable show-and-tell stories over known firmware.

## Proposed Approach

Use a manifest-driven design:

- A source-controlled manifest lists candidate firmware entries and metadata.
- A fetch utility reads the manifest and downloads selected entries to a local corpus cache.
- The utility verifies checksums when known and records fetch status.
- Listing shows whether each entry has downloadable artifacts and how many artifacts are fetched versus referenced.
- Fetching logs each entry, each artifact fetched or cached, and each skipped manual entry.
- A parse command or helper invokes EyeON against the local corpus cache or selected entries.
- Tests use a small approved subset and skip network/large corpus cases by default.

## Candidate Manifest Fields

| Field | Purpose |
| --- | --- |
| `id` | Stable short identifier, such as `openwrt-ath79-tiny` |
| `name` | Human-readable firmware/sample name |
| `category` | `vulnerable-demo`, `vendor-real-world`, `open-source-baseline`, or `research-dataset` |
| `source_url` | Direct download URL or landing page |
| `source_type` | `direct-download`, `vendor-page`, `git-repo`, `archive`, or `dataset-index` |
| `license_or_terms` | Redistribution/use notes |
| `redistributable` | `yes`, `no`, `unknown`, or `user-must-download` |
| `expected_size` | Optional byte size or rough size class |
| `sha256` | Optional checksum for verified fetches |
| `expected_binwalk` | Expected high-level Binwalk signatures, if known |
| `use_cases` | `unit-test`, `integration-test`, `demo`, `spike`, or `research` |
| `notes` | Caveats, CVE story, or manual download instructions |

Entries may also include an `artifacts` list for sources that contain multiple
files or require a path inside a source repository. Each artifact records a
stable artifact `id`, `artifact_type`, repository-relative `path`, local
`filename`, direct artifact `source_url`, optional `sha256`, `fetch` boolean,
and notes. This lets a `git-repo` entry point at one or more specific firmware
binaries while also referencing non-fetched license or metadata files.

## Utility Behavior

Draft command concepts:

```bash
# List curated corpus entries
eyeon-corpus list

# Fetch one entry by ID
eyeon-corpus fetch openwrt-example --dest "$HOME/data/eyeon-corpus"

# Fetch a named subset
eyeon-corpus fetch --subset binwalk-smoke --dest "$HOME/data/eyeon-corpus"

# Parse fetched entries with EyeON
eyeon-corpus parse --subset binwalk-smoke --dataset-path "$HOME/data/eyeon"
```

The command name and location are not final. The implementation may also be a
Python module, development script, or task runner entry before becoming a stable
user-facing command.

## Test Subsets

| Subset | Intended Use | Requirements |
| --- | --- | --- |
| `unit` | Fast automated tests | Small, redistributable, deterministic, no network by default |
| `binwalk-smoke` | Binwalk integration tests | At least one extractable embedded file, bounded runtime |
| `demo` | User-facing examples | Clear story, useful output, legal download path |
| `research` | Larger analysis | May be opt-in, networked, or manually downloaded |

The expanded candidate catalog in [[wiki/work/firmware-corpus/candidates]]
suggests concrete starting subsets: `unit-small`, `binwalk-smoke`,
`demo-vulnerable`, `demo-baseline`, and `bulk-index`.

## Risks

- Firmware redistribution terms may prohibit committing samples.
- Vendor URLs may change or disappear.
- Large firmware files can slow tests and inflate local caches.
- Network-dependent tests can become flaky.
- Some samples may contain sensitive, vulnerable, or dual-use content requiring careful framing.

## Open Questions

- Where should the manifest live once implementation begins?
- Should the fetch utility live in `pEyeON`, `pEyeON-Analytics`, or both?
- Should parsing use `eyeon-parse.sh`, direct `eyeon parse`, or both?
- What is the maximum acceptable committed fixture size?
- Is a small synthetic firmware/container fixture preferable for unit tests?
- How should downloaded files and generated EyeON outputs be cached and ignored by git?
