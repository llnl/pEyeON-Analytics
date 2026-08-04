---
title: "EyeON Project Overview"
type: overview
confidence: high
grounded_by:
  - ../pEyeON/README.md
  - ../pEyeON-Analytics/README.md
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [overview, motivation, supply-chain, ot-ics]
---

# EyeON Project Overview

## What It Is

EyeON is an automated CLI tool for collecting software metadata from binaries,
firmware, and installed software. Its primary purpose is supply chain threat
analysis and software inventory for operational technology (OT) environments.

<!-- GROUND_TRUTH: ../pEyeON/README.md §motivation -->
Existing tools perform hash/signature checks to validate software integrity, but
these checks don't capture the information needed to understand supply chain
threats. EyeON provides a consistent, automated process across users to scan OT
software files and generate reports that track software patterns, shedding light
on supply chain risks.

## Two-Repo Structure

| Repo | Purpose |
|------|---------|
| `pEyeON` | CLI tool, scanner, container image, JSON output |
| `pEyeON-Analytics` | DLT load pipeline, dbt models, Streamlit dashboard |

The split is deliberate: the scanner is kept lightweight and deployable in
constrained environments; analytics requires heavier Python dependencies and
is run separately on collected data. See [[wiki/decisions/two_repo_split]].

## Core Workflow

```
files/firmware
    ↓ eyeon-parse.sh (container wrapper)
    ↓ eyeon parse → one JSON per file
batch directory (timestamped)
    ↓ load_eyeon.py (DLT)
DuckDB bronze + silver
    ↓ dbt build
gold reporting tables
    ↓ Streamlit app
exploration + dashboards
```

## Recommended Field Quickstart

<!-- GROUND_TRUTH: ../pEyeON/README.md §quickstart -->
For normal field use, users do not need to clone `pEyeON`. The documented
quickstart downloads `eyeon-parse.sh` and `eyeon-batch-summary.sh`, pulls
`ghcr.io/llnl/peyeon:latest`, runs a batch parse, then summarizes the newest
batch. The site or utility identifier, such as `TESTSITE`, becomes part of the
batch directory name.

## Analytics Quickstart

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §quickstart -->
The analytics quickstart uses Python 3.13, `uv`, Docker or Podman, and a local
directory of files to scan. Users run `uv sync`, copy
`EyeOnData.toml-template` to `EyeOnData.toml`, set `datasets.dataset_path`,
generate a batch with `eyeon-parse.sh`, launch Streamlit with
`uv run streamlit run EyeOnData.py`, then load selected batches from the app.

## Key Identifiers per Observation

Every observation produces:
- Cryptographic hashes: `md5`, `sha1`, `sha256`
- Fuzzy hashes: `ssdeep` (all files), `imphash` (PE), `telfhash` (ELF)
- `uuid` (uuid4, generated at scan time)
- `filetype` array (can be multiple — see [[wiki/tensions/filetype_multi]])
- `magic` string (libmagic output)
- Format-specific `metadata` block

## Future Work

<!-- GROUND_TRUTH: ../pEyeON/README.md §future-work -->
A second phase is planned: a cloud application that anonymizes and summarizes
findings to enable population-level OT security analysis.

## Contact

`eyeon@llnl.gov` — for Box upload setup and data sharing.
