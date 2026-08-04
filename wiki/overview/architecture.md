---
title: "Architecture Overview"
type: overview
confidence: medium
grounded_by:
  - ../pEyeON/README.md
  - ../pEyeON-Analytics/README.md
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [architecture, data-flow, overview]
---

# Architecture Overview

## System Shape

EyeON is split into a core collection repository (`pEyeON`) and a companion
analytics repository (`pEyeON-Analytics`). The core repository focuses on CLI
collection, parsing, optional Box upload, and JSON outputs. The analytics
repository handles deeper database-backed analysis and reporting workflows.

<!-- GROUND_TRUTH: ../pEyeON/README.md §analytics -->

## Collection Path

The recommended field path starts with the published container and wrapper
scripts:

```text
source directory
    -> eyeon-parse.sh
    -> ghcr.io/llnl/peyeon:latest
    -> eyeon parse
    -> timestamped batch directory under dataset root
```

The wrapper mounts the source directory read-only at `/source` and the dataset
root read-write at `/workdir`. Output is written directly back to the host.

<!-- GROUND_TRUTH: ../pEyeON/README.md §quickstart -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §eyeon-parse.sh -->

## Core CLI Units

The core CLI exposes `observe` and `parse` as the main scanner operations.
`observe.py` works on a single file and writes identifying metadata such as
hashes, modification date, certificate info, and format-specific metadata.
`parse.py` expects a folder and recursively calls `observe` for each file.

<!-- GROUND_TRUTH: ../pEyeON/README.md §core-cli -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §observe -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §parse -->

## Analytics Boundary

The README explicitly redirects database-backed analysis and dashboard workflows
to `pEyeON-Analytics`, while keeping `pEyeON` focused on collection, parsing,
and optional Box upload.

<!-- GROUND_TRUTH: ../pEyeON/README.md §analytics -->

## Analytics Path

`pEyeON-Analytics` is a local analytics stack that combines `dlt`, DuckDB, dbt,
and Streamlit. Its normal workflow is: generate an EyeOn JSON batch, launch the
Streamlit app, select one or more batches, click `Load Selected`, and explore
the loaded data.

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §top -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §overview -->

The analytics data flow is:

```text
EyeOn JSON batch -> load_eyeon.py -> bronze.raw_json
                                 -> silver.raw_obs and metadata
silver -> dbt_eyeon_gold -> gold.* -> Streamlit pages
```

<!-- GROUND_TRUTH: ../pEyeON-Analytics/README.md §data-flow -->

## Related

- [[wiki/overview/project]] — project motivation and two-repo structure
- [[wiki/overview/data_flow]] — analytics bronze/silver/gold flow
- [[wiki/pipeline/eyeon_parse_sh]] — wrapper details
- [[wiki/components/observe]] — single-file observation
- [[wiki/components/parse]] — directory-level scanning
- [[wiki/components/box_integration]] — optional result sharing
