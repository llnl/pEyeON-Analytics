---
title: "Component: Box Integration"
type: component
confidence: high
grounded_by:
  - ../pEyeON/README.md
policy: agent-editable
component: pEyeON-core
last_validated: 2026-06-26
tags: [box, upload, cloud, sharing]
---

# Component: Box Integration

## Purpose

EyeON can optionally upload results to Box for sharing and storage. The README
states that all handled data is voluntarily submitted by users and stored in the
user's Box account. Users who want to share EyeON results with LLNL are directed
to contact `eyeon@llnl.gov` for setup.

<!-- GROUND_TRUTH: ../pEyeON/README.md §uploading-results -->

## Authentication

Box use starts with local configuration and token generation:

```bash
eyeon box-auth
```

The command guides the user through browser authentication and writes a local
`box_tokens.json` file for later CLI use.

<!-- GROUND_TRUTH: ../pEyeON/README.md §authenticating-with-box -->

## Box Commands

The README documents three direct Box commands:

| Command | Purpose |
| --- | --- |
| `eyeon box-list` | List items in the connected Box folder |
| `eyeon box-delete <file-or-id>` | Delete a file from the connected Box folder |
| `eyeon box-upload <archive>` | Upload a zip, tar, or tar.gz archive |

<!-- GROUND_TRUTH: ../pEyeON/README.md §list-items-in-your-box-folder -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §delete-a-file-from-box -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §upload-results-to-box -->

## Parse Upload Path

EyeON can parse, compress, and upload results in one command:

```bash
eyeon parse <dir> --upload
```

This keeps Box upload optional while preserving a single-command path for users
who have configured Box credentials.

<!-- GROUND_TRUTH: ../pEyeON/README.md §checksum-check -->

## Related

- [[wiki/components/parse]] — optional `--upload` flow
- [[wiki/tensions/box_vs_local]] — open design tension between local-only and Box-backed operation
