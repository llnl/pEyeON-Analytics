---
title: "Component: Checksum Verification"
type: component
confidence: high
grounded_by:
  - ../pEyeON/README.md
policy: agent-editable
component: pEyeON-core
last_validated: 2026-06-26
tags: [checksum, md5, sha1, sha256, verification]
---

# Component: Checksum Verification

## Purpose

EyeON can verify a file against a provided checksum using `md5`, `sha1`, or
`sha256`. The capability can be used as a standalone CLI command or attached to
an `observe` run so the result is recorded in the observation output.

<!-- GROUND_TRUTH: ../pEyeON/README.md §checksum-check -->

## Standalone CLI

```bash
eyeon checksum -a [md5,sha1,sha256] <file> <provided_checksum>
```

If no algorithm is specified with `-a` or `--algorithm`, EyeON defaults to
`md5`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §checksum-check -->

## Observe Integration

Checksum verification can be included in `eyeon observe`:

```bash
eyeon observe tests/binaries/Wintap.exe -a sha256 -c <provided_checksum>
```

The output records a `checksum_data` object containing `algorithm`, `expected`,
`actual`, and `verified`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §recorded-result-in-eyeon-output -->

## Related

- [[wiki/components/observe]] — observation output that can include checksum data
- [[wiki/schemas/observation_schema]] — schema-level field documentation
