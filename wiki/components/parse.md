---
title: "Component: Parse"
type: component
confidence: high
grounded_by:
  - ../pEyeON/README.md
policy: agent-editable
component: pEyeON-core
last_validated: 2026-06-26
tags: [parse, directory, batch]
---

# Component: Parse

## Purpose

`parse.py` performs directory-level scanning by calling `observe` recursively and
returning an observation for each file in a directory. It is the batch-scale
counterpart to single-file `Observe`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §parse -->

## CLI Use

The core CLI exposes parse help through:

```bash
eyeon parse --help
```

For normal containerized batch use, the README recommends `eyeon-parse.sh`,
which creates a timestamped batch directory and runs `eyeon parse` in the
container.

<!-- GROUND_TRUTH: ../pEyeON/README.md §core-cli -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §eyeon-parse.sh -->

## Library Use

The README shows direct library usage as:

```python
obs = eyeon.parse.Parse(args.dir)
```

<!-- GROUND_TRUTH: ../pEyeON/README.md §parse -->

## Optional Upload

The core CLI can parse, compress, and upload results to Box in a single command:

```bash
eyeon parse <dir> --upload
```

This connects `parse` to the optional Box-sharing workflow documented in
[[wiki/components/box_integration]].

<!-- GROUND_TRUTH: ../pEyeON/README.md §checksum-check -->

## Related

- [[wiki/components/observe]] — single-file observation
- [[wiki/pipeline/eyeon_parse_sh]] — recommended container wrapper
- [[wiki/components/box_integration]] — upload workflow
