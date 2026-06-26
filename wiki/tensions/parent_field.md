---
title: "Tension: parent field is undefined"
type: tension
status: open
poles:
  - "parent field exists in the schema with description 'some yet-to-be-determined pointer to a parent file or something'"
  - "No implementation sets this field; its semantics are unresolved"
resolution: null
confidence: high
grounded_by:
  - ../pEyeON/schema/observation.schema.json
policy: agent-editable
component: pEyeON-core
last_validated: 2026-06-26
tags: [schema, parent, firmware, archive]
---

# Tension: parent field is undefined

## The Conflict

<!-- GROUND_TRUTH: ../pEyeON/schema/observation.schema.json §parent -->
The observation schema includes a `parent` field typed as `string` with the
description: "Some yet-to-be-determined pointer to a parent file or something."

This field is not populated by the current `Observe` implementation and has no
defined semantics.

## Why It Matters

For firmware analysis in particular, the parent relationship is critical:
a `.so` file extracted from a firmware image has a fundamentally different
risk profile depending on which firmware it came from. Without a parent
pointer, there is no way to reconstruct the containment hierarchy in the
database.

This also affects archive analysis (see [[wiki/tensions/archive_recursion]]):
if archives were recursively scanned, each inner file would need a `parent`
reference to its container.

## Candidate Resolutions

1. **UUID reference** — `parent` is the UUID of the containing file's observation
2. **Path reference** — `parent` is the original archive/firmware path on disk
3. **Both** — a structured object with both UUID and path
4. **Defer** — leave undefined until recursive scanning is implemented

## Status

Open. This should be a deliberate design decision before recursive scanning
or firmware analysis is implemented.
