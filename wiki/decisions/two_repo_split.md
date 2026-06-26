---
title: "Decision: Two-Repo Split (core vs analytics)"
type: decision
status: accepted
decided_on: 2026-06-26
confidence: high
grounded_by:
  - ../pEyeON/README.md
  - ../pEyeON-Analytics/README.md
policy: human-review-required
component: both
last_validated: 2026-06-26
tags: [architecture, repos, split]
---

# Decision: Two-Repo Split (core vs analytics)

## Context

EyeON needs to support field collection on user machines while also enabling
deeper reporting and analysis over collected metadata. The core README presents
`pEyeON` as the CLI collection tool and points users to the companion
`pEyeON-Analytics` project for deeper analysis and reporting workflows.

<!-- GROUND_TRUTH: ../pEyeON/README.md §top -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §analytics -->

## Decision

Keep scanner collection and analytics in separate repositories:

| Repository | Responsibility |
| --- | --- |
| `pEyeON` | Collection, parsing, containerized CLI, optional Box upload, JSON result generation |
| `pEyeON-Analytics` | Database-backed analysis, loading, transformations, reporting, dashboards |

## Rationale

The core README states that `pEyeON` focuses on collection, parsing, and optional
Box upload of EyeON results, while database-backed analysis and dashboard
workflows live in `pEyeON-Analytics`. This separation keeps the field scanner
workflow centered on the CLI/container path and leaves heavier analysis workflows
to the companion project.

<!-- GROUND_TRUTH: ../pEyeON/README.md §analytics -->

## Consequences

Users can run normal field collection without cloning the core repository or
installing analytics dependencies: the quickstart downloads wrapper scripts,
pulls the published container, runs a batch parse, and summarizes the newest
batch. Users who need richer analysis move the collected metadata into
`pEyeON-Analytics`.

<!-- GROUND_TRUTH: ../pEyeON/README.md §quickstart -->

## Alternatives Considered

An all-in-one repository would put scanner, loader, transformations, and UI in
one project. The current README rejects that shape in practice by making the core
repository collection-focused and linking out to the analytics companion for
reporting.
