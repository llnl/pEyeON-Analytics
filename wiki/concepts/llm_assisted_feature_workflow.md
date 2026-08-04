---
title: "Concept: LLM-Assisted Feature Workflow"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [workflow, llm, requirements, design, testing]
---

# Concept: LLM-Assisted Feature Workflow

## Purpose

The LLM-assisted feature workflow is a lightweight process for moving from an
idea to implemented, tested code while preserving useful context in the wiki. It
is intended to be structured enough for implementation and review, but light
enough that it does not become a heavyweight requirements process.

This workflow combines several familiar practices:

- RFC-style feature proposals for requirements and design.
- ADRs for durable architecture or process decisions.
- Spikes for small proof-of-concept experiments.
- Docs-as-code for storing supporting artifacts in version control.
- Requirements traceability from feature brief to tests and implementation.

## When To Use It

Use this workflow for feature work that is large enough to benefit from written
context before coding. It is especially useful when the change affects schemas,
pipeline behavior, CLI behavior, data models, or user-visible workflows.

Skip the full workflow for small, localized fixes where a direct code change and
test are clearer than a new feature folder.

## Workflow Stages

| Stage | Purpose | Typical Artifact |
| --- | --- | --- |
| Define functionality | Describe the problem, goals, non-goals, behavior, acceptance criteria, and test ideas | `brief.md` |
| Gather context | Collect repo references, specs, related pages, libraries, and prior decisions | `references.md` |
| Design | Describe the proposed approach, alternatives, risks, data changes, and edge cases | `design.md` |
| Spike | Run a small proof-of-concept when uncertainty is high | `spike.md` |
| Plan implementation | Break the change into steps and define verification | `implementation_plan.md` |
| Implement and verify | Make code changes, run tests, and record results | `verification.md` |
| Close out | Promote durable facts to canonical wiki pages and log remaining follow-ups | Updated canonical pages and `wiki/log.md` |

## Operating Rule

Feature work artifacts are scaffolding, not the final source of truth. Once a
feature ships, durable facts should be promoted into canonical wiki pages under
`overview/`, `components/`, `schemas/`, `pipeline/`, `decisions/`, `tensions/`,
or `concepts/`.

## Invocation

Use this phrase to start a new feature skeleton:

```text
Start a new feature using the LLM-assisted feature workflow: <feature name>
```

Expected behavior: create a feature folder under `wiki/work/<feature-slug>/`
with the standard feature-work artifacts, then update `wiki/index.md` and
`wiki/log.md`.

## Related

- [[wiki/decisions/feature_work_artifacts]] - accepted artifact organization
- [[wiki/concepts/feature_work_template]] - reusable skeleton templates
