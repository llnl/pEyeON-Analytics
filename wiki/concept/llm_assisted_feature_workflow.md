---
title: "Concept: LLM-Assisted Feature Workflow"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
last_validated: 2026-08-17
repo_scope: cross-repo
implementation_area: dev-environment
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/concept/llm_assisted_feature_workflow.md
tags: [workflow, llm, requirements, design, verification, roles]
---

# Concept: LLM-Assisted Feature Workflow

## Purpose

The LLM-assisted feature workflow is a lightweight process for moving from an
idea to implemented, verified code while preserving useful context in the wiki.
It is intended to be structured enough for implementation and review, but light
enough that it does not become a heavyweight requirements process.

This workflow combines several familiar practices:

- RFC-style feature proposals for requirements and design.
- ADRs for durable architecture or process decisions.
- Spikes for small proof-of-concept experiments.
- Docs-as-code for storing supporting artifacts in version control.
- Requirements traceability from feature brief to tests and implementation.

## Roles

The workflow maps onto the Architect / Engineer / Developer roles defined in
`AGENTS.md`:

- The **Engineer** creates and maintains all artifacts through the handoff
  stage, and promotes durable facts at close-out.
- The **Architect** settles design questions and approves `dev_handoff.md`
  before implementation begins.
- The **Developer** implements from the approved handoff and records the
  audit trail in `verification.md`.

## When To Use It

Use this workflow for feature work that is large enough to benefit from written
context before coding. It is especially useful when the change affects the
observation schema, scanner extraction behavior, DLT/dbt pipeline behavior,
CLI behavior, data models, Streamlit workflows, or anything that spans both
repos (`../pEyeON` and this repo).

Skip the full workflow for small, localized fixes where a direct code change and
test are clearer than a new feature folder.

Long-running research threads (as opposed to bounded features) may also live
under `wiki/work/`; they typically anchor on an `index.md` plus dated snapshot
and handoff pages rather than the full artifact set.

## Workflow Stages

| Stage | Purpose | Typical Artifact |
| --- | --- | --- |
| Define functionality | Problem, goals, non-goals, behavior, acceptance criteria, test ideas | `brief.md` |
| Gather context | Repo references, specs, related pages, prior decisions | `references.md` |
| Design | Proposed approach, alternatives, risks, edge cases | `design.md` |
| Spike | Small proof-of-concept when uncertainty is high | `spike.md` |
| Plan implementation | Steps + verification checklist | `implementation_plan.md` |
| Hand off to the Developer | Architect-approved instruction + curated context | `dev_handoff.md` |
| Implement and verify | Code changes + commands run + full results | `verification.md` |
| Close out | Promote durable facts into canonical pages + log follow-ups | Updated canonical pages and `wiki/log.md` |

## Operating Rule

Feature work artifacts are scaffolding, not the final source of truth. Once a
feature ships, durable facts should be promoted into canonical wiki pages under
`overview/`, `component/`, `schema/`, `data_model/`, `file_format/`,
`pipeline/`, `decision/`, `tension/`, `concept/`, `tool/`, `workflow/`,
`repo/`, or `diagnostic/`. The feature folder can remain as historical context
for review and future reflection.

## Invocation

Use this phrase to start a new feature skeleton:

```text
Start a new feature using the LLM-assisted feature workflow: <feature name>
```

Expected behavior: create a feature folder under `wiki/work/<feature-slug>/`
with the artifacts that add value for the feature (see
[[wiki/concept/feature_work_template]] — only `brief.md` is always required),
then update `wiki/index.md` and append an entry to `wiki/log.md`.

## Related

- [[wiki/decision/feature_work_artifacts]] - accepted artifact organization
- [[wiki/concept/feature_work_template]] - reusable skeleton templates
- [[wiki/work/update-loreforge/brief]] - the feature that adopted this version of the workflow
