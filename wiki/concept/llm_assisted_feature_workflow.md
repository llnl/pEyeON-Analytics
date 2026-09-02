---
title: "Concept: LLM-Assisted Feature Workflow"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
last_validated: 2026-08-27
repo_scope: cross-repo
implementation_area: dev-environment
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/concept/llm_assisted_feature_workflow.md
tags: [workflow, llm, requirements, design, verification, roles, interview, metrics]
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
| Interview | Architect and Engineer establish shared context and flesh out the idea through adaptive Q&A | `interview.md` (optional; distilled into `brief.md`) |
| Define functionality | Problem, goals, non-goals, behavior, acceptance criteria, test ideas | `brief.md` |
| Gather context | Repo references, specs, related pages, prior decisions | `references.md` |
| Design | Proposed approach, alternatives, risks, edge cases | `design.md` |
| Spike | Small proof-of-concept when uncertainty is high | `spike.md` |
| Plan implementation | Steps + verification checklist | `implementation_plan.md` |
| Hand off to the Developer | Architect-approved instruction + curated context | `dev_handoff.md` |
| Implement and verify | Code changes + commands run + full results | `verification.md` |
| Close out | Promote durable facts, record plain-language Velocity Results when enabled, and log follow-ups | Updated canonical pages, optional `metrics.md`, and `wiki/log.md` |

## Interview Stage

The interview is an interactive question-and-answer session between the
Architect and the Engineer at the start of feature creation. Its purpose is to
establish the shared context needed to flesh out the idea *before* drafting
`brief.md`, so the brief reflects actual intent rather than the Engineer's
first guess.

### Protocol

1. **Ground first.** Before asking anything, the Engineer reads
   `wiki/index.md` and skims pages related to the stated idea, and
   spot-checks relevant live repo paths (this repo and `../pEyeON`). Never
   ask the Architect something the wiki or source code already answers —
   cite it instead and ask only for confirmation if uncertain.
2. **Ask in small batches.** Ask 2–4 questions at a time, highest-leverage
   first. Prefer concrete options with a stated recommendation over
   open-ended prompts, but always allow free-form answers.
3. **Adapt.** Each batch is shaped by prior answers. Stop lines of
   questioning that the Architect has resolved; open new ones that their
   answers surface.
4. **Distinguish answer types.** Record each resolved item as one of:
   - *Decision* — the Architect chose; goes into `brief.md` (and later
     `decision/` pages if durable).
   - *Constraint* — a hard boundary (compatibility, platform, schema).
   - *Delegated* — the Architect explicitly leaves it to the Engineer or
     Developer; record the delegation, don't silently assume it.
   - *Deferred* — genuinely unknown; goes to `## Open Questions` in
     `brief.md`, optionally spawning a spike.
5. **Know when to stop.** The interview is done when the Engineer can state
   the problem, goals, non-goals, acceptance criteria, and affected areas
   without guessing — typically 2–4 batches. It is a conversation, not a
   form: end early if the idea is already well specified, and say so.
6. **Play back before writing.** Close with a short summary of what was
   decided, constrained, delegated, and deferred, and get the Architect's
   confirmation before generating `brief.md`.
7. **Ask the two sealed estimate questions last.** After playback, ask
   exactly two fixed metrics questions: (a) the forced counterfactual — if
   the Architect had to build this exact scope alone, without AI, how many
   working hours would it take, plus a realistic calendar availability date
   (the hours are the feature's solo-hours: the Velocity numerator and
   portfolio weight); (b) with the AI workflow, on what date does the
   Architect predict the feature will be available. Record the answers
   verbatim in the `## Sealed — human estimates` section of `interview.md`.
   These are part of the hard three-question metrics budget in
   [[wiki/decision/2026-08-27-adopt-velocity-mini-lab]] (metric definition in
   [[wiki/concept/velocity-metric]]); if the Architect skips them, move on —
   never re-ask.

### Question Areas

Draw from these areas as relevant; skip any already answered by context:

- **Problem and motivation** — what hurts today, who feels it, what
  triggered the idea now.
- **Scope boundary** — smallest useful version vs. full vision; explicit
  non-goals.
- **Affected repos and layers** — scanner extraction (`../pEyeON`),
  observation schema, DLT bronze→silver load, dbt gold models, Streamlit
  app, CLI/container/wrapper, analysis notebooks.
- **Metadata semantics** — whether scanner extraction behavior, the
  observation JSON schema's meaning, DLT-loaded silver semantics, dbt-derived
  gold semantics, or Streamlit-computed presentation changes.
- **Compatibility** — schema changes, existing loaded DuckDB datasets and
  scan corpora, cross-repo consumers, container/VM builds.
- **Verification** — how the Architect will know it works; what evidence
  closes the feature.
- **Uncertainty** — which parts are risky enough to warrant a spike before
  design.

### Artifact

Capturing the interview in `interview.md` is optional. Use it when the
interview surfaced non-obvious rationale worth preserving for the Developer
or future reflection; skip it when the brief captures everything that
mattered. The brief is always the distilled product — `interview.md` is
supporting context, never a second source of truth. See
[[wiki/concept/feature_work_template]] for the skeleton. The
`## Sealed — human estimates` section is the exception: when the metrics
overlay is in play, create `interview.md` at least for that section, since
the seal (see [[wiki/decision/2026-08-27-adopt-velocity-mini-lab]]) depends
on the answers living in a marked section that estimating agents can avoid
reading.

## Lightweight Variant

When a feature emerges from a live working session — typically a diagnostic
or design conversation in which the Architect and Engineer have already
built the shared context the interview stage exists to create — the workflow
may run in a **lightweight** form (first used by
[[wiki/work/dlt-state-consistency/brief]], 2026-08-31):

- The interview is the conversation itself; no `interview.md` is kept. The
  brief must say so and capture the decisions/constraints that emerged.
- The Velocity metrics overlay is skipped (no sealed estimates).
- The artifact set is `brief.md` + `implementation_plan.md` +
  `dev_handoff.md` + `verification.md`.
- Architect approval of the handoff may be given in-session ("Proceed");
  the handoff records the approval verbatim with its date. Implementation
  may then continue in the same session — the Developer still implements
  from the written handoff, and `verification.md` remains the audit
  artifact.

Use it only when the Architect explicitly invokes it and the design is
settled in-session; anything with open architectural questions takes the
full workflow.

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

Expected behavior: run the interview stage first (ground in existing wiki and
repo context, then ask adaptive question batches until the idea is fleshed
out), play back the resolved context for confirmation, ask the two sealed
estimate questions, then create a feature folder under
`wiki/work/<feature-slug>/` with the artifacts that add value for the feature
(see [[wiki/concept/feature_work_template]] — only `brief.md` is always
required), then update `wiki/index.md` and append an entry to `wiki/log.md`.

To skip the interview (e.g. the idea is already fully specified, or the
Architect is pasting in a prepared brief), say so explicitly:

```text
Start a new feature using the LLM-assisted feature workflow (no interview): <feature name>
```

## Related

- [[wiki/decision/feature_work_artifacts]] - accepted artifact organization
- [[wiki/concept/feature_work_template]] - reusable skeleton templates
- [[wiki/decision/2026-08-27-adopt-velocity-mini-lab]] - metrics overlay protocol (interview sealed questions, close-out duties)
- [[wiki/concept/velocity-metric]] - the Velocity metric definition
- [[wiki/concept/metrics-template]] - per-feature `metrics.md` format
- [[wiki/metrics]] - cross-feature Velocity rollup
- [[wiki/work/update-loreforge/brief]] - the feature that adopted this version of the workflow
