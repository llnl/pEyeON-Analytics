---
title: "Concept: Feature Work Template"
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
source_paths: wiki/concept/feature_work_template.md
tags: [workflow, llm, template, feature-work, interview, metrics]
---

# Concept: Feature Work Template

This page defines the reusable skeletons for starting a new feature with the
[[wiki/concept/llm_assisted_feature_workflow]].

Use this invocation:

```text
Start a new feature using the LLM-assisted feature workflow: <feature name>
```

The expected output is:

```text
wiki/work/<feature-slug>/
  interview.md             (optional; record of the context-building Q&A + sealed estimates)
  brief.md                 (required)
  references.md            (optional)
  design.md                (optional)
  spike.md                 (optional)
  implementation_plan.md   (optional)
  dev_handoff.md           (optional)
  verification.md          (optional)
  metrics.md               (optional; plain-language Velocity results + technical record — see [[wiki/concept/metrics-template]])
  index.md                 (optional; long-running research threads)
```

The interview stage (see [[wiki/concept/llm_assisted_feature_workflow]])
normally runs before any artifact is written; `interview.md` is only created
when its Q&A record adds value beyond what lands in `brief.md` — except that
when the metrics overlay is in play, it is created at least for the
`## Sealed — human estimates` section.

Only create the artifacts that are useful for the feature. For a small feature,
`brief.md` plus `verification.md` may be enough. For a high-uncertainty feature,
include `references.md`, `design.md`, and `spike.md` before implementation. Use
`dev_handoff.md` when the work will be handed to the Developer role in a
separate session. Use `index.md` when the folder is a long-running research
thread with dated snapshots rather than a bounded feature.

## Frontmatter For Work Artifacts

Every file under `wiki/work/` is a wiki page and must carry the standard
frontmatter from `AGENTS.md`. Use `type: concept` for work artifacts unless a
page is specifically a `decision` or `tension` (research-thread `index.md`
pages may use `type: workflow`). Start each artifact from this block, adjusting
`title`, `repo_scope`, `implementation_area`, `format_domain`, `tags`, and
`grounded_by` to fit the feature:

```yaml
---
title: "<Artifact>: <Feature Name>"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
last_validated: YYYY-MM-DD
repo_scope: pEyeON | pEyeON-Analytics | cross-repo
implementation_area: scanner | schema | dlt-pipeline | dbt-gold | streamlit | container | surfactant-plugins | analytics | dev-environment
format_domain: executable | firmware | archive | script | document | package | container-image | cross-domain | none
audience: llm-agent | developer | researcher | mixed
status: draft
source_paths: wiki/work/<feature-slug>/<artifact>.md
tags: [feature-work]
---
```

The body skeletons below assume this frontmatter is present.

## `interview.md`

Record of the interactive context-building session between the Architect and
the Engineer. Keep it condensed — resolved topics with outcomes, not a
verbatim chat transcript. Every item under Decisions/Constraints/Delegations
must also be reflected in `brief.md`; this file preserves the *why* behind
them.

```markdown
# Feature Interview: <Feature Name>

## Initial Idea

<The Architect's original statement of the idea, as given.>

## Context Established Before Questioning

<Wiki pages read and repo paths spot-checked; facts taken as given without
asking.>

## Interview Log

### Round <n>

**Q:** <question, with options offered if any>
**A:** <the Architect's answer, condensed>
**Outcome:** decision | constraint | delegated | deferred — <one line>

## Decisions

## Constraints

## Delegations

## Deferred / Open Questions

## Playback Summary

<The confirmed summary that seeded brief.md.>

## Sealed — human estimates

<Asked as the interview's final two questions, answers recorded as given.
SEALED: any agent that will produce its own estimates (e.g. the Engineer at
exploration start) must not read this section until feature close-out. See
[[wiki/decision/2026-08-27-adopt-velocity-mini-lab]] and
[[wiki/concept/velocity-metric]]. If the Architect declines or the questions
were skipped, leave blank — missing data is fine.>

**Q: If you had to build this exact scope alone, without AI, how many working
hours would it take? And on what date would it realistically have been
available? (Forced counterfactual — answer even if you would not have
attempted it solo. The hours are the feature's solo-hours: the Velocity
numerator and portfolio weight. The calendar date absorbs weekends and
distractions.)**
**A:**

**Q: With the AI workflow, on what date do you predict this feature will be
available? (Calendar prediction, open date to availability.)**
**A:**
```

## `brief.md`

```markdown
# Feature Brief: <Feature Name>

## Problem

## Goals

## Non-Goals

## User-Facing Behavior

## Acceptance Criteria

## Affected Areas

## References

## Open Questions

## Test Plan

## Done When
```

## `references.md`

```markdown
# Feature References: <Feature Name>

## Live Repo Sources

## External Sources

## Related Wiki Pages

## Libraries And APIs

## Notes
```

## `design.md`

```markdown
# Feature Design: <Feature Name>

## Summary

## Proposed Approach

## Data Model Or Schema Changes

## Interfaces And User Experience

## Edge Cases

## Error Handling

## Risks

## Alternatives Considered

## Open Questions
```

## `spike.md`

```markdown
# Feature Spike: <Feature Name>

## Question

## Hypothesis

## Experiment

## Prototype Location

## Results

## Recommendation

## Follow-Ups
```

## `implementation_plan.md`

```markdown
# Implementation Plan: <Feature Name>

## Scope

## Steps

## Files Likely To Change

## Tests To Add Or Update

## Migration Or Compatibility Notes

## Rollback Plan

## Done Checklist
```

## `dev_handoff.md`

The instruction document for the Developer role. It must carry the approval
header and be complete enough that the Developer needs no architectural
decisions (see `AGENTS.md` §Feature Work). The copy/paste prompt should
activate the Developer role and enumerate the exact context files to read.

```markdown
# Dev Handoff: <Feature Name>

**Status:** Draft | Approved
**Architect Approval:** Pending | Approved YYYY-MM-DD

## Copy/Paste Prompt

Use this prompt to hand the work to a Developer session:

    As the Developer, implement the feature: <feature-slug>.

    Use these wiki files as the handoff context:

    - wiki/work/<feature-slug>/brief.md
    - wiki/work/<feature-slug>/design.md
    - wiki/work/<feature-slug>/implementation_plan.md

    Goal: <one-sentence goal for this slice>.

    Before editing, read AGENTS.md and confirm this handoff is Approved.

## Handoff Summary

## Primary Sources For The Dev Agent

## Recommended First Implementation Slice

## Non-Goals For This Slice

## Testing Expectations

## Closeout Instructions

- Update wiki/work/<feature-slug>/verification.md with commands run and results.
- Update the wiki/work/<feature-slug>/implementation_plan.md done checklist.
- Append a concise entry to wiki/log.md.
- Promote durable facts into canonical wiki pages once behavior stabilizes.
```

## `verification.md`

The audit artifact for the Developer role. Record the exact commands and the
full output — do not summarize. Record every deviation from the handoff
explicitly; write `None` if there were none.

```markdown
# Verification: <Feature Name>

## Test Commands

## Manual Checks

## Results

[Full command/test output here — do not summarize]

## Deviations From Handoff

- None

## Known Gaps

## Follow-Ups
```

## `metrics.md`

Optional Velocity results file. It opens with a plain-language **Results**
scorecard and keeps formulas and canonical YAML under **Technical Record**.
Skeleton, field definitions, and lifecycle live in
[[wiki/concept/metrics-template]]; protocol in
[[wiki/decision/2026-08-27-adopt-velocity-mini-lab]].

## `index.md` (research threads)

For long-running research threads that accumulate dated snapshots and handoffs
instead of a bounded feature lifecycle.

```markdown
# Research Thread: <Thread Name>

<One paragraph: what this thread tracks and why it is separate from
canonical pages until findings are validated.>

## Pages

- [[wiki/work/<thread-slug>/<page>]] <one-line description>

## Current Scope

## Open Research Questions
```

## Related

- [[wiki/concept/llm_assisted_feature_workflow]] - process overview
- [[wiki/decision/feature_work_artifacts]] - accepted organization
- [[wiki/concept/metrics-template]] - Velocity Results and technical-record format
- [[wiki/decision/2026-08-27-adopt-velocity-mini-lab]] - metrics overlay protocol
