---
title: "Concept: Feature Work Template"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: 2026-06-26
tags: [workflow, template, feature-work]
---

# Concept: Feature Work Template

## Purpose

This page defines the reusable skeleton for starting a new feature with the
LLM-assisted feature workflow.

Use this invocation:

```text
Start a new feature using the LLM-assisted feature workflow: <feature name>
```

The expected output is:

```text
wiki/work/<feature-slug>/
  brief.md
  references.md
  design.md
  spike.md
  implementation_plan.md
  verification.md
```

Only create the artifacts that are useful for the feature. For a small feature,
`brief.md` plus `verification.md` may be enough. For a high-uncertainty feature,
include `references.md`, `design.md`, and `spike.md` before implementation.

## `brief.md`

```markdown
---
title: "Feature Brief: <Feature Name>"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: YYYY-MM-DD
tags: [feature-work]
---

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
---
title: "Feature References: <Feature Name>"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: YYYY-MM-DD
tags: [feature-work, references]
---

# Feature References: <Feature Name>

## Live Repo Sources

## External Sources

## Related Wiki Pages

## Libraries And APIs

## Notes
```

## `design.md`

```markdown
---
title: "Feature Design: <Feature Name>"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: YYYY-MM-DD
tags: [feature-work, design]
---

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
---
title: "Feature Spike: <Feature Name>"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: YYYY-MM-DD
tags: [feature-work, spike]
---

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
---
title: "Implementation Plan: <Feature Name>"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: YYYY-MM-DD
tags: [feature-work, implementation]
---

# Implementation Plan: <Feature Name>

## Scope

## Steps

## Files Likely To Change

## Tests To Add Or Update

## Migration Or Compatibility Notes

## Rollback Plan

## Done Checklist
```

## `verification.md`

```markdown
---
title: "Verification: <Feature Name>"
type: concept
confidence: medium
grounded_by: []
policy: agent-editable
component: both
last_validated: YYYY-MM-DD
tags: [feature-work, verification]
---

# Verification: <Feature Name>

## Test Commands

## Manual Checks

## Results

## Known Gaps

## Follow-Ups
```

## Related

- [[wiki/concepts/llm_assisted_feature_workflow]] - process overview
- [[wiki/decisions/feature_work_artifacts]] - accepted organization
