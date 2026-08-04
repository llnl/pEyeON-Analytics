---
title: "Decision: Feature Work Artifacts"
type: decision
status: accepted
decided_on: 2026-06-26
confidence: medium
grounded_by: []
policy: human-review-required
component: both
last_validated: 2026-06-26
tags: [workflow, llm, documentation, feature-work]
---

# Decision: Feature Work Artifacts

## Context

LLM-assisted coding benefits from compact, durable context: requirements,
references, design tradeoffs, proof-of-concept results, implementation plans,
and verification results. Without a standard location, this context can be lost
in chat history or mixed into canonical wiki pages before it has stabilized.

## Decision

Organize non-trivial feature work under a feature-specific wiki folder:

```text
wiki/work/<feature-slug>/
  brief.md
  references.md
  design.md
  spike.md
  implementation_plan.md
  verification.md
```

Each file is optional except `brief.md`; only create artifacts that add value for
the feature. All files under `wiki/work/` remain wiki pages and must include the
standard wiki frontmatter. Use `type: concept` for work artifacts unless a page
is specifically a `decision` or `tension`.

## Rationale

A folder-per-feature structure keeps requirements, references, design notes,
prototype notes, implementation planning, and verification close together. It is
easier to review than scattered notes and lighter weight than a formal product
requirements process.

This structure also separates temporary working context from canonical wiki
pages. Canonical pages should describe the system as it is or the durable
decisions behind it. `wiki/work/<feature-slug>/` can describe how a feature is
being discovered and built.

## Consequences

When starting a new non-trivial feature, the maintainer or assistant can create a
standard skeleton from [[wiki/concepts/feature_work_template]].

When implementation finishes, durable facts should be promoted into the relevant
canonical pages. The feature folder can remain as historical context for review,
merge discussion, and future reflection.

`wiki/index.md` should list active feature-work folders in a dedicated section
once the first feature folder exists.

## Alternatives Considered

One combined page per feature is simpler, but it becomes harder to scan as a
feature grows.

Only using chat history avoids extra files, but it loses traceability and makes
later review difficult.

Creating an OpenCode skill immediately would automate the workflow, but the
process should be exercised manually first. A skill can be added later if this
convention proves stable and useful.

## Related

- [[wiki/concepts/llm_assisted_feature_workflow]] - process overview
- [[wiki/concepts/feature_work_template]] - skeleton templates
