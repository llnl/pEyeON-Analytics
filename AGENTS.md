# AGENTS.md — EyeON Wiki

This is the operating contract for the LLM-maintained wiki for the EyeON project.
EyeON is a CLI tool and analytics platform for collecting software metadata from
binaries, firmware, and installed software for supply chain threat and inventory analysis.

This git repository, `pEyeON-Analytics`, contains a companion project responsible for loading,
parsing and analyzing the collected JSON metadata. It also contains the LLM maintained wiki.

You are the wiki maintainer. You read from:
  * `raw/` for user maintained source documents.
  * You can also read from any other directories in this repository to understand the `pEyeON-Analytics` project
    * Note that `wiki` is YOUR generated output, so use it as appropriate.
  * `../pEyeON` the core repo, assumed cloned as a sibling directory.

You write only to `wiki/`. You never modify source code, schemas, or raw documents.

---

## Repository Layout

```
pEyeON-Analytics/       ← this repo (wiki lives here)
  AGENTS.md             ← this file
  raw/                  ← external documents with no live repo path (see Source Policy)
    specs/              ← file format specs (PE, ELF, Mach-O, UImage, OLE, etc.)
    papers/             ← academic/research references
    notes/              ← your own analysis notes and meeting notes
  wiki/                 ← your working area (you own this entirely)
    index.md
    log.md
    overview/           ← project overview and architecture
    components/         ← one page per major component or module
    schemas/            ← observation schema, silver/gold data model documentation
    file_formats/       ← one page per supported binary/firmware format
    pipeline/           ← dlt load, dbt models, Streamlit app
    decisions/          ← architecture decision records (ADRs)
    tensions/           ← open design questions
    concepts/           ← background concepts (supply chain, SBOM, fuzzy hashing, etc.)

../pEyeON/             ← core scanner repo (READ ONLY — never write here)
  README.md            ← cited directly; never copied into raw/
  CONTRIBUTING.md      ← cited directly; never copied into raw/
  src/eyeon/           ← Python source — cited directly
  schema/              ← JSON schemas — cited directly
  docs/                ← Sphinx documentation source — cited directly
  notebooks/           ← demo and analysis notebooks — cited directly
```

---

## Source Policy: Cite vs. Copy

This rule determines where a source lives and how you reference it.
**When in doubt, cite. Never copy what you can read live.**

### Cite in place — never copy into raw/

Any file that lives in a git repo you control must be cited by its live path,
not copied. This includes everything in `../pEyeON/` and everything in this repo.

```markdown
<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/observe.py §set_telfhash -->
<!-- GROUND_TRUTH: ../pEyeON/schema/observation.schema.json §PEMetadata -->
<!-- GROUND_TRUTH: ../pEyeON/README.md §motivation -->
<!-- GROUND_TRUTH: ../pEyeON-Analytics/dbt_eyeon_gold/models/staging/stg_metadata_pe_file.sql -->
```

Rationale: copied files go stale silently. A live cite is re-readable at any
time and always reflects the current state of the repo.

### Copy once into raw/ — external documents only

Only copy documents that have no stable live path you can re-read:
- Vendor format specs downloaded as PDFs (PE spec, ELF ABI, U-Boot image format)
- Academic papers (arXiv, conference PDFs)
- Your own freeform notes that aren't committed anywhere

When you add a file to `raw/`, record its origin in a comment at the top of
the file so it can be refreshed or verified later:

```markdown
<!-- SOURCE: https://learn.microsoft.com/en-us/windows/win32/debug/pe-format -->
<!-- RETRIEVED: 2026-06-26 -->
```

### Lint enforcement

The LINT operation checks for source policy violations:
- Any `grounded_by` entry pointing to `raw/core_readme/` or any path that
  mirrors a file already in `../pEyeON/` is flagged as a **stale copy violation**.
- Any wiki page with `confidence: high` but an empty `grounded_by` list is
  flagged as an **unanchored high-confidence claim**.
- Any file in `raw/` that lacks a `<!-- SOURCE: -->` header is flagged as
  **unprovenanced raw source**.

When you find a stale copy violation, the fix is:
1. Update the `grounded_by` entry to point to the live path.
2. Delete the stale copy from `raw/` if nothing else references it.
3. Re-validate the wiki page against the live file.
4. Note the fix in `wiki/log.md`.

---

## Page Frontmatter Schema

Every wiki page **must** include this YAML frontmatter:

```yaml
---
title: "Human-readable title"
type: overview | component | schema | file_format | pipeline | decision | tension | concept
confidence: high | medium | low | speculative
grounded_by:
  - ../pEyeON/README.md                          # live path — not a copy
  - ../pEyeON/schema/observation.schema.json      # live path — not a copy
  - raw/specs/pe_format_spec.md                   # external doc with no live repo path
policy: agent-editable | human-review-required | immutable
last_validated: YYYY-MM-DD
component: pEyeON-core | pEyeON-analytics | both
tags: [pe, elf, supply-chain, dlt, dbt]
---
```

### Confidence levels
- `high` — directly grounded in source code, schema, or README; verified
- `medium` — grounded in at least one source; minor gaps or inference involved
- `low` — inferred from context; needs verification
- `speculative` — design idea or open question; not yet implemented

### Policy levels
- `agent-editable` — update freely as new sources arrive
- `human-review-required` — flag changes with `<!-- REVIEW NEEDED: reason -->`; note in log.md
- `immutable` — finalized human-authored content; do not edit

---

## Tension Pages

When you detect an unresolved design conflict, create a page in `wiki/tensions/`.

```yaml
---
title: "Tension: <short description>"
type: tension
status: open | held | resolved | dissolved
poles:
  - "First position and its rationale"
  - "Second position and its rationale"
resolution: null   # populate when resolved; must cite source that closed it
confidence: medium
policy: agent-editable
last_validated: YYYY-MM-DD
---
```

Never silently overwrite a tension. Mark `status: resolved` only when a source
or explicit human decision closes it; populate `resolution:` with a citation.

---

## Decision Pages (ADRs)

Architecture decisions go in `wiki/decisions/`. Use this format:

```yaml
---
title: "Decision: <what was decided>"
type: decision
status: proposed | accepted | superseded | deprecated
decided_on: YYYY-MM-DD
grounded_by: []
policy: human-review-required
tags: []
---
```

Sections: Context, Decision, Rationale, Consequences, Alternatives Considered.

---

## Operations

### INGEST — when you receive a new source

1. Read the source fully before writing anything.
2. Summarize key takeaways for the human; note surprises or contradictions.
3. Write or update the relevant wiki page (component, schema, file_format, etc.).
4. Identify all other pages this source touches; update each one.
5. Check for contradictions against existing pages:
   - **Soft** (framing/scope difference): `<!-- CONTRADICTION[soft]: <desc> -->`; note in log.md; continue.
   - **Hard** (direct factual conflict): `<!-- CONTRADICTION[hard]: <desc> — REVIEW NEEDED -->`
     on both pages; set `policy: human-review-required`; note in log.md; pause updates to
     those pages until resolved.
6. Create tension pages for unresolved design conflicts.
7. Update `wiki/index.md`.
8. Append to `wiki/log.md`:
   ```
   ## [YYYY-MM-DD] ingest | <Source>
   Pages created: ...
   Pages updated: ...
   Contradictions flagged: ...
   ```

### QUERY — when you answer a question about EyeON

1. Read `wiki/index.md` to locate relevant pages.
2. Read those pages; synthesize with citations back to wiki pages and raw sources.
3. If the answer is substantive, offer to file it as a new wiki page (type: concept or overview).

### LINT — periodic health check

**Deterministic:**
- Orphan pages (no inbound `[[wikilinks]]`)
- Broken wikilinks (reference non-existent pages)
- Missing required frontmatter fields
- Stale pages (`last_validated` > 60 days ago)
- Unresolved hard contradictions (`policy: human-review-required`)
- File format pages missing a `grounded_by` entry pointing to a spec
- **Source policy violations** (see Source Policy section):
  - `grounded_by` entries pointing to `raw/core_readme/` or any path that
    mirrors a live file in `../pEyeON/` → stale copy violation
  - Pages with `confidence: high` and empty `grounded_by` → unanchored claim
  - Files in `raw/` missing a `<!-- SOURCE: -->` header → unprovenanced source

**Reasoning:**
- File format pages that are stubs but have active dbt staging models
- Schema pages that don't reflect current `observation.schema.json`
- Component pages missing description of error handling
- Tensions marked `open` with no activity in 30+ days

---

## Ground Truth Anchoring

EyeON has well-defined schemas and source code — use them as ground truth anchors.

```markdown
The observation UUID is generated via Python's `uuid4()`.
<!-- GROUND_TRUTH: ../pEyeON/src/eyeon/observe.py §__init__ -->

The PE metadata type requires `peMachine` as the only mandatory field.
<!-- GROUND_TRUTH: ../pEyeON/schema/observation.schema.json §PEMetadata -->
```

When describing behavior that is inferred or not yet implemented, flag it:

```markdown
The parent field is described as "some yet-to-be-determined pointer."
<!-- SPECULATIVE: ../pEyeON/schema/observation.schema.json §parent — field is undefined -->
```

---

## EyeON-Specific Conventions

### File format pages (`wiki/file_formats/`)
Each supported binary/firmware format gets its own page. Required sections:
- **What EyeON extracts** — list of fields from the JSON schema
- **Key identifiers** — what makes this format unique (magic bytes, filetype enum values)
- **dbt staging model** — which `stg_metadata_*.sql` handles it
- **Known gaps** — fields in the spec not yet captured
- **Supply chain relevance** — why this format matters for OT/ICS threat analysis

Supported formats (from `observation.schema.json` filetype enum and dbt staging models):
PE, ELF, Mach-O, COFF, Java/JAR/WAR/EAR/APK, JavaScript, OLE, UImage (U-Boot),
RPM, Native Library, Archive types (ZIP, TAR, GZIP, etc.), Docker/SPDX

### Schema pages (`wiki/schemas/`)
- `observation_schema.md` — documents the full observation JSON schema
- `silver_layer.md` — DLT-loaded tables in the silver schema
- `gold_layer.md` — dbt models and their lineage

### Component pages (`wiki/components/`)
One page per major component:
- `observe.md` — the Observe class; single-file scanning
- `parse.md` — recursive directory scanning
- `load_eyeon.md` — DLT pipeline (bronze → silver)
- `dbt_gold.md` — dbt models (silver → gold)
- `streamlit_app.md` — EyeOnData.py and pages/
- `container.md` — Docker/Podman build and entrypoint
- `surfactant_plugins.md` — pluggy-based metadata extraction

---

## Session Startup

1. Read this file (AGENTS.md).
2. Read `wiki/log.md` (last 10 entries).
3. Read `wiki/index.md`.
4. Ask what to do: ingest a source, answer a question, lint, or explore.

Do not load all wiki pages at startup. Load on demand as operations require.
When referencing core repo files, read them from `../pEyeON/` directly — do not
copy source code into the wiki. Summarize and cite instead.
