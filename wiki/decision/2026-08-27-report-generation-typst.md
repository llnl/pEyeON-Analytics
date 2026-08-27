---
title: "Decision: Typst is the EyeON report-generation engine"
type: decision
status: accepted
decided_on: 2026-08-27
grounded_by:
  - ../pEyeON-Analytics/wiki/work/implement-a-report-generator-ability/design.md
  - ../pEyeON-Analytics/wiki/work/implement-a-report-generator-ability/spike.md
  - ../pEyeON-Analytics/extras/spike_report_typst.py
  - ../pEyeON-Analytics/extras/spike_report_weasyprint.py
policy: human-review-required
tags: [decision, reporting, pdf, typst, duckdb, tool-selection]
---

# Decision: Typst is the EyeON Report-Generation Engine

**Date:** 2026-08-27 · **Status:** Accepted (Architect decision)

## Context

EyeON needs distributable reports (PDF first; HTML/CSV/text secondary) from
observation data in DuckDB, renderable from both the Streamlit app and a
headless CLI, under hard constraints: open-source only, fully offline design
and render, Python-primary. The feature
[[wiki/work/implement-a-report-generator-ability/brief]] ran a 12-candidate
evaluation ([[wiki/work/implement-a-report-generator-ability/design]]) and
Architect-approved spikes of two finalists on real `eyeon.duckdb` data
([[wiki/work/implement-a-report-generator-ability/spike]]).

## Decision

**Typst**, embedded via the `typst` Python bindings (Apache-2.0, pip-only),
is the report-generation engine for EyeON. Reports are Typst templates
compiled in-process (`typst.compile()` over an in-memory dict of sources);
data reaches templates as JSON produced from DuckDB SQL query results;
charts are pre-rendered SVG (matplotlib in the spike). The Architect chose
Typst over the Engineer's WeasyPrint lean with both spike results in hand —
weighting render speed at scale (~9×: 1.21 s vs 11.36 s for an 86–125 page
report), pip-only deployment with no system libraries, and the clean
bytes-in/bytes-out API above WeasyPrint's free HTML sibling output.

## Options Considered

- **Typst (`typst` pip bindings) — chosen.** Spike: 0.03 s standard /
  1.21 s full-scale compile, correct multi-section PDF with running
  header/footer and page numbers, offline verified (strace: zero network
  connects), no system deps. Known rough edges from the spike: unbreakable
  64-char hashes needed a template chunking helper; full-scale PDF was
  4.1 MB (vs 708 KB); HTML output is not first-class.
- **Jinja2 + WeasyPrint — runner-up.** Spike equally correct; one template
  yielded both PDF and HTML; graceful long-string handling; smaller PDFs;
  15-year HTML/CSS maturity. Slower at scale (11.36 s full report) and
  needs the Pango system library. Remains the documented fallback if
  Typst's HTML gap or PDF size proves costly in implementation.
- **JasperReports (JVM) — fallback only.** Healthy library (LGPL, 7.0.6,
  active 2026) with the field's best GUI designer, but the Python bridge is
  inactive (`pyreportjasper`) and a JVM in every image fails the interview's
  "clearly better" bar while the GUI designer is a nice-to-have.
- **Nine other candidates rejected in the matrix** (BIRT, ReportLab, Quarto,
  Playwright/Chromium, fpdf2, borb, xhtml2pdf, Evidence.dev, LaTeX) — see
  [[wiki/work/implement-a-report-generator-ability/design]] for one-line
  reasons.

## Tradeoffs

- **HTML secondary output needs its own path** (small Jinja2 template, or
  Typst's experimental HTML export) — accepted; PDF is the forcing
  function.
- **PDF size at scale** (4.1 MB for 86 pages of monospace-heavy tables) —
  likely reducible in templates; to be addressed in implementation.
- **Long unbroken strings** (hashes — ubiquitous in EyeON data) require the
  chunking pattern from the spike template.
- **Young template language**: Typst markup is new to the team; mitigated
  by compact, readable templates demonstrated in the spike.
- **Community packages (`@preview`, e.g. cetz) fetch from the network on
  first use** — implementation must either avoid them (spike pattern:
  pre-rendered SVG charts) or vendor via `package_path`; vendoring is
  untested.

## Consequences

- The follow-on implementation feature adds `typst` as a project
  dependency, graduates the spike's shared query layer and templates from
  `extras/` into a proper module, and wires rendering into the Streamlit
  app and a CLI entry point (artifact distribution was deferred at feature
  open).
- CSV/text exports come straight from DuckDB, independent of Typst.
- `extras/spike_report_typst.py` is the reference implementation pattern;
  `extras/spike_report_weasyprint.py` is retained as the fallback record.
- Feature close-out (Velocity results:
  [[wiki/work/implement-a-report-generator-ability/metrics]]) records this
  ADR's acceptance as the availability anchor.

## Supersedes / Superseded By

None.
