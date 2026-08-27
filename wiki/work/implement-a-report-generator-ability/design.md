---
title: "Feature Design: Implement a Report Generator Ability"
type: concept
confidence: medium
grounded_by:
  - ../pEyeON-Analytics/wiki/work/implement-a-report-generator-ability/interview.md
  - ../pEyeON-Analytics/wiki/work/implement-a-report-generator-ability/references.md
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/implement-a-report-generator-ability/design.md
tags: [feature-work, reporting, pdf, tool-selection, duckdb, weasyprint, typst, jasperreports]
---

# Feature Design: Implement a Report Generator Ability

## Summary

Tool-selection evaluation for EyeON report generation. Constraint set from
the [[wiki/work/implement-a-report-generator-ability/interview]]: open-source
only, fully offline design + render, DuckDB/SQL data access, PDF first-class
(HTML secondary, CSV/text free), callable from both Streamlit and CLI,
Python-first with JVM admitted under burden of proof, GUI designer a
developer-operated nice-to-have.

**Proposed finalists for spikes: (1) Jinja2 + WeasyPrint, (2) Typst via the
`typst` Python bindings.** JasperReports survives the constraint filter and
is the strongest GUI-designer option, but its Python bridge story is weak
enough (see Candidate Notes) that it does not meet the interview's "clearly
better" bar; recommended as the fallback if both spikes disappoint.
Web research snapshot taken 2026-08-27; sources in
[[wiki/work/implement-a-report-generator-ability/references]].

## Evaluation Criteria

From the frozen acceptance criteria, in rough priority order:

1. **License + offline** (gate): OSS license; design and render with no
   network, no phone-home.
2. **PDF quality**: multi-section documents, tables, repeating
   headers/footers, page numbering, embedded charts.
3. **Python API ergonomics**: in-process or clean subprocess; bridge cost
   for JVM tools counts against.
4. **DuckDB/SQL data path**: how query results reach the template.
5. **App + CLI embeddability**: one programmatic core callable from both.
6. **HTML output**: second-class OK, from the same template ideally.
7. **Designer experience**: GUI is bonus; else template legibility/learning
   curve.
8. **Maturity/maintenance**: release cadence, bus factor, ecosystem age.
9. **Deployment weight**: pip-only < system libs < JVM < headless browser.

## Candidate Matrix

| Candidate | License | Offline | PDF quality | Python API | DuckDB path | HTML out | Designer | Maintenance (2026) | Deploy weight | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| **Jinja2 + WeasyPrint** | BSD (WeasyPrint), BSD (Jinja2) | Yes (URL fetcher can be disabled/local-only) | Strong: CSS Paged Media, flexbox, most grid, running headers/footers, PDF/A-3b; no JS | In-process, pure Python, pip | duckdb → dicts/DataFrames → Jinja2 context | **Free**: same template renders HTML directly | Code-first (HTML/CSS — familiar) | Active: v69, ~2-3 month release cadence (CourtBouillon) | pip + Pango system lib | **Finalist A** |
| **Typst (`typst` pip bindings)** | Apache-2.0 | Yes, with care: `@preview` packages fetch from network — must vendor via `package_path` | Strong: modern typesetting, fast compiles, PDF/UA; charts via cetz or embedded SVG | In-process bindings (`typst.compile`, in-memory dict of sources); `pypst` generates Typst from pandas | duckdb → JSON/CSV → Typst's built-in parsers, or generate markup | Experimental HTML export; not first-class | Code-first (Typst markup — new language, easy) | Very active: 0.15.x, high momentum | pip only (compiler embedded) | **Finalist B** |
| **JasperReports + Jaspersoft Studio** | LGPLv3 (library), EPL (Studio) | Yes (library + desktop Studio) | Excellent: pixel-perfect, banded layout, crosstabs, subreports; PDF/HTML/XLSX/CSV/DOCX exporters | **Weak**: `pyreportjasper` inactive (no release in 12+ months); realistic path is subprocess to JasperStarter or a custom thin Java CLI | JDBC (DuckDB JDBC driver) — direct SQL from the report definition | Yes (exporter) | **Best in field**: Jaspersoft Studio visual designer | Library very active: 7.0.6 Mar 2026, releases through Aug 2026 | JVM + jars in images | Fallback; burden of proof not met given bridge cost and nice-to-have GUI |
| Eclipse BIRT | EPL-2.0 | Yes | Good: banded designer, charts | None native; same subprocess/JVM pattern, weaker tooling than Jasper | JDBC | Yes | Eclipse-based designer | Active-ish (4.24, JDK 21) but governance wobbles: past no-project-lead period, vendor-neutrality questions | JVM + Eclipse-scale footprint | Rejected: dominated by Jasper on every JVM-side axis |
| ReportLab (OSS core) | BSD | Yes | Strong engine, but low-level programmatic layout (platypus); tables/headers hand-built | In-process, pip | duckdb → Python objects → canvas/platypus calls | No | Code-only, verbose | Mature, decades old, maintained | pip only | Rejected: layout productivity far below templating options; RML designer is commercial |
| Quarto (+ Typst/LaTeX) | MIT (CLI) | Yes after install | Strong via Typst/LaTeX backends | Subprocess (`quarto render`), parameterized docs | Python chunks can query duckdb inline | Yes, first-class | Code-first (markdown) | Active (Posit) | pandoc + deno + engine (~large) | Rejected for core engine: notebook-publishing orientation, heavy stack; viable later for analyst-authored docs on top of whatever we pick |
| Playwright/Chromium print-to-PDF | Apache-2.0 (Playwright) | Yes after browser install | Full modern CSS + JS (interactive chart libs render) | In-process API driving subprocess browser | Same as WeasyPrint (HTML pipeline) | Free (it IS html) | Code-first | Active | ~300 MB headless browser in images | Rejected: browser weight unjustified when charts can be pre-rendered SVG; revisit only if WeasyPrint CSS limits bite |
| fpdf2 | LGPL | Yes | Basic-to-moderate; imperative API | In-process, pip | Python objects | No | Code-only | Active | pip only | Rejected: below the bar for multi-section formatted reports |
| borb | AGPL | Yes | Moderate | In-process | Python objects | No | Code-only | Active | pip only | Rejected: AGPL friction for an embedded component; no advantage over finalists |
| xhtml2pdf | Apache | Yes | Dated CSS support | In-process | HTML pipeline | n/a | Code-first | Low activity | pip | Rejected: WeasyPrint dominates |
| Evidence.dev | MIT | Partial | HTML-first; PDF via print | Node stack, not Python | DuckDB-native (its strength) | First-class | Markdown+SQL | Active | Node toolchain | Rejected: wrong stack and PDF-second; noted for its DuckDB-native SQL-in-markdown model |
| LaTeX (pylatex etc.) | Various OSS | Yes | Excellent | Template/subprocess | Python → tex | No | Code-first, steep | Mature | TeX distribution (GB-scale) | Rejected: toolchain weight and learning curve vs Typst, which is its modern successor here |

## Candidate Notes (grounded highlights)

### Finalist A — Jinja2 + WeasyPrint

- BSD-licensed pure-Python HTML/CSS→PDF renderer; no browser, no JS engine;
  CSS layout engine designed for pagination. Current major version 69,
  maintained by CourtBouillon with a steady release cadence.
- Supports CSS Paged Media (`@page`, running headers/footers, page
  numbers), flexbox fully, grid mostly; PDF/A-3b variant available.
- The decisive structural advantage: **one Jinja2 template yields both the
  PDF (via WeasyPrint) and the HTML output natively**, and the app already
  speaks HTML. Charts pre-rendered to SVG (matplotlib/plotly static export)
  embed cleanly.
- Known limits: no JavaScript (client-rendered charts impossible — must
  pre-render), very complex CSS grid layouts partial. System dependency on
  Pango (already common in Linux images).

### Finalist B — Typst via `typst` Python bindings

- Apache-2.0 Rust typesetting compiler; the `typst` pip package compiles
  in-process (`typst.compile(...)`), accepts an in-memory dict of source
  files, returns PDF bytes — a clean fit for both Streamlit and CLI use.
  Reproducible-PDF timestamp support is built in.
- Fast compiles (ms-scale) aimed explicitly at batch document generation;
  PDF/UA accessibility; JSON/CSV parsers built into the language — the
  natural data path is duckdb → JSON → `sys.inputs`.
- `pypst` (PyPI) generates Typst markup from pandas DataFrames if we prefer
  building documents programmatically over templating.
- Offline caveat to verify in the spike: community packages (`@preview`,
  e.g. cetz for charts) are network-fetched on first use; must vendor them
  into a local `package_path` for air-gapped render. HTML export exists but
  is not first-class — HTML output would come from a separate small Jinja2
  path or be accepted as second-class.

### JVM benchmark — JasperReports (fallback, not spiked)

- The library itself is healthy: LGPLv3, 7.0.6 (Mar 2026), continued
  releases and CVE response in 2026; Jaspersoft Studio remains the best
  GUI report designer in the open-source field; JDBC gives direct
  SQL-in-report against DuckDB's JDBC driver.
- The Python bridge is the problem: `pyreportjasper` shows inactive-project
  signals (no PyPI release in 12+ months, ~1k weekly downloads) and wraps
  the third-party JasperStarter CLI; the robust integration is a
  subprocess boundary plus a JVM and driver jars in every deployment image.
  With the GUI designer explicitly a nice-to-have, that cost fails the
  interview's "clearly better" test — unless both Python finalists
  disappoint in the spikes.

## Proposed Approach

1. Architect reviews this matrix and the finalist proposal (this page).
2. Spike both finalists against the same target: a **batch change-detection
   report** (from `gold.mart_batch_changes`) plus a **per-observation
   dossier fragment** — together they exercise tables, multi-section
   layout, headers/footers, and a chart. Real data from the local
   `eyeon.duckdb`; prototypes in `extras/`; findings in `spike.md`.
3. Spike checks: PDF fidelity side by side; offline render verified (no
   network); render time for a ~50-page batch report; template legibility;
   HTML story (WeasyPrint native vs Typst workaround); Streamlit + CLI
   invocation shape.
4. Write the ADR naming the winner; close out per the metrics protocol.

## Risks

- WeasyPrint render time on very large reports (hundreds of pages) is its
  known weak spot — the spike should measure a realistic worst case.
- Typst's offline package vendoring may be fiddly; if cetz charting can't
  be cleanly vendored, charts fall back to embedded SVG (same as A).
- Both finalists are code-first: if the Architect's GUI-designer
  nice-to-have hardens into a requirement later, the decision would need
  revisiting toward Jasper (a logged criteria amendment).

## Open Questions

- Whether HTML output must come from the same template as PDF (favors A) or
  a second render path is acceptable (neutralizes B's HTML gap).
- Chart library standard for pre-rendered SVGs (matplotlib vs plotly
  static) — can be settled in the spike.
