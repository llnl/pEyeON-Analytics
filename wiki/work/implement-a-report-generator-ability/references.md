---
title: "Feature References: Implement a Report Generator Ability"
type: concept
confidence: high
grounded_by: []
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: llm-agent
status: draft
source_paths: wiki/work/implement-a-report-generator-ability/references.md
tags: [feature-work, reporting, references]
---

# Feature References: Implement a Report Generator Ability

Web research snapshot retrieved 2026-08-27 for the candidate evaluation in
[[wiki/work/implement-a-report-generator-ability/design]]. Live URLs — not
copied into `raw/` since each is re-checkable; re-verify before the ADR if
the decision is delayed substantially.

## Live Repo Sources

- `../pEyeON-Analytics/load_eyeon.py` — DLT loads natively into
  `eyeon.duckdb` (duckdb destination).
- `../pEyeON-Analytics/dbt_eyeon_gold/macros/parquet_glob.sql` — vestigial
  parquet source path; DuckDB reads parquet natively.
- `../pEyeON-Analytics/dbt_eyeon_gold/models/` — gold marts that reports
  would consume (e.g. `gold.mart_batch_changes`).

## External Sources

### Finalist A — WeasyPrint / Jinja2

- https://weasyprint.org/ — project home; sample gallery (reports, invoices,
  books). RETRIEVED: 2026-08-27
- https://doc.courtbouillon.org/weasyprint/stable/ — v69 documentation; BSD
  license; pure-Python CSS layout engine for pagination.
  RETRIEVED: 2026-08-27
- https://pdf4.dev/blog/weasyprint-vs-wkhtmltopdf — maintenance cadence
  (~2-3 month releases, CourtBouillon), CSS coverage (flexbox full, grid
  mostly), PDF/A-3b, no-JS tradeoff, Chromium alternative framing.
  RETRIEVED: 2026-08-27

### Finalist B — Typst

- https://pypi.org/project/typst/ — `typst` Python bindings: in-process
  compile, in-memory source dicts, reproducible timestamps, package_path /
  package_cache_path for local packages. RETRIEVED: 2026-08-27
- https://typst.app/blog/2025/automated-generation/ — batch PDF generation
  positioning: ms-scale compiles, JSON/XML/CSV parsers built in, PDF/UA.
  RETRIEVED: 2026-08-27
- https://typst.app/open-source/ — Apache-2.0 compiler, embedding guidance,
  current release 0.15.x. RETRIEVED: 2026-08-27
- https://pypi.org/project/pypst/ — Typst markup generation from Python /
  pandas (no compiler). RETRIEVED: 2026-08-27

### JVM benchmark — JasperReports / BIRT

- https://github.com/Jaspersoft/jasperreports — JasperReports Library, LGPL;
  pixel-perfect banded reports; PDF/HTML/XLSX/etc. exporters; Jaspersoft
  Studio designer. RETRIEVED: 2026-08-27
- https://sourceforge.net/p/jasperreports/news/ — release activity: 7.0.6
  (2026-03-13), further news 2026-05/2026-08 incl. CVE-2026-6009 fix.
  RETRIEVED: 2026-08-27
- https://snyk.io/advisor/python/pyreportjasper — inactive-project signals
  for the Python bridge: no PyPI release in 12+ months, ~998 weekly
  downloads. RETRIEVED: 2026-08-27
- https://pypi.org/project/pyreportjasper/2.0.2/ — bridge mechanics: wraps
  JasperStarter CLI; JDBC drivers dropped into package directory.
  RETRIEVED: 2026-08-27
- https://github.com/eclipse-birt/birt — BIRT 4.24, JDK 21, Tomcat 10.
  RETRIEVED: 2026-08-27
- https://www.eclipse.org/lists/birt-dev/msg11060.html — governance
  concerns: project-lead vacancy, vendor-neutrality questions, release-gap
  history. RETRIEVED: 2026-08-27
- https://metrics.eclipse.org/projects/technology.birt/ — 186 commits / 15
  people / 12 months (as of late 2025). RETRIEVED: 2026-08-27

## Related Wiki Pages

- [[wiki/work/implement-a-report-generator-ability/interview]] — resolved
  constraints and delegations
- [[wiki/work/implement-a-report-generator-ability/brief]] — frozen
  acceptance criteria
- [[wiki/schema/gold_layer]] — the data reports consume
- [[wiki/component/streamlit_app]] — one of the two render contexts

## Libraries And APIs

- `duckdb` (already a dependency) — query engine for both finalists.
- Finalist A adds: `jinja2`, `weasyprint` (+ system Pango).
- Finalist B adds: `typst` (pip, embedded compiler), optionally `pypst`.
- Chart pre-rendering (either finalist): matplotlib or plotly static SVG
  export — to be settled in the spike.

## Notes

- Rejected candidates and one-line reasons live in the design.md matrix
  (ReportLab, Quarto, Playwright/Chromium, fpdf2, borb (AGPL), xhtml2pdf,
  Evidence.dev, LaTeX, BIRT).
- Offline caveat to verify in the Typst spike: `@preview` packages are
  network-fetched on first use; vendoring via `package_path` required for
  air-gapped render.
