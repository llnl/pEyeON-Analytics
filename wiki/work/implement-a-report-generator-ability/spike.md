---
title: "Feature Spike: Report Generator Finalists — WeasyPrint vs Typst"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/extras/spike_report_data.py
  - ../pEyeON-Analytics/extras/spike_report_weasyprint.py
  - ../pEyeON-Analytics/extras/spike_report_typst.py
policy: agent-editable
last_validated: 2026-08-27
repo_scope: pEyeON-Analytics
implementation_area: analytics
format_domain: none
audience: mixed
status: reviewed
source_paths: wiki/work/implement-a-report-generator-ability/spike.md
tags: [feature-work, spike, reporting, pdf, weasyprint, typst, duckdb]
---

# Feature Spike: Report Generator Finalists — WeasyPrint vs Typst

## Question

Which approved finalist — (A) Jinja2 + WeasyPrint or (B) Typst via the
`typst` Python bindings — better satisfies the frozen acceptance criteria
when rendering real EyeON reports (PDF-first, offline, DuckDB/SQL, callable
from Streamlit and CLI)?

## Hypothesis

Both can produce a professional multi-section PDF from `eyeon.duckdb`; the
decision will hinge on secondary criteria: HTML output, render speed at
scale, deployment weight, and template ergonomics.

## Experiment

Identical content from identical queries (shared data layer,
`extras/spike_report_data.py`): a batch change-detection report
(`gold.batch_summary` + `gold.mart_batch_changes`: metric tiles, embedded
matplotlib SVG chart, three tables) plus a per-observation dossier
(`silver.raw_obs` + filetype/signature children +
`gold.fct_observation_certificates`: 15-field detail, filetype list, cert
table — the sample observation is a Sectigo-signed EFI binary). Standard run
caps detail at 40 rows; `--full` renders all 3,275 change rows.

Environment note: the repo is mounted over sshfs from macOS, and the
project `.venv` contains Mac-side symlinks this Linux host cannot resolve or
remove; the spike venv lives on local disk via
`UV_PROJECT_ENVIRONMENT=/home/ubuntu/.venvs/peyeon-analytics`. Spike deps
(`weasyprint`, `typst`, `matplotlib`, `pypdf` for verification) were run via
`uv run --with ...` and deliberately NOT added to `pyproject.toml`.

Commands (repo root = cwd `extras/`):

```
uv run --with weasyprint --with matplotlib python spike_report_weasyprint.py [--full]
uv run --with typst      --with matplotlib python spike_report_typst.py      [--full]
```

## Results

Measured 2026-08-27 on this host (aarch64 Linux, database/eyeon.duckdb):

| Measure | A: Jinja2 + WeasyPrint | B: Typst (`typst` pip) |
|---|---|---|
| Standard report (40-row detail + dossier) | 5 pages, **1.40 s** render, 47 KB | 3 pages, **0.03 s** compile, 137 KB |
| Full report (3,275-row detail) | 125 pages, **11.36 s**, 708 KB | 86 pages, **1.21 s**, 4.1 MB |
| Data+chart query time (shared) | ~0.5 s | ~0.5 s |
| Offline render (strace, connect syscalls) | **0 network connects** | **0 network connects** |
| HTML output | **Yes — same template wrote report_weasyprint.html** | Not exercised (HTML export second-class) |
| PDF fidelity | Metric tiles, chart, striped tables, repeating `thead` across pages, running header/footer with page numbers — all correct | Same features, denser layout — correct after one fix (below) |
| Long-string handling | Graceful (cells wrap/shrink) | **Defect found**: unbreakable 64-char `raw()` hash overflowed its cell and overprinted the next column; fixed with an explicit 32-char chunking helper in the template |
| Python API | In-process (`HTML(string=...).write_pdf()`); needs system Pango (present on this host) | In-process (`typst.compile(dict-of-sources)` → PDF bytes; in-memory data.json + chart.svg; no temp files); pip-only, no system libs |
| Template ergonomics | HTML/CSS + Jinja2 — familiar, verbose CSS for print | Typst markup — compact, readable, new language; functional style (map/join) for tables |
| Chart embedding | Inline SVG in HTML | In-memory `chart.svg` via `image()` |

Both engines rendered the frozen-criteria content correctly from real data.

Notable asymmetries:

- **Speed at scale is ~9× in Typst's favor** (1.21 s vs 11.36 s for the full
  table). For realistic report sizes (summaries, not raw dumps) both are
  interactive-acceptable; for very large dossier batches WeasyPrint's
  latency would be felt in Streamlit.
- **File size is ~6× in WeasyPrint's favor** at full scale (708 KB vs
  4.1 MB) — Typst's per-cell `raw()` monospace runs inflate the PDF; likely
  reducible with template changes, untested.
- **Dual output**: WeasyPrint's HTML sibling artifact came free from the
  same template, directly satisfying the "HTML secondary" criterion. Typst
  would need a parallel HTML path (or its experimental HTML export).
- **Typst offline caveat resolved by avoidance**: the spike used no
  `@preview` packages, so nothing fetched. Vendoring via `package_path`
  (needed if e.g. cetz charting is ever wanted) remains untested.

## Prototype Location

`extras/spike_report_data.py` (shared queries + chart),
`extras/spike_report_weasyprint.py`, `extras/spike_report_typst.py`.
Rendered artifacts in `extras/spike_report_out/` (gitignored; regenerate
with the commands above).

## Recommendation

Both finalists pass the frozen acceptance criteria; neither disqualified
itself, so the call rests on weighting:

- **Choose A (Jinja2 + WeasyPrint)** if the "one template → PDF *and*
  HTML" property and 15 years of HTML/CSS maturity matter most. Engineer's
  lean: this is the better default for EyeON — the app already lives in
  HTML, the HTML report satisfies the secondary output requirement with
  zero extra work, and realistic EyeON reports are summary-sized, where the
  render-time gap is imperceptible (1.4 s).
- **Choose B (Typst)** if render latency at scale, pip-only deployment (no
  Pango), and the tidy bytes-in/bytes-out API dominate — at the cost of a
  second path for HTML, a young template language, and the observed
  long-string/file-size rough edges.

## Follow-Ups

- Architect picks the winner → Engineer writes the ADR
  (`wiki/decision/`), closes out per the metrics protocol.
- If A wins: decide Pango presence in the container/VM images (present on
  dev host; needs a Dockerfile check in the implementation feature).
- If B wins: prototype the HTML secondary path and PDF-size reduction
  before production reports.
- Either way: report templates and the shared query layer graduate from
  `extras/` into a proper module in the follow-on implementation feature.
