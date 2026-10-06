"""Finalist B spike: Typst (in-process `typst` bindings) report prototype.

Run from the repo root (typst/matplotlib are spike-only deps, not in
pyproject):

    uv run --with typst --with matplotlib python extras/spike_report_typst.py [--full]

Renders the same batch change-detection report + observation dossier as the
WeasyPrint spike, from database/eyeon.duckdb, into extras/spike_report_out/:

    report_typst.pdf

Data reaches Typst as an in-memory JSON virtual file (json("data.json")) and
the matplotlib chart as an in-memory SVG — the whole compile happens
in-process via typst.compile() on a dict of sources, no temp files, no CLI.
Offline: deliberately uses NO @preview packages (they would fetch from the
network on first use — vendoring via package_path is the documented path if
we ever need one; that remains untested in this spike).
--full renders every mart_batch_changes row (multi-page scale/timing test).
See wiki/work/implement-a-report-generator-ability/spike.md for findings.
"""

from __future__ import annotations

import json
import sys
import time

from spike_report_data import OUT_DIR, load_all

MAIN_TYP = r"""
#let data = json("data.json")

#set page(
  paper: "us-letter",
  margin: (top: 2cm, bottom: 2.2cm, x: 1.6cm),
  header: text(size: 8pt, fill: gray)[EyeON Batch Change Report — spike],
  footer: context [
    #text(size: 8pt, fill: gray)[#data.generated]
    #h(1fr)
    #text(size: 8pt, fill: gray)[Page #counter(page).display("1 of 1", both: true)]
  ],
)
#set text(font: "DejaVu Sans", size: 9.5pt)
#let navy = rgb("#23425f")
#show heading.where(level: 1): it => [
  #text(size: 17pt, weight: "bold")[#it.body]
  #line(length: 100%, stroke: 2pt + navy)
]
#show heading.where(level: 2): set text(size: 12.5pt, fill: navy)

// 64-char hashes have no natural breakpoints; chunk them so table cells can
// wrap instead of overflowing into the next column (spike finding).
#let chunked(s, n: 32) = {
  let parts = ()
  let i = 0
  while i < s.len() {
    parts.push(s.slice(i, calc.min(i + n, s.len())))
    i += n
  }
  parts.map(p => raw(p)).join(linebreak())
}

#let styled-table(headers, rows) = table(
  columns: headers.len(),
  stroke: none,
  fill: (_, y) => if y == 0 { navy } else if calc.even(y) { rgb("#f2f5f8") },
  table.header(..headers.map(h => text(fill: white, size: 8.5pt, weight: "bold")[#h])),
  ..rows.flatten().map(c => text(size: 8.5pt)[#c]),
)

= EyeON Batch Change Report
#text(size: 8pt, fill: gray, style: "italic")[Spike prototype — generated #data.generated from #raw(data.db_path)]

#let metric(v, l) = box(stroke: 1pt + rgb("#ccc"), radius: 4pt, inset: 8pt)[
  #align(center)[#text(size: 16pt, weight: "bold", fill: navy)[#v] \ #text(size: 8pt, fill: gray)[#l]]
]
#stack(dir: ltr, spacing: 1em,
  metric(str(data.total_changes), "total changes"),
  ..data.totals.map(t => metric(str(t.n), t.change_type)),
)

#align(center)[#image("chart.svg", width: 90%)]

== Batches Loaded
#styled-table(
  ("Utility", "Batches", "Rows", "Metadata types"),
  data.summary_rows.map(r => (r.utility_id, str(r.num_batches), str(r.num_rows), str(r.num_md_types))),
)

== Changes by Utility and Type
#styled-table(
  ("Utility", "Change type", "Count"),
  data.by_type_rows.map(r => (r.utility_id, r.change_type, str(r.n))),
)

== Change Detail #if data.detail_truncated [(first #data.detail_rows.len() rows)]
#styled-table(
  ("Utility", "Batch", "Type", "SHA-256", "Run"),
  data.detail_rows.map(r => (
    r.utility_id, str(r.batch_seq), r.change_type,
    raw(r.sha256), r.run_ts,
  )),
)

#pagebreak()
= Observation Dossier
== #data.dossier.obs.filename

#table(
  columns: (11em, 1fr),
  stroke: none,
  fill: (_, y) => if calc.even(y) { rgb("#f2f5f8") },
  ..(
    ("uuid", "bytecount", "magic", "md5", "sha1", "sha256", "ssdeep",
     "imphash", "telfhash", "authentihash", "permissions",
     "observation_ts", "source_path", "eyeon_version", "parent")
      .map(k => (
        text(size: 8.5pt, weight: "bold")[#k],
        raw(str(data.dossier.obs.at(k, default: ""))),
      ))
      .flatten()
  ),
)

== Filetype Classifications
#for f in data.dossier.filetypes [- #f]

== Signatures & Certificates
#if data.dossier.certs.len() > 0 [
  #styled-table(
    ("Cert SHA-256", "Signers", "Digest", "Verification"),
    data.dossier.certs.map(c => (chunked(c.cert_sha256), c.signers, c.digest_algorithm, c.verification)),
  )
] else [
  #text(size: 8pt, fill: gray, style: "italic")[No signature/certificate records for this observation.]
]
"""


def main() -> None:
    full = "--full" in sys.argv
    t0 = time.perf_counter()
    data = load_all(full_detail=full)
    t_data = time.perf_counter() - t0

    chart_svg = data.pop("chart_svg")
    files = {
        "main.typ": MAIN_TYP.encode(),
        "data.json": json.dumps(data, default=str).encode(),
        "chart.svg": chart_svg.encode(),
    }

    OUT_DIR.mkdir(exist_ok=True)
    suffix = "_full" if full else ""
    pdf_path = OUT_DIR / f"report_typst{suffix}.pdf"

    t0 = time.perf_counter()
    import typst

    pdf_bytes = typst.compile(files)
    t_pdf = time.perf_counter() - t0
    pdf_path.write_bytes(pdf_bytes)

    rows = len(data["detail_rows"])
    print(f"rows={rows} data={t_data:.2f}s compile={t_pdf:.2f}s")
    print(f"wrote {pdf_path} ({pdf_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    sys.path.insert(0, str(OUT_DIR.parent))
    main()
