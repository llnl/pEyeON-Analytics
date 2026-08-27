"""Finalist A spike: Jinja2 + WeasyPrint report prototype.

Run from the repo root (weasyprint/matplotlib are spike-only deps, not in
pyproject):

    uv run --with weasyprint --with matplotlib python extras/spike_report_weasyprint.py [--full]

Renders the batch change-detection report + one observation dossier from
database/eyeon.duckdb into extras/spike_report_out/:

    report_weasyprint.pdf   (WeasyPrint, CSS Paged Media)
    report_weasyprint.html  (same template rendered as standalone HTML —
                             the "dual output from one template" claim)

--full renders every mart_batch_changes row (multi-page scale/timing test).
Offline: all assets inline; base_url is local; no network fetches.
See wiki/work/implement-a-report-generator-ability/spike.md for findings.
"""

from __future__ import annotations

import sys
import time

import jinja2
from spike_report_data import OUT_DIR, load_all

TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>EyeON Batch Change Report</title>
<style>
  @page {
    size: letter;
    margin: 2cm 1.6cm 2.2cm 1.6cm;
    @top-center { content: "EyeON Batch Change Report — spike"; font-size: 8pt; color: #666; }
    @bottom-right { content: "Page " counter(page) " of " counter(pages); font-size: 8pt; color: #666; }
    @bottom-left { content: "{{ generated }}"; font-size: 8pt; color: #666; }
  }
  body { font-family: sans-serif; font-size: 9.5pt; color: #1a1a1a; }
  h1 { font-size: 17pt; border-bottom: 2px solid #23425f; padding-bottom: 4px; }
  h2 { font-size: 12.5pt; color: #23425f; margin-top: 1.2em; }
  table { border-collapse: collapse; width: 100%; margin: 0.5em 0 1em 0; }
  th { background: #23425f; color: white; text-align: left; padding: 3px 6px; font-size: 8.5pt; }
  td { border-bottom: 0.5pt solid #ccc; padding: 2.5px 6px; font-size: 8.5pt; }
  tr:nth-child(even) td { background: #f2f5f8; }
  thead { display: table-header-group; }  /* repeat headers across pages */
  .metrics { display: flex; gap: 1em; margin: 1em 0; }
  .metric { border: 1px solid #ccc; border-radius: 4px; padding: 8px 14px; text-align: center; }
  .metric .v { font-size: 16pt; font-weight: bold; color: #23425f; }
  .metric .l { font-size: 8pt; color: #666; }
  .mono { font-family: monospace; font-size: 7.5pt; }
  .dossier { page-break-before: always; }
  .kv td:first-child { font-weight: bold; width: 11em; }
  .note { font-size: 8pt; color: #666; font-style: italic; }
  figure { margin: 0.5em 0; text-align: center; }
</style>
</head>
<body>

<h1>EyeON Batch Change Report</h1>
<p class="note">Spike prototype — generated {{ generated }} from {{ db_path }}</p>

<div class="metrics">
  <div class="metric"><div class="v">{{ total_changes }}</div><div class="l">total changes</div></div>
  {% for t in totals %}
  <div class="metric"><div class="v">{{ t.n }}</div><div class="l">{{ t.change_type }}</div></div>
  {% endfor %}
</div>

<figure>{{ chart_svg }}</figure>

<h2>Batches Loaded</h2>
<table>
  <thead><tr><th>Utility</th><th>Batches</th><th>Rows</th><th>Metadata types</th></tr></thead>
  <tbody>
  {% for r in summary_rows %}
    <tr><td>{{ r.utility_id }}</td><td>{{ r.num_batches }}</td><td>{{ r.num_rows }}</td><td>{{ r.num_md_types }}</td></tr>
  {% endfor %}
  </tbody>
</table>

<h2>Changes by Utility and Type</h2>
<table>
  <thead><tr><th>Utility</th><th>Change type</th><th>Count</th></tr></thead>
  <tbody>
  {% for r in by_type_rows %}
    <tr><td>{{ r.utility_id }}</td><td>{{ r.change_type }}</td><td>{{ r.n }}</td></tr>
  {% endfor %}
  </tbody>
</table>

<h2>Change Detail{% if detail_truncated %} (first {{ detail_rows|length }} rows){% endif %}</h2>
<table>
  <thead><tr><th>Utility</th><th>Batch</th><th>Type</th><th>SHA-256</th><th>Run</th></tr></thead>
  <tbody>
  {% for r in detail_rows %}
    <tr><td>{{ r.utility_id }}</td><td>{{ r.batch_seq }}</td><td>{{ r.change_type }}</td>
        <td class="mono">{{ r.sha256 }}</td><td>{{ r.run_ts }}</td></tr>
  {% endfor %}
  </tbody>
</table>

<div class="dossier">
<h1>Observation Dossier</h1>
<h2>{{ dossier.obs.filename }}</h2>
<table class="kv">
  {% for k in ["uuid", "bytecount", "magic", "md5", "sha1", "sha256", "ssdeep",
               "imphash", "telfhash", "authentihash", "permissions",
               "observation_ts", "source_path", "eyeon_version", "parent"] %}
  <tr><td>{{ k }}</td><td class="mono">{{ dossier.obs[k] }}</td></tr>
  {% endfor %}
</table>

<h2>Filetype Classifications</h2>
<ul>{% for f in dossier.filetypes %}<li>{{ f }}</li>{% endfor %}</ul>

<h2>Signatures &amp; Certificates</h2>
{% if dossier.certs %}
<table>
  <thead><tr><th>Cert SHA-256</th><th>Signers</th><th>Digest</th><th>Verification</th></tr></thead>
  <tbody>
  {% for c in dossier.certs %}
    <tr><td class="mono">{{ c.cert_sha256 }}</td><td>{{ c.signers }}</td>
        <td>{{ c.digest_algorithm }}</td><td>{{ c.verification }}</td></tr>
  {% endfor %}
  </tbody>
</table>
{% else %}<p class="note">No signature/certificate records for this observation.</p>{% endif %}
</div>

</body>
</html>
"""


def main() -> None:
    full = "--full" in sys.argv
    t0 = time.perf_counter()
    data = load_all(full_detail=full)
    t_data = time.perf_counter() - t0

    env = jinja2.Environment(autoescape=jinja2.select_autoescape(["html"]))
    # chart_svg is trusted generated markup — mark safe explicitly
    data["chart_svg"] = jinja2.Markup(data["chart_svg"]) if hasattr(jinja2, "Markup") else data["chart_svg"]
    try:
        from markupsafe import Markup

        data["chart_svg"] = Markup(data["chart_svg"])
    except ImportError:
        pass

    t0 = time.perf_counter()
    html = env.from_string(TEMPLATE).render(**data)
    t_template = time.perf_counter() - t0

    OUT_DIR.mkdir(exist_ok=True)
    suffix = "_full" if full else ""
    html_path = OUT_DIR / f"report_weasyprint{suffix}.html"
    html_path.write_text(html)

    t0 = time.perf_counter()
    from weasyprint import HTML

    pdf_path = OUT_DIR / f"report_weasyprint{suffix}.pdf"
    HTML(string=html, base_url=str(OUT_DIR)).write_pdf(str(pdf_path))
    t_pdf = time.perf_counter() - t0

    rows = len(data["detail_rows"])
    print(f"rows={rows} data={t_data:.2f}s template={t_template:.3f}s pdf={t_pdf:.2f}s")
    print(f"wrote {pdf_path} ({pdf_path.stat().st_size:,} bytes)")
    print(f"wrote {html_path} ({html_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    sys.path.insert(0, str(OUT_DIR.parent))
    main()
