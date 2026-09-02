"""Shared data layer for the report-generator spikes.

Both finalist prototypes (spike_report_weasyprint.py, spike_report_typst.py)
render the SAME content from the SAME queries so the comparison is fair:

  1. Batch change-detection report (gold.mart_batch_changes + batch_summary)
  2. Per-observation detail dossier (silver.raw_obs + filetype/signature
     children + gold cert fact)
  3. A matplotlib bar chart pre-rendered to SVG (both engines embed it)

See wiki/work/implement-a-report-generator-ability/design.md for the spike
plan and evaluation criteria.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "database" / "eyeon.duckdb"
OUT_DIR = Path(__file__).resolve().parent / "spike_report_out"


def connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


def batch_change_data(con: duckdb.DuckDBPyConnection, detail_limit: int | None = 40) -> dict:
    """Data for the batch change-detection report."""
    summary = con.sql(
        """
        select utility_id, num_batches, num_rows, num_md_types
        from gold.batch_summary
        order by utility_id
        """
    ).df()

    by_type = con.sql(
        """
        select utility_id, change_type, count(*) as n
        from gold.mart_batch_changes
        group by 1, 2
        order by 1, 2
        """
    ).df()

    limit_clause = f"limit {int(detail_limit)}" if detail_limit else ""
    detail = con.sql(
        f"""
        select utility_id, batch_seq, change_type, sha256,
               strftime(run_ts, '%Y-%m-%d %H:%M') as run_ts
        from gold.mart_batch_changes
        order by utility_id, batch_seq, change_type, sha256
        {limit_clause}
        """
    ).df()

    totals = con.sql(
        "select change_type, count(*) as n from gold.mart_batch_changes group by 1 order by 2 desc"
    ).df()

    return {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "db_path": str(DB_PATH),
        "total_changes": int(totals["n"].sum()),
        "totals": totals.to_dict("records"),
        "summary_rows": summary.to_dict("records"),
        "by_type_rows": by_type.to_dict("records"),
        "detail_rows": detail.to_dict("records"),
        "detail_truncated": bool(detail_limit) and len(detail) == detail_limit,
    }


def dossier_data(con: duckdb.DuckDBPyConnection) -> dict:
    """Detail dossier for one observation — prefer one that has signatures."""
    obs = con.sql(
        """
        select o.uuid, o.filename, o.bytecount, o.magic, o.md5, o.sha1, o.sha256,
               o.ssdeep, o.imphash, o.telfhash, o.authentihash, o.permissions,
               strftime(o.observation_ts, '%Y-%m-%d %H:%M') as observation_ts,
               o.source_path, o.eyeon_version, o.parent
        from silver.raw_obs o
        join silver.raw_obs__signatures s on s._dlt_parent_id = o._dlt_id
        limit 1
        """
    ).df()
    if obs.empty:  # fall back to any observation
        obs = con.sql(
            """
            select uuid, filename, bytecount, magic, md5, sha1, sha256,
                   ssdeep, imphash, telfhash, authentihash, permissions,
                   strftime(observation_ts, '%Y-%m-%d %H:%M') as observation_ts,
                   source_path, eyeon_version, parent
            from silver.raw_obs limit 1
            """
        ).df()
    row = obs.to_dict("records")[0]

    filetypes = con.sql(
        """
        select f.value as filetype
        from silver.raw_obs o
        join silver.raw_obs__filetype f on f._dlt_parent_id = o._dlt_id
        where o.uuid = ?
        """,
        params=[row["uuid"]],
    ).df()["filetype"].tolist()

    certs = con.sql(
        """
        select cert_sha256, signers, digest_algorithm, verification
        from gold.fct_observation_certificates
        where observation_uuid = ?
        order by cert_sha256
        """,
        params=[row["uuid"]],
    ).df()

    return {
        "obs": {k: ("" if v is None else v) for k, v in row.items()},
        "filetypes": filetypes,
        "certs": certs.to_dict("records"),
    }


def change_chart_svg(by_type_rows: list[dict]) -> str:
    """Grouped bar chart of changes per utility, pre-rendered to SVG."""
    import matplotlib

    matplotlib.use("svg")
    import matplotlib.pyplot as plt

    utilities = sorted({r["utility_id"] for r in by_type_rows})
    types = sorted({r["change_type"] for r in by_type_rows})
    lookup = {(r["utility_id"], r["change_type"]): r["n"] for r in by_type_rows}

    fig, ax = plt.subplots(figsize=(7.0, 2.8), dpi=100)
    width = 0.8 / max(len(types), 1)
    for i, t in enumerate(types):
        xs = [x + i * width for x in range(len(utilities))]
        ys = [lookup.get((u, t), 0) for u in utilities]
        ax.bar(xs, ys, width=width, label=t)
    ax.set_xticks([x + width * (len(types) - 1) / 2 for x in range(len(utilities))])
    ax.set_xticklabels(utilities, rotation=30, ha="right", fontsize=7)
    ax.set_ylabel("changes")
    ax.legend(fontsize=7)
    ax.set_title("Batch changes by utility and type", fontsize=9)
    fig.tight_layout()

    buf = io.StringIO()
    fig.savefig(buf, format="svg")
    plt.close(fig)
    return buf.getvalue()


def load_all(full_detail: bool = False) -> dict:
    """Everything both spikes need, in one call."""
    con = connect()
    try:
        data = batch_change_data(con, detail_limit=None if full_detail else 40)
        data["dossier"] = dossier_data(con)
        data["chart_svg"] = change_chart_svg(data["by_type_rows"])
        return data
    finally:
        con.close()
