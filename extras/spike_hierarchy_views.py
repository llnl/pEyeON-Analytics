"""Spike prototype: options for displaying the raw_obs nested-table hierarchy.

Run standalone from the repo root:

    uv run streamlit run extras/spike_hierarchy_views.py

Demonstrates the two leading options from
wiki/work/cleanup-streamlit-app/spike.md:
  A. Document view — the original nested JSON from bronze.raw_json
  B. Re-nested from silver — DuckDB reassembles child tables into JSON
     (shown both as st.json and as JsonColumn cells in a grid)
"""

from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

import utils.db as db
from utils.queries import Query

q = Query()


def _child_tables(parent: str) -> list[str]:
    """Immediate DLT child tables of a silver table (by __ naming)."""
    rows = q.df(
        """
        select table_name from information_schema.tables
        where table_schema = 'silver'
          and table_name like ? escape '\\'
          and table_name not like ? escape '\\'
        order by table_name
        """,
        [parent.replace("_", "\\_") + "\\_\\_%", parent.replace("_", "\\_") + "\\_\\_%\\_\\_%"],
    )
    return rows["table_name"].tolist() if not rows.empty else []


def _renest(table: str, dlt_id: str) -> dict:
    """Rebuild a nested dict for one row from its DLT child tables, recursively."""
    row = q.df(f"select * from silver.{table} where _dlt_id = ?", [dlt_id])
    if row.empty:
        return {}
    doc = {
        k: v
        for k, v in row.iloc[0].to_dict().items()
        if not k.startswith("_dlt") and pd.notna(v)
    }
    for child in _child_tables(table):
        kids = q.df(
            f"select * from silver.{child} where _dlt_parent_id = ? order by _dlt_list_idx",
            [dlt_id],
        )
        if kids.empty:
            continue
        key = child.removeprefix(table + "__")
        if list(kids.columns) >= ["value"] and "value" in kids.columns and len(
            [c for c in kids.columns if not c.startswith("_dlt")]
        ) == 1:
            # simple list table (e.g. raw_obs__filetype)
            doc[key] = kids["value"].tolist()
        else:
            doc[key] = [
                _renest(child, kid_id) for kid_id in kids["_dlt_id"].tolist()
            ]
    return doc


def main():
    st.set_page_config(page_title="Spike: Hierarchy Views", layout="wide")
    st.title("Spike: Hierarchical Display Options")
    st.caption(
        "Prototype for wiki/work/cleanup-streamlit-app/spike.md — pick a row, "
        "compare the display options."
    )

    obs = q.df(
        """
        select r.filename, r.magic, r.bytecount, r.uuid, r._dlt_id,
               exists (select 1 from silver.raw_obs__signatures s
                       where s._dlt_parent_id = r._dlt_id) as has_signatures
        from silver.raw_obs r
        order by has_signatures desc, r.filename
        limit 300
        """
    )
    st.markdown("**1. Pick an observation** (signed files first — they have the deepest nesting)")
    event = st.dataframe(
        obs.drop(columns=["_dlt_id"]),
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="spike_obs",
    )
    if not event.selection.rows:
        st.info("Select a row above.")
        return
    sel = obs.iloc[event.selection.rows[0]]

    tab_a, tab_b, tab_c = st.tabs(
        [
            "A · Original document (bronze.raw_json)",
            "B · Re-nested from silver (st.json)",
            "B2 · Grid with JsonColumn children",
        ]
    )

    with tab_a:
        st.caption(
            "The scanner's original nested JSON — zero reconstruction, exactly "
            "what EyeON emitted. st.json gives free expand/collapse."
        )
        doc = q.df(
            "select json from bronze.raw_json where uuid = ?", [sel["uuid"]]
        )
        if doc.empty:
            st.warning("No bronze.raw_json row for this uuid.")
        else:
            st.json(json.loads(doc.iloc[0]["json"]), expanded=2)

    with tab_b:
        st.caption(
            "Reassembled from the silver child tables (reflects what DLT "
            "actually loaded, including normalization)."
        )
        st.json(_renest("raw_obs", sel["_dlt_id"]), expanded=2)

    with tab_c:
        st.caption(
            "One flat grid; each child table becomes an expandable JSON cell "
            "(st.column_config.JsonColumn). Keeps table sorting/scanning."
        )
        grid = q.df(
            """
            select
              r.filename, r.magic, r.bytecount,
              (select to_json(list(ft.value order by ft._dlt_list_idx))
                 from silver.raw_obs__filetype ft
                 where ft._dlt_parent_id = r._dlt_id) as filetypes,
              (select to_json(list(struct_pack(
                        signers := s.signers,
                        digest_algorithm := s.digest_algorithm,
                        verification := s.verification,
                        certs := (select list(to_json(c))
                                    from silver.raw_obs__signatures__certs c
                                    where c._dlt_parent_id = s._dlt_id))))
                 from silver.raw_obs__signatures s
                 where s._dlt_parent_id = r._dlt_id) as signatures,
              r.uuid
            from silver.raw_obs r
            where r.uuid = ?
            """,
            [sel["uuid"]],
        )
        st.dataframe(
            grid,
            width="stretch",
            hide_index=True,
            column_config={
                "filetypes": st.column_config.JsonColumn("filetypes"),
                "signatures": st.column_config.JsonColumn("signatures", width="large"),
            },
        )
        st.caption(
            "In a real page this grid would show many rows at once; the JSON "
            "cells expand on click."
        )


if __name__ in ("__main__", "__page__"):
    main()
