from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.metadata_catalog import MetadataCatalog
from utils.queries import Query
from utils.sqlutil import ilike_pattern

catalog = MetadataCatalog()
q = Query()


def _roots(include_leaf_roots: bool, filename_filter: str) -> pd.DataFrame:
    where = []
    params = []
    if filename_filter:
        where.append("r.filename ilike ?")
        params.append(ilike_pattern(filename_filter))
    if not include_leaf_roots:
        where.append("coalesce(c.child_count, 0) > 0")
    where_sql = "where " + " and ".join(where) if where else ""
    return q.df(
        f"""
        with child_counts as (
          select parent as uuid, count(*) as child_count
          from silver.raw_obs
          where parent is not null
          group by parent
        )
        select
          r.uuid,
          r.filename,
          r.bytecount,
          r.magic,
          r.sha256,
          coalesce(c.child_count, 0) as child_count,
          r.source_path,
          r.source_file
        from silver.raw_obs r
        left join child_counts c on c.uuid = r.uuid
        {where_sql}
        order by child_count desc, filename
        limit 500
        """,
        params,
    )


def _tree(root_uuid: str, max_depth: int) -> pd.DataFrame:
    return q.df(
        """
        with recursive tree as (
          select
            uuid,
            parent,
            filename,
            bytecount,
            magic,
            md5,
            sha1,
            sha256,
            source_path,
            source_file,
            0 as depth
          from silver.raw_obs
          where uuid = ?

          union all

          select
            c.uuid,
            c.parent,
            c.filename,
            c.bytecount,
            c.magic,
            c.md5,
            c.sha1,
            c.sha256,
            c.source_path,
            c.source_file,
            t.depth + 1 as depth
          from silver.raw_obs c
          join tree t on c.parent = t.uuid
          where t.depth < ?
        )
        select * from tree
        order by depth, filename, uuid
        """,
        [root_uuid, max_depth],
    )


def _tree_with_metadata(root_uuid: str, max_depth: int) -> pd.DataFrame:
    metadata_sql = catalog.detail_union_sql()
    return q.df(
        f"""
        with recursive tree as (
          select
            uuid,
            parent,
            filename,
            bytecount,
            magic,
            sha256,
            0 as depth
          from silver.raw_obs
          where uuid = ?

          union all

          select
            c.uuid,
            c.parent,
            c.filename,
            c.bytecount,
            c.magic,
            c.sha256,
            t.depth + 1 as depth
          from silver.raw_obs c
          join tree t on c.parent = t.uuid
          where t.depth < ?
        ), metadata as ({metadata_sql})
        select
          t.*,
          coalesce(m.metadata_type, 'No metadata') as metadata_type,
          m.extension,
          m.mime_type
        from tree t
        left join metadata m on m.uuid = t.uuid
        """,
        [root_uuid, max_depth],
    )


def _summary(tree_md: pd.DataFrame) -> pd.DataFrame:
    if tree_md.empty:
        return pd.DataFrame()
    summary = (
        tree_md.groupby(["depth", "metadata_type", "extension", "mime_type"], dropna=False)
        .agg(files=("uuid", "nunique"), bytes=("bytecount", "sum"))
        .reset_index()
        .sort_values(["depth", "files", "bytes"], ascending=[True, False, False])
    )
    return summary


def _render_tree_preview(tree: pd.DataFrame, max_rows: int) -> None:
    st.subheader("Hierarchy Preview")
    if tree.empty:
        st.info("No rows found for this root.")
        return
    lines = []
    for _, row in tree.head(max_rows).iterrows():
        indent = "  " * int(row["depth"])
        label = f"{indent}- `{row['filename']}` ({row['bytecount']} bytes)"
        lines.append(label)
    if len(tree) > max_rows:
        lines.append(f"- ... {len(tree) - max_rows} more rows hidden by preview limit")
    st.markdown("\n".join(lines))



def main():
    st.title("Observation Hierarchy")
    st.caption(
        "Browse parent/child observations with summary groups so large containers do not render as thousands of rows by default."
    )

    with st.sidebar.expander("Hierarchy Controls", expanded=True):
        include_leaf_roots = st.checkbox("Include roots with no children", value=False)
        filename_filter = st.text_input("Root filename filter", placeholder="openwrt, DVRF, *.bin")
        max_depth = st.slider("Max depth", min_value=1, max_value=8, value=4)
        preview_rows = st.slider("Tree preview rows", min_value=25, max_value=500, value=100, step=25)
        detail_limit = st.slider("Drilldown row limit", min_value=25, max_value=1000, value=200, step=25)

    roots = _roots(include_leaf_roots, filename_filter)
    if roots.empty:
        st.info("No root observations matched the current filters.")
        return

    root_labels = [
        f"{row.filename} | children={row.child_count} | {row.uuid[:8]}"
        for row in roots.itertuples(index=False)
    ]
    selected_label = st.selectbox("Root observation", root_labels)
    selected_idx = root_labels.index(selected_label)
    root = roots.iloc[selected_idx]

    tree = _tree(root["uuid"], max_depth)
    tree_md = _tree_with_metadata(root["uuid"], max_depth)
    descendants = max(len(tree) - 1, 0)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Root children", int(root["child_count"]))
    k2.metric("Rows Shown", len(tree))
    k3.metric("Descendants Shown", descendants)
    k4.metric("Metadata Types", tree_md["metadata_type"].nunique() if not tree_md.empty else 0)

    with st.expander("Selected Root", expanded=False):
        st.dataframe(pd.DataFrame([root]), width="stretch", hide_index=True)

    summary = _summary(tree_md)
    st.subheader("Metadata Summary")
    st.caption("Grouped by friendly metadata category derived from the silver metadata tables.")
    st.dataframe(summary, width="stretch", hide_index=True)

    group_options = ["all"] + sorted(tree_md["metadata_type"].dropna().unique().tolist())
    selected_group = st.selectbox("Drill down by metadata type", group_options)
    details = tree_md if selected_group == "all" else tree_md[tree_md["metadata_type"] == selected_group]
    details = details.sort_values(["depth", "filename", "uuid"]).head(detail_limit)

    st.subheader("Drilldown Rows")
    st.dataframe(
        details[
            [
                "depth",
                "filename",
                "metadata_type",
                "extension",
                "mime_type",
                "bytecount",
                "magic",
                "uuid",
                "parent",
            ]
        ],
        width="stretch",
        hide_index=True,
    )

    _render_tree_preview(tree, preview_rows)


if __name__ in ("__main__", "__page__"):
    main()
