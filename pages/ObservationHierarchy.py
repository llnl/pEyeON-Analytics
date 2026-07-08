from __future__ import annotations

import pandas as pd
import streamlit as st

import utils.db as db
from pages._base_page import BasePageLayout
from pages.pages import app_pages
from utils.utils import sidebar_config


METADATA_LABELS = {
    "metadata_binwalk_file": "Binwalk scan",
    "metadata_container_file": "Container",
    "metadata_device_tree_file": "Device tree",
    "metadata_elf_file": "ELF binary",
    "metadata_error": "Error",
    "metadata_generic_file": "Generic file",
    "metadata_java_file": "Java",
    "metadata_js_file": "JavaScript",
    "metadata_mach_o_file": "Mach-O",
    "metadata_native_lib_file": "Native library",
    "metadata_ole_file": "OLE document",
    "metadata_opkg_file": "OpenWrt package metadata",
    "metadata_pe_file": "PE binary",
    "metadata_symlink_file": "Symlink",
    "metadata_text_file": "Text/config/script",
    "metadata_uimage_file": "U-Boot image",
    "metadata_unknown": "Unknown",
    "metadata_web_asset": "Web asset",
}


KIND_LABELS = {
    "binary-blob": "binary blob",
    "config-file-list": "config file list",
    "control": "package control",
    "css": "CSS",
    "device-tree": "device tree",
    "file-list": "installed file list",
    "html": "HTML",
    "javascript": "JavaScript",
    "linux-kernel-image": "Linux kernel image",
    "lua": "Lua",
    "shell": "shell script",
    "text": "plain text",
    "ucode": "ucode",
    "unclassified": "unclassified",
}


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _metadata_label(table_name: str) -> str:
    return METADATA_LABELS.get(
        table_name,
        table_name.removeprefix("metadata_").removesuffix("_file").replace("_", " ").title(),
    )


def _base_metadata_tables() -> list[str]:
    rows = db.get_conn().execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'silver'
          -- DuckDB LIKE treats '_' as a single-character wildcard, so avoid
          -- patterns like '%__%' which would match almost anything.
          and left(table_name, 9) = 'metadata_'
          and instr(table_name, '__') = 0
        order by table_name
        """
    ).fetchall()
    return [row[0] for row in rows]


def _table_columns(table_name: str) -> set[str]:
    rows = db.get_conn().execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'silver'
          and table_name = ?
        """,
        [table_name],
    ).fetchall()
    return {row[0] for row in rows}


def _metadata_union_sql() -> str:
    selects = []
    for table_name in _base_metadata_tables():
        columns = _table_columns(table_name)
        metadata_label = _metadata_label(table_name)
        kind_expr = "cast(kind as varchar)" if "kind" in columns else _sql_literal(metadata_label)
        extension_expr = "cast(extension as varchar)" if "extension" in columns else "NULL"
        mime_expr = "cast(mime_type as varchar)" if "mime_type" in columns else "NULL"
        selects.append(
            f"""
            select
              uuid,
              '{table_name}' as metadata_table,
              {_sql_literal(metadata_label)} as metadata_type,
              {kind_expr} as semantic_kind,
              {extension_expr} as extension,
              {mime_expr} as mime_type
            from silver.{table_name}
            """
        )
    if not selects:
        return "select NULL as uuid, NULL as metadata_table, NULL as metadata_type, NULL as semantic_kind, NULL as extension, NULL as mime_type where false"
    return "\nunion all\n".join(selects)


def _roots(include_leaf_roots: bool, filename_filter: str) -> pd.DataFrame:
    where = []
    params = []
    if filename_filter:
        where.append("r.filename ilike ?")
        params.append(f"%{filename_filter.replace('*', '%')}%")
    if not include_leaf_roots:
        where.append("coalesce(c.child_count, 0) > 0")
    where_sql = "where " + " and ".join(where) if where else ""
    return db.get_conn().execute(
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
    ).df()


def _tree(root_uuid: str, max_depth: int) -> pd.DataFrame:
    return db.get_conn().execute(
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
    ).df()


def _tree_with_metadata(root_uuid: str, max_depth: int) -> pd.DataFrame:
    metadata_sql = _metadata_union_sql()
    return db.get_conn().execute(
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
          coalesce(m.semantic_kind, 'No metadata') as semantic_kind,
          m.extension,
          m.mime_type,
          case
            when m.metadata_type is null then 'No metadata'
            when m.semantic_kind is null or m.semantic_kind = m.metadata_type then m.metadata_type
            else m.metadata_type || ': ' || m.semantic_kind
          end as semantic_group
        from tree t
        left join metadata m on m.uuid = t.uuid
        """,
        [root_uuid, max_depth],
    ).df()


def _summary(tree_md: pd.DataFrame) -> pd.DataFrame:
    if tree_md.empty:
        return pd.DataFrame()
    display = tree_md.copy()
    display["semantic_kind"] = display["semantic_kind"].map(lambda value: KIND_LABELS.get(value, value))
    display["semantic_group"] = display.apply(
        lambda row: row["metadata_type"]
        if row["semantic_kind"] in {None, row["metadata_type"], "No metadata"}
        else f"{row['metadata_type']}: {row['semantic_kind']}",
        axis=1,
    )
    summary = (
        display.groupby(["depth", "semantic_group", "metadata_type", "semantic_kind", "extension", "mime_type"], dropna=False)
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


class ObservationHierarchyPage(BasePageLayout):
    def page_content(self):
        st.set_page_config(page_title="Observation Hierarchy", layout="wide")
        sidebar_config(app_pages())
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
        k4.metric("Metadata Groups", tree_md["semantic_group"].nunique() if not tree_md.empty else 0)

        with st.expander("Selected Root", expanded=False):
            st.dataframe(pd.DataFrame([root]), width="stretch", hide_index=True)

        summary = _summary(tree_md)
        st.subheader("Semantic Summary")
        st.caption("Grouped by friendly metadata category plus fallback `kind` where available. This is more stable than raw MIME type for EyeON metadata.")
        st.dataframe(summary, width="stretch", hide_index=True)

        group_options = ["all"] + sorted(tree_md["semantic_group"].dropna().unique().tolist())
        selected_group = st.selectbox("Drill down by semantic group", group_options)
        details = tree_md if selected_group == "all" else tree_md[tree_md["semantic_group"] == selected_group]
        details = details.sort_values(["depth", "filename", "uuid"]).head(detail_limit)
        details = details.copy()
        details["semantic_kind"] = details["semantic_kind"].map(lambda value: KIND_LABELS.get(value, value))

        st.subheader("Drilldown Rows")
        st.dataframe(
            details[
                [
                    "depth",
                    "filename",
                    "metadata_type",
                    "semantic_kind",
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


def main():
    page = ObservationHierarchyPage()
    page.page_content()


if __name__ == "__main__":
    main()
