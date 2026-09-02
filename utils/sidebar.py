"""Sidebar fragments rendered on every page by the EyeOnData.py entrypoint."""

import streamlit as st

import utils.db as db
from utils.schema_ext import EnrichedTable


def _db_settings():
    schema_list = [
        s[0]
        for s in db.get_conn()
        .execute(
            "SELECT distinct schema_name FROM information_schema.schemata order by all"
        )
        .fetchall()
    ]

    # Schema selection inside the same expander context
    # Default to the "raw" schema
    cur_schema = st.selectbox(
        "Schema to use", schema_list, index=schema_list.index("silver")
    )

    if cur_schema is not None:
        db.get_conn().sql(f"use {cur_schema}")

    def _build_tree_md(table: EnrichedTable, depth: int = 0) -> list[str]:
        """Recursively build markdown lines for a table and its children."""
        indent = "  " * depth
        desc = f" — *{table.description}*" if table.description else ""
        col_count = len(table.columns)
        col_label = f"`{col_count} col{'s' if col_count != 1 else ''}`"
        lines = [f"{indent}- **{table.name}** {col_label}{desc}"]
        for child in sorted(table.get_children(), key=lambda t: t.name):
            lines.extend(_build_tree_md(child, depth + 1))
        return lines

    st.header("Tables")

    # Get root tables (tables with no parent)
    all_tables = db.get_schema().get_all_tables()
    root_tables = [
        name
        for name, defn in all_tables.items()
        if defn.get_parent() is None and not name.startswith("_dlt")
    ]

    # TODO: This should be dynamic from the eyeon_schema_overlay definitions. But for now, just hardwire for this single instance.
    default_option = "raw_obs"

    selected_root = st.selectbox(
        "Select Root Table",
        sorted(root_tables),
        key="root_table_selector",
        index=root_tables.index(default_option) if default_option in root_tables else 0,
    )

    # --- In your expander ---
    with st.expander("Schema Info"):
        st.write(f"**Total Tables:** {len(all_tables)}")

        root_table = db.get_schema().get_table(selected_root)
        if root_table:
            st.markdown("**Table hierarchy:**")
            st.markdown("\n".join(_build_tree_md(root_table)))
    # Clear selections button
    if st.button("🔄 Clear All Selections"):
        st.session_state.selections = {}
        st.rerun()


def sidebar_db_chooser():
    if db.exists():
        with st.sidebar:
            _db_settings()
