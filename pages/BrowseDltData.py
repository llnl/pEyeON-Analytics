import hashlib
from pathlib import Path
import subprocess

import duckdb
import pandas as pd
import streamlit as st

import utils.db as db
from pages._base_page import BasePageLayout
from pages.pages import app_pages
from utils.schema_ext import EnrichedTable
from utils.utils import sidebar_config
import utils.search_forms as sf


class LandingPage(BasePageLayout):
    def __init__(self):
        super().__init__()

    def page_content(self):
        st.set_page_config(layout="wide")
        sidebar_config(app_pages())

        def get_child_tables(table_name):
            """Get immediate child tables"""
            return db.get_schema().get_table(table_name).get_children()

        def clear_selection_branch(table: EnrichedTable):
            """Clear selection state for a table and all of its children."""
            st.session_state.selections.pop(table.name, None)
            for child in get_child_tables(table.name):
                clear_selection_branch(child)

        def selection_key_for(table_name, level, key_prefix, query_signature):
            query_hash = hashlib.md5(query_signature.encode("utf-8")).hexdigest()[:12]
            return f"{key_prefix}_{table_name}_{level}_{query_hash}"

        def query_table(table_name, parent_row=None, state_prefix=""):
            """
            Query a table, optionally filtering by parent_id
            """
            parent_column = "_dlt_parent_id"
            parent_id = None

            table = db.get_schema().get_table(table_name)

            results = None
            query_signature = f"table:{table_name}"
            if parent_row is None:
                # Root level query
                if table.search_field:
                    results, sql = sf.search_raw_obs(
                        table_name, table, key_prefix=state_prefix
                    )
                    query_signature = f"sql:{sql}"
                else:
                    if any(col.extra.get("search") for col in table.columns.values()):
                        search_cols = {}
                        for k, v in table.columns.items():
                            if v.extra.get("search"):
                                checkbox_key = (
                                    f"{state_prefix}_{table_name}_null_search_{k}"
                                )
                                search_cols[k] = st.checkbox(
                                    f"{k} is null", False, key=checkbox_key
                                )
                        filter = "true"
                        for k, v in search_cols.items():
                            if v:
                                filter += f" and {k} is null"
                        query_signature = (
                            f"sql:select * from {table_name} where {filter}"
                        )
                        # TODO: Ultimately replace table_name string with an object that includes schema and possibly other metadata.
                        if table_name == "raw_json":
                            hack = "bronze.raw_json"
                        else:
                            hack = table_name
                        results = (
                            db.get_conn()
                            .execute(f"select * from {hack} where {filter}")
                            .df()
                        )
                    else:
                        try:
                            results = (
                                db.get_conn()
                                .execute(f"SELECT * FROM {table_name}")
                                .df()
                            )
                        except duckdb.CatalogException:
                            st.write(
                                "Table not found. Check the selected schema in the sidebar."
                            )
                            results = pd.DataFrame()
            else:
                # Child query filtered by parent
                # TODO:
                if table_name.startswith("metadata_") and "__" not in table_name:
                    parent_column = "uuid"
                    parent_id = parent_row["uuid"]
                else:
                    parent_column = "_dlt_parent_id"
                    parent_id = parent_row["_dlt_id"]
                query_signature = f"child:{table_name}:{parent_column}:{parent_id}"
                results = (
                    db.get_conn()
                    .execute(
                        f"SELECT * FROM {table_name} WHERE {parent_column} = ? LIMIT 100",
                        [parent_id],
                    )
                    .df()
                )

            return results, query_signature

        def render_table_level(
            table: EnrichedTable, parent_row=None, level=0, key_prefix=""
        ):
            # "Recursively render table and its children"
            # Query the data
            df, query_signature = query_table(table.name, parent_row, key_prefix)

            if df.empty:
                clear_selection_branch(table)
                if not hide_empty_tables:
                    st.markdown(
                        f"{'  ' * level}### {'📄' if level == 0 else '📋'} {table.name}"
                    )
                    st.info(f"No data in {table.name}")
                return
            else:
                st.markdown(
                    f"{'  ' * level}### {'📄' if level == 0 else '📋'} {table.name} {'&nbsp;' * 10} _{len(df)} rows_",
                    unsafe_allow_html=True,
                )

            # Show dataframe with selection enabled
            selection_key = selection_key_for(
                table.name, level, key_prefix, query_signature
            )
            persisted_selected_id = st.session_state.selections.get(table.name)

            column_config = {}
            if hide_dlt_columns:
                column_config = {
                    col: None for col in df.columns if col.startswith("_dlt")
                }

            display_df = df
            if persisted_selected_id is not None and "_dlt_id" in df.columns:

                def highlight_selected_row(row):
                    if row["_dlt_id"] == persisted_selected_id:
                        return ["background-color: rgba(49, 51, 63, 0.12)"] * len(row)
                    return [""] * len(row)

                display_df = df.style.apply(highlight_selected_row, axis=1)

            event = st.dataframe(
                display_df,
                width="stretch",
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key=selection_key,
                column_config=column_config,
            )

            # Check if a row was selected
            selected_row = None
            if event.selection.rows:
                selected_idx = event.selection.rows[0]
                if 0 <= selected_idx < len(df):
                    selected_row = df.iloc[selected_idx]

            if selected_row is None and table.name in st.session_state.selections:
                selected_id = st.session_state.selections[table.name]
                matching_rows = df[df["_dlt_id"] == selected_id]
                if not matching_rows.empty:
                    selected_row = matching_rows.iloc[0]

            if selected_row is not None:
                selected_id = selected_row["_dlt_id"]

                # Store selection in session state
                st.session_state.selections[table.name] = selected_id

                if table.name == "json_errors":
                    # Display an inspect button
                    if st.button("Inspect File"):
                        source_file = selected_row.get("source_file")
                        source_path = selected_row.get("source_path")
                        if source_file and source_path:
                            full_path = Path(str(source_path)) / str(source_file)
                            try:
                                subprocess.run(
                                    ["code", str(full_path)],
                                    check=True,
                                    capture_output=True,
                                    text=True,
                                )
                            except (
                                FileNotFoundError,
                                subprocess.CalledProcessError,
                            ) as exc:
                                st.error(f"Unable to open file in VS Code: {exc}")
                        else:
                            st.warning(
                                "Selected error row does not include a source path and file name."
                            )

                # Show selected row details
                with st.expander("🔍 Selected Row Details", expanded=False):
                    st.json(selected_row.to_dict())

                # Get and render child tables
                children = get_child_tables(table.name)

                if children:
                    with st.container(border=True):
                        st.markdown(f"**Child Tables ({len(children)}):**")

                        # Use columns for multiple children
                        if len(children) <= 2:
                            cols = st.columns(len(children))
                            for idx, child in enumerate(children):
                                with cols[idx]:
                                    render_table_level(
                                        child,
                                        selected_row,
                                        level + 1,
                                        key_prefix=f"{key_prefix}_{selected_id}",
                                    )
                        else:
                            # Stack vertically for many children
                            for child in children:
                                render_table_level(
                                    child,
                                    selected_row,
                                    level + 1,
                                    key_prefix=f"{key_prefix}_{selected_id}",
                                )
            else:
                clear_selection_branch(table)

        # Main UI

        # Initialize session state for tracking selections
        if "selections" not in st.session_state:
            st.session_state.selections = {}

        st.title("Master-Detail Schema Explorer")

        with st.container(border=True):
            c1, c2 = st.columns(2)
            with c1:
                hide_empty_tables = st.checkbox("Hide Empty Tables", True)
            with c2:
                hide_dlt_columns = st.checkbox("Hide DLT columns (_dlt_id, etc)")

        # Main content area
        if st.session_state.root_table_selector:
            render_table_level(
                db.get_schema().get_table(st.session_state.root_table_selector),
                level=0,
                key_prefix="root",
            )
        else:
            st.info("Select a root table from the sidebar")

        # Footer showing current selection path
        if st.session_state.selections:
            st.divider()
            st.caption("Current Selection Path:")
            st.caption(
                " → ".join(
                    [
                        f"{k}: {v[:20]}..."
                        for k, v in st.session_state.selections.items()
                    ]
                )
            )


def main():
    page = LandingPage()
    page.page_content()


if __name__ == "__main__":
    main()
