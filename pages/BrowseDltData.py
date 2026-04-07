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

        def query_table(table_name, parent_row=None):
            """
            Query a table, optionally filtering by parent_id
            """
            parent_column = "_dlt_parent_id"
            parent_id = None

            table = db.get_schema().get_table(table_name)

            results = None
            if parent_row is None:
                # Root level query
                if table.search_field:
                    results = sf.search_raw_obs(table_name, table)
                else:
                    if any(col.extra.get("search") for col in table.columns.values()):
                        search_cols = {}
                        for k, v in table.columns.items():
                            if v.extra.get("search"):
                                search_cols[k] = st.checkbox(f"{k} is null", False)
                        filter = "true"
                        for k, v in search_cols.items():
                            if v:
                                filter += f" and {k} is null"
                        results = (
                            db.get_conn()
                            .execute(f"select * from {table_name} where {filter}")
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
                results = (
                    db.get_conn()
                    .execute(
                        f"SELECT * FROM {table_name} WHERE {parent_column} = ? LIMIT 100",
                        [parent_id],
                    )
                    .df()
                )

            return results

        def render_table_level(
            table: EnrichedTable, parent_row=None, level=0, key_prefix=""
        ):
            # "Recursively render table and its children"
            # Query the data
            df = query_table(table.name, parent_row)

            if df.empty:
                if not hide_empty_tables:
                    st.markdown(
                        f"{'  ' * level}### {'📄' if level == 0 else '📋'} {table.name}"
                    )
                    st.info(f"No data in {table.name}")
                return
            else:
                st.markdown(
                    f"{'  ' * level}### {'📄' if level == 0 else '📋'} {table.name} {'&nbsp;' * 10} _{len(df)} rows_",
                     unsafe_allow_html=True
                )

            # Show dataframe with selection enabled
            selection_key = f"{key_prefix}_{table.name}_{level}"

            column_config = {}
            if hide_dlt_columns:
                column_config = {col: None for col in df.columns if col.startswith('_dlt')}


            event = st.dataframe(
                df,
                width="stretch",
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key=selection_key,
                column_config=column_config
            )

            # Check if a row was selected
            if event.selection.rows:
                selected_idx = event.selection.rows[0]
                selected_row = df.iloc[selected_idx]
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
                with st.expander(f"🔍 Selected Row Details", expanded=False):
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
                # Clear selection if no row selected
                if table.name in st.session_state.selections:
                    del st.session_state.selections[table.name]

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
