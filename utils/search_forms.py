import utils.db as db
import streamlit as st
from utils.config import settings


def search_raw_obs(table_name, table, key_prefix=""):
    """
    Display a form for searching the Observations table
    Default is to AND conditions together
    """
    widget_prefix = f"{key_prefix}_{table_name}_search"

    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            filter_uuid = st.text_input("UUID", key=f"{widget_prefix}_uuid")
            filter_text = st.text_input(
                f"Filter on: {table.search_field}",
                placeholder="Use % or * for wildcard (case insensitive)",
                key=f"{widget_prefix}_text",
            )
        with c2:
            filter_metadata = st.selectbox(
                "Metadata Type",
                [
                    "any",
                    "coff",
                    "elf",
                    "java",
                    "mach_o",
                    "native_lib",
                    "ole",
                    "pe",
                    "uimage",
                    "unknown",
                    "error"
                ],
                key=f"{widget_prefix}_metadata",
            )
            # Find an example file. Present a list of metadata types (that exist in this dataset) and then randomly pick one.
            filter_ignore_unknown = st.checkbox(
                "Ignore observations with unknown/no metadata",
                key=f"{widget_prefix}_ignore_unknown",
            )

        raw_obs_summary()

        # Build up a complete SQL WHERE clause
        # AND conditions together
        conditions = []
        if filter_uuid:
            conditions.append(f"uuid ilike '%{filter_uuid.replace('*', '%')}%'")

        if filter_text:
            conditions.append(f"filename ilike '%{filter_text.replace('*', '%')}%'")

        if filter_metadata:
            md_table = f"metadata_{filter_metadata}_file"
            if filter_metadata == "unknown":
                # Filter to observations with NO metadata defined of any type
                conditions.append("uuid not in (select uuid from gold.all_metadata)")
            elif filter_metadata == "any":
                # Filter to observations with any metadata type defined
                conditions.append("uuid in (select uuid from gold.all_metadata)")
            elif filter_metadata == "error":
                # This one doesn't have the "_file" suffix, just hard code the name
                conditions.append("uuid in (select uuid from silver.metadata_error)")
            else:
                # Filter the specific metadata type selected
                conditions.append(f"uuid in (select uuid from silver.{md_table})")

        sql = f"SELECT * FROM {table_name}"
        if len(conditions) > 0:
            where_clause = " and ".join([f"({c})" for c in conditions])
            sql += f" where {where_clause}"
        results = db.get_conn().execute(sql).df()

    return results, sql


def raw_obs_summary():
    """
    Display a summary about the Observations
    """
    with st.container(border=True):
        st.markdown("For all observations:")
        col1, col2 = st.columns([0.2, 0.8])

        # High level stats
        with col1:
            st.metric(
                "Observations",
                db.get_conn()
                .execute("select count(*) from silver.raw_obs")
                .fetchone()[0],
            )
            # st.dataframe( db.get_conn().execute('select count(*) from silver.raw_obs').df())

        with col2:
            # Get the list of tables with data, then simplify the names for display.
            tables = (
                db.get_conn()
                .execute(
                    "select list_sort(list(distinct _metadata_table_name)) from gold.all_metadata"
                )
                .fetchone()[0]
            )
            if tables == None:
                type_names = ["_None_"]
            else:
                type_names = [
                    s.removeprefix("metadata_").removesuffix("_file") for s in tables
                ]
            st.metric("Types", f"{', '.join(type_names)}")
