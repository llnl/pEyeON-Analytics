import streamlit as st

from utils.metadata_catalog import MetadataCatalog
from utils.queries import Query

catalog = MetadataCatalog()
q = Query()


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
                ["any", "no_metadata"]
                + ["unknown"]
                + [k for k in catalog.curated_type_keys() if k not in {"unknown"}],
                key=f"{widget_prefix}_metadata",
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
            if filter_metadata in {"any", "no_metadata"}:
                union_sql = catalog.uuid_union_sql()
                if union_sql is None:
                    # No metadata tables exist in the DB yet.
                    if filter_metadata == "any":
                        conditions.append("false")
                    else:
                        conditions.append("true")
                else:
                    op = "in" if filter_metadata == "any" else "not in"
                    conditions.append(f"uuid {op} ({union_sql})")
            else:
                silver_table = catalog.silver_table_for(filter_metadata)
                if silver_table:
                    conditions.append(f"uuid in (select uuid from silver.{silver_table})")

        sql = f"SELECT * FROM {table_name}"
        if len(conditions) > 0:
            where_clause = " and ".join([f"({c})" for c in conditions])
            sql += f" where {where_clause}"
        results = q.df(sql)

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
                q.scalar("select count(*) from silver.raw_obs"),
            )

        with col2:
            # Metadata types with loaded data, simplified for display.
            st.metric("Types", ", ".join(catalog.loaded_type_names()))
