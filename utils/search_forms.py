import utils.db as db
import streamlit as st


def _parse_stg_metadata_model_to_type_key(model_name: str) -> str | None:
    # gold_staging.stg_metadata_<type>[_file]
    if not model_name.startswith("stg_metadata_"):
        return None
    raw = model_name.removeprefix("stg_metadata_")
    if raw.endswith("_file"):
        raw = raw.removesuffix("_file")
    return raw


@st.cache_data(show_spinner=False)
def _curated_metadata_type_keys() -> list[str]:
    """Curated/known types for analytics.

    Canonical source: dbt staging models materialized into `gold_staging`.
    Falls back to a small static list if dbt hasn't run yet.
    """
    fallback = [
        "binwalk",
        "coff",
        "container",
        "device_tree",
        "elf",
        "error",
        "generic",
        "java",
        "js",
        "mach_o",
        "native_lib",
        "ole",
        "opkg",
        "pe",
        "symlink",
        "text",
        "uimage",
    ]

    try:
        rows = db.get_conn().execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = 'gold_staging'
              and left(table_name, 13) = 'stg_metadata_'
            order by table_name
            """
        ).fetchall()
    except Exception:
        return fallback

    keys: set[str] = set()
    for (table_name,) in rows:
        key = _parse_stg_metadata_model_to_type_key(str(table_name))
        if key:
            keys.add(key)

    # Historical naming mismatch: model drops `_file`, silver table keeps it.
    if "native_lib" not in keys and any(
        str(t[0]) == "stg_metadata_native_lib" for t in rows
    ):
        keys.add("native_lib")

    return sorted(keys)


@st.cache_data(show_spinner=False)
def _silver_metadata_base_tables() -> list[str]:
    """Top-level `silver.metadata_*` tables that represent per-file metadata types."""
    rows = db.get_conn().execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'silver'
          and left(table_name, 9) = 'metadata_'
          and instr(table_name, '__') = 0
        order by table_name
        """
    ).fetchall()
    return [str(r[0]) for r in rows]


def _silver_table_for_type_key(type_key: str) -> str | None:
    """Map dropdown key -> silver base table name."""
    if type_key == "error":
        return "metadata_error"
    if type_key == "unknown":
        return "metadata_unknown"
    if type_key == "native_lib":
        return "metadata_native_lib_file"
    # Most types follow the `metadata_<type>_file` convention.
    return f"metadata_{type_key}_file"


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
                + [k for k in _curated_metadata_type_keys() if k not in {"unknown"}],
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
                base_tables = _silver_metadata_base_tables()
                if not base_tables:
                    # No metadata tables exist in the DB yet.
                    if filter_metadata == "any":
                        conditions.append("false")
                    else:
                        conditions.append("true")
                else:
                    union_sql = "\nunion all\n".join(
                        [f"select uuid from silver.{t}" for t in base_tables]
                    )
                    op = "in" if filter_metadata == "any" else "not in"
                    conditions.append(f"uuid {op} ({union_sql})")
            else:
                silver_table = _silver_table_for_type_key(filter_metadata)
                if silver_table:
                    conditions.append(f"uuid in (select uuid from silver.{silver_table})")

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
            if tables is None:
                type_names = ["_None_"]
            else:
                type_names = [
                    s.removeprefix("metadata_").removesuffix("_file") for s in tables
                ]
            st.metric("Types", f"{', '.join(type_names)}")
