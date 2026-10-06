import streamlit as st
import duckdb
import dlt
import utils.dlt_state as dlt_state
import utils.schema_ext as schema_ext
from pathlib import Path
from utils.config import duckdb_path, resolve_dlt_path


def db_path():
    return str(duckdb_path())


def exists() -> bool:
    return Path(db_path()).exists()


@st.cache_resource
def _get_conn():
    return duckdb.connect(db_path())


def get_conn(schema="silver"):
    """
    Returns a shared DB connection and switches to the requested schema.
    Silver is the default as it is generally used the most.
    """
    conn = _get_conn()
    if schema:
        conn.execute(f"use {schema}")
    return conn


def init():
    "Initialize a new database instance."
    # Get db conn...
    sql_file = resolve_dlt_path("schemas/schema.sql")

    con = duckdb.connect(db_path())

    with open(sql_file, "r", encoding="utf-8") as f:
        ddl_sql = f.read()
    statements = con.extract_statements(ddl_sql)
    for statement in statements:
        con.execute(statement)

    # Stamp this database file with an instance id and record the bootstrap,
    # so a later "DB replaced under a live pipeline" state is detectable
    # (see utils/dlt_state.py and wiki/work/dlt-state-consistency/brief.md).
    dlt_state.ensure_meta_tables(con)
    dlt_state.log_event(con, "db_initialized", {"source": str(sql_file)})
    con.close()


# Load schema (cached)
@st.cache_resource
def get_schema():
    # Attaching to the pipeline gives us access to the file-based metadata DLT manages, such as the most current schema.
    pipeline = dlt.attach(pipeline_name="eyeon_metadata")
    schema = schema_ext.build_schema(
        pipeline, str(resolve_dlt_path("schemas/eyeon_schema_overlay.yaml"))
    )
    return schema
