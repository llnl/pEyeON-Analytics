import logging
from duckdb import CatalogException

import duckdb
import streamlit as st
import common.rawutil as ru
from utils.config import settings


def _getdbcon(file=":memory:"):
    # Create and attach to a memory instance
    # Note: For now, using a memory instance is an easy way to provide multiple web sessions.
    con = duckdb.connect(file)
    # List of markdown text for feedback
    try:
        msgs = createviews(con)
    except duckdb.IOException as ioe:
        errmsg = f":exclamation: Database Create Failed {file}: {md_code(ioe)}\\\n"
        con = None
        msgs = msgs + errmsg
    return con, msgs


def createviews(con):
    # Get the database filename. Returns "memory" if not filebased.
    msgs = f":smile: Database: {con.sql('SELECT database_name FROM duckdb_databases').fetchone()[0]}\n"
    # Fix: ,'metadata'
    try:
        for table_type in ["observations", "defaults", "sigs_n_certs", "metadata"]:
            # Create views for the base tables
            print(f". Creating: {table_type} using path: ")
            con.sql(
                f"create or replace view {table_type} as from read_parquet('{settings.datasets.dataset_path}/{table_type}/**/*.parquet',union_by_name=true)"
            )
        # Build a summary view for unique certs
        con.sql(
            "create or replace view raw_uniq_certs as from (SELECT *, count(*) num_rows from sigs_n_certs group by all)"
        )
        # Build a view for raw JSON
        createrawjsonview(con)
    except duckdb.IOException as ioe:
        errmsg = f":exclamation: View Create Failed {con}: {md_code(ioe)}\\\n"
        msgs = msgs + errmsg
    return msgs


# For now, caching is performed using session_state.
# @st.cache_resource
def opendb():
    """
    Open the dbfile read-only and create a memory instance that will host the analytic views/tables.
    Use session_state.con to cache the db instance.
    """
    con, msgs = None, None
    if "con" in st.session_state:
        # Same db, return the cached con
        con = st.session_state["con"]
        msgs = st.session_state["msgs"]
        logging.debug("Using cached con in session_state")
    else:
        # New connection
        print("  Opening db - init")
        con, msgs = _getdbcon()
        _savecon(con, msgs, ":memory:")
    return con, msgs


def _savecon(con, msgs, filename):
    # Save db con and filename in session_state
    st.session_state["con"] = con
    st.session_state["filename"] = filename
    st.session_state["msgs"] = msgs
    logging.debug("Saved in session_state")


def getcon():
    con = None
    if "con" in st.session_state:
        con = st.session_state.con
    else:
        # Redirect to Welcome page to initialize db connection
        print("No DB defined, redirecting to main.py")
        st.switch_page("main.py")
    return con


def md_code(str):
    """
    Wrap the sring in code markdown
    """
    return f"\n```\n{str}\n```\n"


def createrawjsonview(con):
    # Build a view for raw JSON
    sql = getsqlstmt("utils/raw_json.sql", "raw_json").sql
    # TODO: Implement a parameter replacement strategy!
    sql = sql.replace("{dataset}", settings.datasets.dataset_path)
    con.sql(sql)
    # Sometimes, columns are missing!!! Add them if needed.
    # TODO: can't alter a view. What to do when incoming schema isn't complete? Missing fields should be OK, but what if there is a new field never seen?
    # con.sql('alter view raw_json add column if not exists metadata varchar')

    # Build a summary view of batches. Using table rather than a view to improve performance.
    con.sql(
        "create or replace table batches as select batch_id, location_pk, count(*) num_rows from raw_json group by all"
    )
    try:
        # In the case of no existing data at all, this will fail as there is no observations table. It also isn't critical, so just ignore.
        con.sql(
            "create or replace view new_batches as select b.* from batches b anti join (select distinct batch_id, location_pk from observations) using (batch_id, location_pk)"
        )
    except CatalogException as e:
        print(f"Warning: No existing observations table: \n{e}")


def convert_from_json(con):
    """
    Read all JSON file in source and write as parquet partitioned by "location_pk" in dest.
    Note: An upload may provide a location which will override the Default value in the JSON.
    """
    createrawjsonview(con)

    # The ETL views have an "etl_" prefix as they are different from the resulting, converted views.
    ru.run_sql_no_args(con, "utils/etl.sql")
    # Now, copy to parquet files, fix: 'metadata',
    for table in ["observations", "defaults", "sigs_n_certs", "metadata"]:
        # Add ETL_ prefix to table names.
        try:
            print(f"Writing {table}")
            con.sql(
                f"copy etl_{table} to '{settings.datasets.dataset_path}/{table}' (format parquet, OVERWRITE_OR_IGNORE, partition_by (location_pk, batch_id))"
            )
        except duckdb.duckdb.CatalogException as e:
            logging.error(f"Error copying {table}: \n{e}")


def getsqlstmt(filename, name) -> ru.SqlStmt:
    sqls = ru.loadSqlStatements(filename)
    try:
        sql = next(x for x in sqls if x.name == name)
    except StopIteration:
        error = f"SQL statement not found: {name} in {filename}"
        logging.error(error)
        st.write(error)
    return sql


def get(con, sqlname: str, queryfile="queries"):
    sqlstmt = getsqlstmt(f"common/{queryfile}.sql", sqlname)
    return getonerow(con, sqlstmt.sql)


def getonerow(con, sql: str):
    return con.sql(sql).fetchone()


def getdatafor(con, sqlname: str, queryfile="queries"):
    return getdata(con, getsqlstmt(f"common/{queryfile}.sql", sqlname).sql)


def getdata(con, sql: str):
    df = None
    try:
        df = con.execute(sql).fetchdf()
    except CatalogException as e:
        logging.warning(
            f"Warning! SQL Failed due to missing object:\n {sql} due to:\n{e}"
        )
    return df
