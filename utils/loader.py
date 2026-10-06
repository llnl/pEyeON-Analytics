"""Pipeline orchestration from the app: DLT load + dbt run."""

import os

import streamlit as st
from dbt.cli.main import dbtRunner, dbtRunnerResult

import load_eyeon
from utils.batches import parse_batch_dir
from utils.config import duckdb_path, settings


def load_me_some_data(selected_rows: list[dict]) -> None:
    """Hook for loading selected batch rows"""
    with st.status("Processing data...", expanded=True) as status:
        for row in selected_rows:
            full_path = os.path.join(row["directory_path"], row["directory_name"])
            st.write(f"Loading using DLT: {full_path}")
            batch_info = parse_batch_dir(row["directory_name"])
            load_data(full_path, batch_info.utility_id)
        # DBT only needs to be run once for all batches
        st.write("Running DBT...")
        run_dbt()
        # New batches can introduce new metadata types/tables; drop cached
        # discovery (e.g. MetadataCatalog) so pages see them immediately.
        st.cache_data.clear()
        status.update(label="Processing complete!", state="complete", expanded=False)
        st.rerun()


def run_dbt():
    # Initialize the runner
    dbt = dbtRunner()

    # Ensure dbt points at the same DuckDB file as the app/DLT.
    os.environ["EYEON_DUCKDB_PATH"] = str(duckdb_path())

    # Define CLI arguments as a list of strings
    cli_args = [
        "run",
        "--project-dir",
        "dbt_eyeon_gold",
        "--profiles-dir",
        "dbt_eyeon_gold",
    ]

    # Invoke the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # Inspect the results
    if res.success:
        for r in res.result:
            print(f"Node {r.node.name} finished with status: {r.status}")
    else:
        print("dbt execution failed.")


def load_data(batch_dir: str, utility_id=None):
    utility_id = utility_id or settings.defaults.utility_id
    load_eyeon.main(utility_id=utility_id, source=batch_dir)
