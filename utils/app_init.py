"""First-run onboarding: database initialization form and batch selection."""

from pathlib import Path
import os

import streamlit as st

import utils.db as db
from utils.batches import list_dirs
from utils.config import resolve_dlt_path, settings, update_eyeondata_toml
from utils.loader import load_me_some_data
from utils.st_widgets import select_rows


def init_app_form():
    """
    If no existing database is found, prompt for an initial batch of data to load allow user to specify DB location.
    """
    with st.form(key="init_db_form", width="stretch"):
        st.markdown("Initialize Database")
        utility_id = st.text_input(
            "Utility ID",
            value=str(getattr(settings.defaults, "utility_id", "") or ""),
        )
        batch_dir = st.text_input(
            "Dataset Path",
            value=str(getattr(settings.datasets, "dataset_path", "") or ""),
            placeholder="/path/to/eyeon_json_data",
        )

        selected_rows = batch_selector(batch_dir)

        database_path = st.text_input(
            "DB Directory path",
            value=str(getattr(settings.db, "db_path", "") or ""),
            placeholder="/path/to/database",
        )

        create_db_dir = st.checkbox(
            "Create DB directory if missing",
            value=False,
            help="If the directory does not exist, check this to allow the app to create it.",
        )

        submitted = st.form_submit_button("Submit")
        if submitted:
            errors: list[str] = []

            # Utility ID: required, string with no whitespace.
            utility_id_clean = (utility_id or "").strip()
            if not utility_id_clean:
                errors.append("Utility ID is required.")
            elif any(ch.isspace() for ch in utility_id_clean):
                errors.append("Utility ID must not contain spaces or other whitespace.")

            # JSON input directory: must exist and contain JSON files.
            batch_path = Path((batch_dir or "").strip()).expanduser()
            if not str(batch_path):
                errors.append("Dataset Path is required.")
            elif not batch_path.exists():
                errors.append(f"Dataset Path does not exist: {batch_path}")
            elif not batch_path.is_dir():
                errors.append(f"Dataset Path must be a directory: {batch_path}")

            # Must select at least 1 dataset dir
            if len(selected_rows) == 0:
                errors.append("Must select at least 1 Dataset to process")

            # DB directory: create if requested; must end up writable.
            db_dir_path = Path((database_path or "").strip()).expanduser()
            if not str(db_dir_path):
                errors.append("DB Directory path is required.")
            elif db_dir_path.exists() and not db_dir_path.is_dir():
                errors.append(f"DB Directory path must be a directory: {db_dir_path}")

            if not errors:
                if not db_dir_path.exists():
                    if not create_db_dir:
                        errors.append(
                            f"DB Directory path does not exist: {db_dir_path}. Check 'Create DB directory if missing' to create it."
                        )
                    else:
                        try:
                            db_dir_path.mkdir(parents=True, exist_ok=True)
                        except Exception as e:
                            errors.append(
                                f"Failed to create DB Directory path {db_dir_path}: {e}"
                            )

                if not errors:
                    if not db_dir_path.exists() or not db_dir_path.is_dir():
                        errors.append(
                            f"DB Directory path must be a directory: {db_dir_path}"
                        )
                    elif not os.access(str(db_dir_path), os.W_OK):
                        errors.append(
                            f"DB Directory path is not writable: {db_dir_path}"
                        )

            if errors:
                for msg in errors:
                    st.error(msg)
            else:
                # Persist to TOML so future runs pick these up as defaults.
                update_eyeondata_toml(
                    {
                        "db": {
                            "db_path": (database_path or "").strip(),
                        },
                        "datasets": {
                            "dataset_path": str(batch_path),
                        },
                        "defaults": {
                            "utility_id": utility_id_clean,
                        },
                    }
                )

                # Ensure the rest of this run uses the selected DB location.
                db_file = str(getattr(settings.db, "db_file", "eyeon.duckdb"))
                os.environ["EYEON_DUCKDB_PATH"] = str(
                    (resolve_dlt_path(db_dir_path) / db_file).resolve()
                )

                with st.spinner("Initializing..."):
                    db.init()
                    load_me_some_data(selected_rows)


def batch_selector(dataset_path: str) -> list[dict]:
    with st.container(border=True, width="stretch"):
        dataset_path = (dataset_path or "").strip()
        if not dataset_path:
            st.caption("Enter a dataset path to list available batch directories.")
            return []

        batch_dirs = list_dirs(dataset_path)
        if batch_dirs.empty:
            st.info("No batch directories found for the selected dataset path.")
            return []

        return select_rows(batch_dirs, key="dataset_dirs")
