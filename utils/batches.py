"""Batch-directory domain logic: naming convention, discovery, and listing."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import os
import re

import duckdb
import pandas as pd

import utils.db as db

# 20260326T153450Z_MAC -> ts=2026-03-26T15:34:50Z, utility_id=MAC
DIR_RE = re.compile(r"^(?P<ts>\d{8}T\d{6}Z)_(?P<utility_id>[^/]+)$")

# Columns produced by list_dirs; the empty frame must match so downstream
# SQL that references these columns binds even when nothing is found.
_DIR_COLUMNS = ["directory_path", "directory_name", "modified_time"]


@dataclass(frozen=True)
class BatchDir:
    path: Path
    utility_id: str
    ts_utc: datetime  # timezone-aware


def parse_batch_dir_name(name: str) -> tuple[datetime, str]:
    m = DIR_RE.match(name)
    if not m:
        raise ValueError(f"Unrecognized batch dir name: {name!r}")
    ts_utc = datetime.strptime(m.group("ts"), "%Y%m%dT%H%M%SZ").replace(
        tzinfo=timezone.utc
    )
    utility_id = m.group("utility_id")
    return ts_utc, utility_id


def parse_batch_dir(path: str | Path) -> BatchDir:
    p = Path(path)
    ts_utc, utility_id = parse_batch_dir_name(p.name)
    return BatchDir(path=p, utility_id=utility_id, ts_utc=ts_utc)


def list_dirs(directory_path: str) -> pd.DataFrame:
    empty_df = pd.DataFrame(columns=_DIR_COLUMNS)
    rows = []
    raw_path = (directory_path or "").strip()
    if not raw_path:
        return empty_df

    base_path = Path(raw_path).expanduser()

    if not base_path.exists() or not base_path.is_dir():
        return empty_df

    try:
        with os.scandir(base_path) as entries:
            for entry in entries:
                if entry.is_dir():
                    mtime_timestamp = entry.stat().st_mtime
                    mtime_readable = datetime.fromtimestamp(mtime_timestamp).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    rows.append(
                        {
                            "directory_path": str(base_path),
                            "directory_name": entry.name,
                            "modified_time": mtime_readable,
                        }
                    )

        return pd.DataFrame(rows, columns=_DIR_COLUMNS)

    except Exception as e:
        print(f"An error occurred: {e}")
        return empty_df


def list_all_batches(directory_path):
    all_batches_sql = """
    select b.*, d.*
    from silver.batch_info b
    full outer join dirs d on concat_ws('/',d.directory_path, d.directory_name)=regexp_replace(b.source, '/$', '')
    """
    # Note: dirs is indirectly referenced by the above SQL. DuckDB automagically maps pandas DF to a table.
    dirs = list_dirs(directory_path)
    # We'll register the df explicitly to satisfy linters
    duckdb.register("dirs", dirs)
    batches = db.get_conn().sql(all_batches_sql).df()
    return batches
