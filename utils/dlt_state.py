"""Consistency layer between the three stores of DLT schema state.

DLT state lives in three places that must move together:

1. The local pipeline working dir (``~/.dlt/pipelines/eyeon_metadata/``):
   live schema, pipeline state, and pending load packages.
2. The destination's dlt metadata tables (``_dlt_version``, ``_dlt_loads``,
   ``_dlt_pipeline_state``) inside the DuckDB file.
3. The physical tables in the DuckDB file.

On a dev machine the DuckDB file is routinely deleted and re-bootstrapped
from the intentionally-stale ``schemas/schema.sql`` base schema while the
pipeline working dir survives. A pending load package from the old database
skips DLT's reflect-and-migrate step on retry, so its merge SQL fails with a
Binder Error on columns the re-bootstrapped tables lack.

This module heals physical drift before every load, detects a replaced
database via an instance-identity marker, and records every action in
``_meta.consistency_log`` so UIs can surface the history with plain SQL.

See wiki/work/dlt-state-consistency/brief.md for the full design.

This module must stay importable without Streamlit.
"""

import json
import logging
import uuid as uuid_module
from pathlib import Path

logger = logging.getLogger(__name__)

INSTANCE_MARKER_FILENAME = "eyeon_db_instance"


def _duckdb_type(dlt_type: str | None) -> str:
    # Minimal mapping; anything unknown becomes VARCHAR.
    match (dlt_type or "").lower():
        case "text" | "varchar" | "string":
            return "VARCHAR"
        case "bigint" | "int" | "integer":
            return "BIGINT"
        case "double" | "float":
            return "DOUBLE"
        case "bool" | "boolean":
            return "BOOLEAN"
        case "timestamp" | "timestamp_tz" | "datetime":
            return "TIMESTAMP"
        case "json":
            return "JSON"
        case _:
            return "VARCHAR"


def _q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ensure_meta_tables(conn) -> str:
    """Create the `_meta` schema and its tables; return this DB's instance id.

    `_meta.db_instance` holds a single row identifying this physical database
    file. `_meta.consistency_log` is an append-only event trail (see
    `log_event`) that UIs can surface with plain SQL.
    """
    conn.execute("create schema if not exists _meta")
    conn.execute(
        """
        create table if not exists _meta.db_instance (
            uuid VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    conn.execute(
        """
        create table if not exists _meta.consistency_log (
            ts TIMESTAMP DEFAULT current_timestamp,
            event VARCHAR NOT NULL,
            detail JSON
        )
        """
    )
    row = conn.execute("select uuid from _meta.db_instance limit 1").fetchone()
    if row:
        return row[0]
    instance_id = str(uuid_module.uuid4())
    conn.execute("insert into _meta.db_instance (uuid) values (?)", [instance_id])
    return instance_id


def log_event(conn, event: str, detail: dict | None = None) -> None:
    """Append one event to `_meta.consistency_log`."""
    conn.execute(
        "insert into _meta.consistency_log (event, detail) values (?, ?)",
        [event, json.dumps(detail or {})],
    )


def _reflect_dataset(conn, dataset_name: str) -> dict[str, set[str]]:
    """Physical tables and columns currently in `dataset_name`."""
    rows = conn.execute(
        """
        select c.table_name, c.column_name
        from information_schema.columns c
        join information_schema.tables t
          on t.table_schema = c.table_schema and t.table_name = c.table_name
        where c.table_schema = ? and t.table_type = 'BASE TABLE'
        """,
        [dataset_name],
    ).fetchall()
    reflected: dict[str, set[str]] = {}
    for table, column in rows:
        reflected.setdefault(table, set()).add(column)
    return reflected


def root_table_name(tables: dict, name: str) -> str:
    """Walk the `parent` chain of a DLT table up to its root table."""
    seen: set[str] = set()
    while True:
        parent = (tables.get(name) or {}).get("parent")
        if not parent or name in seen:
            return name
        seen.add(name)
        name = str(parent)


def schema_root_tables(schema) -> set[str]:
    """All root (parent-less) table names in the schema, minus DLT internals."""
    tables = getattr(schema, "tables", {}) or {}
    return {
        root_table_name(tables, str(name))
        for name in tables
        if not str(name).startswith("_dlt")
    }


def compute_drift(
    schema,
    conn,
    dataset_name: str,
    roots: set[str] | None = None,
    columns_only: bool = False,
) -> dict:
    """Diff the DLT schema against the physical tables in `dataset_name`.

    Returns {"missing_tables": [...], "missing_columns": {table: [cols]}}.
    Only complete columns (those with a data_type, i.e. materialized by DLT)
    are considered; `_dlt*` bookkeeping tables are DLT's own to manage.

    The single `eyeon_metadata` schema spans both the bronze and silver
    datasets, so pass `roots` (root table names that live in this dataset)
    to avoid reporting bronze tables as "missing" from silver and vice
    versa. `None` means consider every table.

    `columns_only` skips missing-table detection: staging datasets only ever
    hold the merge-chain tables DLT materializes itself, so an absent table
    there is normal — only stale columns on existing tables are drift.
    """
    reflected = _reflect_dataset(conn, dataset_name)
    tables = getattr(schema, "tables", {}) or {}
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}

    for table_name, table_def in tables.items():
        table_name = str(table_name)
        if table_name.startswith("_dlt"):
            continue
        if roots is not None and root_table_name(tables, table_name) not in roots:
            continue
        cols_def = {
            str(name): col
            for name, col in ((table_def or {}).get("columns", {}) or {}).items()
            if col.get("data_type")
        }
        if not cols_def:
            continue
        if table_name not in reflected:
            if not columns_only:
                missing_tables.append(table_name)
            continue
        absent = [c for c in cols_def if c not in reflected[table_name]]
        if absent:
            missing_columns[table_name] = absent

    return {"missing_tables": missing_tables, "missing_columns": missing_columns}


def ensure_destination_tables(
    schema,
    conn,
    dataset_name: str,
    roots: set[str] | None = None,
    columns_only: bool = False,
) -> dict:
    """Create missing tables / add missing columns for `schema` in `dataset_name`.

    This is the self-heal for physical drift: DLT only migrates the
    destination when it sees a schema *version* change, so a table that
    drifted (stale schema.sql bootstrap, retried pending package) is never
    repaired by DLT itself. All DDL here is add-only and idempotent.

    Takes the DLT `Schema` object directly — a previous version of this
    guard looked up `pipeline.schemas[dataset_name]`, but that mapping is
    keyed by schema name, so the lookup always failed and the guard silently
    no-oped.

    Returns the changes made (empty dict values when the dataset was healthy).
    """
    conn.execute(f"create schema if not exists {_q_ident(dataset_name)}")
    drift = compute_drift(schema, conn, dataset_name, roots=roots, columns_only=columns_only)
    tables = getattr(schema, "tables", {}) or {}

    for table_name in drift["missing_tables"]:
        cols_def = (tables[table_name] or {}).get("columns", {}) or {}
        col_sql = []
        for col_name, col in cols_def.items():
            if not col.get("data_type"):
                continue
            dtype = _duckdb_type(col.get("data_type"))
            nullable = col.get("nullable")
            null_sql = "" if nullable or nullable is None else " NOT NULL"
            col_sql.append(f"{_q_ident(str(col_name))} {dtype}{null_sql}")
        conn.execute(
            f"create table if not exists {_q_ident(dataset_name)}.{_q_ident(table_name)}"
            f" ({', '.join(col_sql)})"
        )

    for table_name, col_names in drift["missing_columns"].items():
        cols_def = (tables[table_name] or {}).get("columns", {}) or {}
        for col_name in col_names:
            dtype = _duckdb_type(cols_def[col_name].get("data_type"))
            conn.execute(
                f"alter table {_q_ident(dataset_name)}.{_q_ident(table_name)}"
                f" add column {_q_ident(col_name)} {dtype}"
            )

    if drift["missing_tables"] or drift["missing_columns"]:
        logger.warning(
            "Healed physical drift in dataset %s: created tables %s, added columns %s",
            dataset_name,
            drift["missing_tables"],
            drift["missing_columns"],
        )
        log_event(conn, "tables_healed", {"dataset": dataset_name, **drift})
    return drift


def _pending_load_ids(pipeline) -> list[str]:
    """Load ids of all pending (extracted or normalized) packages."""
    pending: list[str] = []
    for lister in (
        pipeline.list_extracted_load_packages,
        pipeline.list_normalized_load_packages,
    ):
        try:
            pending.extend(lister())
        except Exception:
            # Storage not initialized yet (fresh pipeline) — nothing pending.
            pass
    return pending


def _instance_marker_path(pipeline) -> Path:
    return Path(pipeline.working_dir) / INSTANCE_MARKER_FILENAME


def reconcile_db_instance(pipeline, conn) -> str:
    """Detect a database replaced under a live pipeline, and recover.

    Compares the database's instance id (`_meta.db_instance`) with the
    sidecar marker in the pipeline working dir. On mismatch the pending load
    packages belong to a database that no longer exists: they are dropped
    (their source JSON batches remain re-loadable), the event is logged to
    `_meta.consistency_log`, and the marker is updated.

    Returns "match", "first_contact", or "mismatch".
    """
    db_instance = ensure_meta_tables(conn)
    marker_path = _instance_marker_path(pipeline)
    local_instance = (
        marker_path.read_text(encoding="utf-8").strip() if marker_path.exists() else None
    )

    if local_instance == db_instance:
        return "match"

    pending = _pending_load_ids(pipeline)

    if local_instance is None:
        # First contact between this pipeline dir and an instance-stamped DB.
        # Pending packages cannot be proven orphaned, so never drop them here.
        if pending:
            logger.warning(
                "No DB instance marker for pipeline %s but %d pending load "
                "package(s) exist (%s). If the database was recently replaced, "
                "inspect with `load_eyeon.py --doctor` before loading.",
                pipeline.pipeline_name,
                len(pending),
                pending,
            )
            log_event(
                conn,
                "db_instance_marker_created",
                {"instance": db_instance, "pending_load_ids": pending},
            )
        marker_path.write_text(db_instance, encoding="utf-8")
        return "first_contact"

    logger.warning(
        "Database instance changed since this pipeline last ran "
        "(old=%s, new=%s). Dropping %d pending load package(s) built against "
        "the previous database: %s. Re-load those batches from their source "
        "directories.",
        local_instance,
        db_instance,
        len(pending),
        pending or "none",
    )
    if pending:
        pipeline.drop_pending_packages(with_partial_loads=True)
    log_event(
        conn,
        "db_instance_changed",
        {
            "old_instance": local_instance,
            "new_instance": db_instance,
            "dropped_load_ids": pending,
        },
    )
    marker_path.write_text(db_instance, encoding="utf-8")
    return "mismatch"


def refresh_instance_marker(pipeline, conn) -> None:
    """Stamp the DB (if needed) and point the sidecar marker at it."""
    _instance_marker_path(pipeline).write_text(ensure_meta_tables(conn), encoding="utf-8")


def dataset_exists(conn, dataset_name: str) -> bool:
    return (
        conn.execute(
            "select 1 from information_schema.schemata where schema_name = ?",
            [dataset_name],
        ).fetchone()
        is not None
    )


def doctor_report(
    pipeline, conn, db_path: str, dataset_roots: dict[str, set[str] | None] | None = None
) -> str:
    """Human-readable comparison of the three stores of DLT state.

    `dataset_roots` maps each dataset to the root tables that live in it
    (None value = consider all tables). Read-only apart from creating the
    `_meta` tables if absent.
    """
    if dataset_roots is None:
        dataset_roots = {"bronze": None, "silver": None}
    lines = ["=== EyeON DLT state doctor ===", f"database: {db_path}"]

    db_instance = ensure_meta_tables(conn)
    marker_path = _instance_marker_path(pipeline)
    local_instance = (
        marker_path.read_text(encoding="utf-8").strip() if marker_path.exists() else None
    )
    verdict = (
        "consistent"
        if local_instance == db_instance
        else ("no marker (first contact)" if local_instance is None else "MISMATCH")
    )
    lines += [
        "",
        "-- instance identity --",
        f"db instance:      {db_instance}",
        f"pipeline marker:  {local_instance or '<absent>'}  [{verdict}]",
        f"pipeline dir:     {pipeline.working_dir}",
    ]

    lines += ["", "-- local pipeline schema --"]
    if pipeline.default_schema_name:
        schema = pipeline.default_schema
        lines.append(
            f"schema: {schema.name}  version={schema.version}  hash={schema.version_hash}"
        )
    else:
        schema = None
        lines.append("schema: <none — pipeline has not run yet>")

    pending = _pending_load_ids(pipeline)
    lines += ["", "-- pending load packages --"]
    lines.append(f"{len(pending)} pending: {pending or '<none>'}")

    for dataset, roots in dataset_roots.items():
        lines += ["", f"-- dataset: {dataset} --"]
        if not dataset_exists(conn, dataset):
            lines.append("dataset not present in database")
            continue
        try:
            version, version_hash = conn.execute(
                f"select version, version_hash from {_q_ident(dataset)}._dlt_version"
                " order by inserted_at desc limit 1"
            ).fetchone()
            loads = conn.execute(
                f"select count(*) from {_q_ident(dataset)}._dlt_loads"
            ).fetchone()[0]
            lines.append(
                f"destination schema: version={version}  hash={version_hash}  loads={loads}"
            )
        except Exception:
            lines.append("destination schema: no _dlt_version / _dlt_loads records")
        if schema is not None:
            for target in (dataset, f"{dataset}_staging"):
                if target != dataset and not dataset_exists(conn, target):
                    continue
                drift = compute_drift(
                    schema, conn, target, roots=roots, columns_only=target != dataset
                )
                if drift["missing_tables"] or drift["missing_columns"]:
                    lines.append(
                        f"PHYSICAL DRIFT in {target}: missing tables "
                        f"{drift['missing_tables'] or 'none'}, missing columns "
                        f"{drift['missing_columns'] or 'none'}"
                    )
                else:
                    lines.append(f"physical tables in {target}: in sync with schema")

    lines += ["", "-- recent consistency events (_meta.consistency_log) --"]
    events = conn.execute(
        "select ts, event, detail from _meta.consistency_log order by ts desc limit 10"
    ).fetchall()
    if not events:
        lines.append("<none>")
    for ts, event, detail in events:
        lines.append(f"{ts}  {event}  {detail}")

    return "\n".join(lines)
