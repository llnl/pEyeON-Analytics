"""Regression tests for the DLT three-store state consistency layer.

Reproduces the 2026-08-31 incident: a dev DuckDB file deleted and
re-bootstrapped from a stale schema.sql while the DLT pipeline working dir
(with its evolved schema and pending load packages) survives, causing merge
SQL to fail with a Binder Error on columns the recreated tables lack.

See wiki/work/dlt-state-consistency/brief.md.
"""

import json
import uuid as uuid_module
from types import SimpleNamespace

import duckdb
import pytest

import load_eyeon
import utils.dlt_state as dlt_state

CERTS_TABLE = "raw_obs__signatures__certs"


def _write_sample(source_dir):
    obs = {
        "uuid": str(uuid_module.uuid4()),
        "filename": "sample.exe",
        "bytecount": 1234,
        "md5": "d41d8cd98f00b204e9800998ecf8427e",
        "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "magic": "PE32 executable",
        "filetype": ["pe"],
        "signatures": [
            {
                "verified": "OK",
                "certs": [
                    {
                        "issuer_name": "Test CA",
                        "subject_name": "Test Subject",
                        "rfc822_name": "signer@example.com",
                    }
                ],
            }
        ],
    }
    (source_dir / "sample.json").write_text(json.dumps(obs))


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Isolate the DB, the DLT home dir, and schema exports in tmp_path."""
    db_path = tmp_path / "db" / "eyeon.duckdb"
    db_path.parent.mkdir()
    source_dir = tmp_path / "batch"
    source_dir.mkdir()
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_home"))
    monkeypatch.setenv("EYEON_DUCKDB_PATH", str(db_path))
    # Keep pipeline schema exports out of the repo's schemas/ directory.
    monkeypatch.setattr(load_eyeon, "resolve_dlt_path", lambda p: tmp_path / p)
    _write_sample(source_dir)
    return SimpleNamespace(db_path=db_path, source=str(source_dir))


def _silver_columns(conn, table):
    return {
        r[0]
        for r in conn.execute(
            "select column_name from information_schema.columns"
            " where table_schema = 'silver' and table_name = ?",
            [table],
        ).fetchall()
    }


def _events(conn):
    return {r[0] for r in conn.execute("select event from _meta.consistency_log").fetchall()}


def test_ensure_tables_heals_dropped_column(env):
    """A drifted table is repaired before the load instead of failing it.

    Without the heal this reproduces the incident mechanism exactly: the
    schema version hash is already recorded in _dlt_version, so DLT skips
    migration and the raw_obs merge SQL binder-errors on the missing column.
    """
    load_eyeon.main(utility_id="t1", source=env.source)

    conn = duckdb.connect(str(env.db_path))
    assert conn.execute("select count(*) from silver.raw_obs").fetchone()[0] == 1
    assert "rfc822_name" in _silver_columns(conn, CERTS_TABLE)
    conn.execute(f"alter table silver.{CERTS_TABLE} drop column rfc822_name")
    conn.close()

    load_eyeon.main(utility_id="t1", source=env.source)

    conn = duckdb.connect(str(env.db_path))
    assert "rfc822_name" in _silver_columns(conn, CERTS_TABLE)
    assert "tables_healed" in _events(conn)
    conn.close()


def test_db_replaced_under_pipeline_is_reconciled(env):
    """The incident: DB deleted + stale re-bootstrap + surviving pending package."""
    load_eyeon.main(utility_id="t1", source=env.source)

    # Leave a pending package behind, simulating an interrupted load.
    conn = duckdb.connect(str(env.db_path))
    pipeline = load_eyeon._build_pipeline(conn)
    pipeline.extract(
        load_eyeon.eyeon_source("t1", env.source, 4).with_resources("files_resource")
    )
    assert pipeline.has_pending_data
    conn.close()

    # Simulate the dev reset: delete the DB and re-bootstrap a stale-shaped
    # certs table (schema.sql vintage: no rfc822_name), keeping ~/.dlt as-is.
    env.db_path.unlink()
    conn = duckdb.connect(str(env.db_path))
    conn.execute("create schema silver")
    conn.execute(
        f"""
        create table silver.{CERTS_TABLE} (
            issuer_name VARCHAR, subject_name VARCHAR,
            _dlt_root_id VARCHAR, _dlt_parent_id VARCHAR,
            _dlt_list_idx BIGINT, _dlt_id VARCHAR
        )
        """
    )
    conn.close()

    # Must load cleanly: instance mismatch detected, orphaned package dropped,
    # stale table healed.
    load_eyeon.main(utility_id="t1", source=env.source)

    conn = duckdb.connect(str(env.db_path))
    events = _events(conn)
    assert "db_instance_changed" in events
    assert "rfc822_name" in _silver_columns(conn, CERTS_TABLE)
    assert conn.execute("select count(*) from silver.raw_obs").fetchone()[0] == 1

    pipeline = load_eyeon._build_pipeline(conn)
    assert not pipeline.has_pending_data
    conn.close()


def test_doctor_reports_drift_and_events(env):
    load_eyeon.main(utility_id="t1", source=env.source)

    conn = duckdb.connect(str(env.db_path))
    pipeline = load_eyeon._build_pipeline(conn)
    roots = load_eyeon._doctor_dataset_roots(pipeline)
    healthy = dlt_state.doctor_report(pipeline, conn, str(env.db_path), dataset_roots=roots)
    assert "instance identity" in healthy
    assert "PHYSICAL DRIFT" not in healthy

    conn.execute(f"alter table silver.{CERTS_TABLE} drop column rfc822_name")
    drifted = dlt_state.doctor_report(pipeline, conn, str(env.db_path), dataset_roots=roots)
    conn.close()
    assert "PHYSICAL DRIFT" in drifted
    assert "rfc822_name" in drifted
