"""Query façade over the shared DuckDB connection for page code.

Standardizes the query→DataFrame patterns that every page previously
hand-rolled, including the guarded "model not materialized yet" UX.
"""

import pandas as pd
import streamlit as st

import utils.db as db


class Query:
    """Thin façade over the shared connection (see utils.db.get_conn)."""

    def __init__(self, schema: str = "silver"):
        self._schema = schema

    def _conn(self):
        return db.get_conn(schema=self._schema)

    def df(self, sql: str, params=None) -> pd.DataFrame:
        """Run a query and return a DataFrame."""
        if params is None:
            return self._conn().sql(sql).df()
        return self._conn().execute(sql, params).df()

    def scalar(self, sql: str, params=None):
        """Run a query and return the first column of the first row."""
        if params is None:
            return self._conn().execute(sql).fetchone()[0]
        return self._conn().execute(sql, params).fetchone()[0]

    def try_df(self, sql: str, params=None, missing_msg: str | None = None) -> pd.DataFrame:
        """Run a query that may reference not-yet-materialized tables.

        On failure, renders the standard warning + exception caption (when
        missing_msg is given) and returns an empty DataFrame.
        """
        try:
            return self.df(sql, params)
        except Exception as e:
            if missing_msg:
                st.warning(missing_msg)
                st.caption(f"{type(e).__name__}: {e}")
            return pd.DataFrame()

    def existing_tables(self, schema: str) -> set[str]:
        """Table names present in an attached schema."""
        rows = (
            self._conn()
            .execute(
                """
                select table_name
                from information_schema.tables
                where table_schema = ?
                """,
                [schema],
            )
            .fetchall()
        )
        return {row[0] for row in rows}

    def missing_tables(self, schema: str, required: list[str]) -> list[str]:
        """Which of the required tables/models are not materialized yet."""
        existing = self.existing_tables(schema)
        return [t for t in required if t not in existing]
