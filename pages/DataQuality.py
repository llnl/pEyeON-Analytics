"""Data quality and load history: coverage, errors, drift, and batch timeline."""

import altair as alt
import streamlit as st

from utils.queries import Query
from utils.st_widgets import metric_row, page_link

q = Query()


def _render_kpis() -> None:
    coverage = q.try_df(
        """
        select
          (select count(*) from silver.raw_obs) as observations,
          (select count(distinct uuid) from gold.all_metadata) as with_metadata,
          (select count(*) from silver.metadata_error) as metadata_errors
        """,
        missing_msg="Silver/gold tables are not available yet — load data first.",
    )
    if coverage.empty:
        st.stop()
    r = coverage.iloc[0]
    observations = int(r["observations"] or 0)
    with_md = int(r["with_metadata"] or 0)
    pct = f"{100.0 * with_md / observations:.1f}%" if observations else "n/a"

    multi = q.try_df("select count(*) as n from gold.obs_with_multiple_uuid")
    multi_n = int(multi.iloc[0]["n"] or 0) if not multi.empty else 0

    drift = q.try_df(
        "select count(*) as n from gold.metadata_type_drift where status = 'unmodeled'"
    )
    drift_n = int(drift.iloc[0]["n"] or 0) if not drift.empty else 0

    metric_row(
        {
            "Observations": f"{observations:,}",
            "Metadata Coverage": pct,
            "Metadata Errors": int(r["metadata_errors"] or 0),
            "Multi-Type Files": multi_n,
            "Unmodeled Types": drift_n,
        }
    )
    page_link("pages/Schema_Blame.py", "Schema Inspector →")


def _render_load_timeline() -> None:
    st.subheader("Load Timeline")
    st.caption("Observations per loaded batch over time, colored by utility.")
    timeline = q.try_df(
        """
        select
          b.run_ts,
          b.utility_id,
          count(o.uuid) as observations
        from silver.batch_info b
        left join silver.raw_obs o on o._dlt_load_id = b._dlt_load_id
        group by b.run_ts, b.utility_id
        order by b.run_ts
        """
    )
    if timeline.empty:
        st.info("No batches loaded yet.")
        return
    chart = (
        alt.Chart(timeline)
        .mark_circle(size=90)
        .encode(
            x=alt.X("run_ts:T", title="Batch run time"),
            y=alt.Y("observations:Q", title="Observations"),
            color=alt.Color("utility_id:N", title="Utility"),
            tooltip=["run_ts:T", "utility_id", "observations"],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_errors() -> None:
    st.subheader("Metadata Errors")
    errors = q.try_df(
        """
        select e.error_type, e.message, r.filename, r.source_path, e.uuid
        from silver.metadata_error e
        left join silver.raw_obs r on r.uuid = e.uuid
        order by e.error_type, r.filename
        limit 200
        """
    )
    if errors.empty:
        st.success("No metadata extraction errors recorded.")
        return
    summary = errors.groupby("error_type", dropna=False).size().reset_index(name="count")
    st.dataframe(summary, width="stretch", hide_index=True)
    with st.expander("Error details", expanded=False):
        st.dataframe(errors, width="stretch", hide_index=True)


def _render_multi_type() -> None:
    st.subheader("Files Claimed by Multiple Metadata Types")
    st.caption(
        "One file, several metadata extractors — the open `filetype_multi` design tension, live."
    )
    multi = q.try_df(
        """
        select
          any_value(f.filename) as filename,
          m.metadata_tables,
          any_value(f.magic) as magic,
          m.uuid
        from gold.obs_with_multiple_uuid m
        left join gold.gold_files f on f.uuid = m.uuid
        group by m.uuid, m.metadata_tables
        order by filename
        limit 200
        """,
        missing_msg="`gold.obs_with_multiple_uuid` is not available yet. Run dbt to materialize it.",
    )
    if multi.empty:
        st.success("No files are claimed by multiple metadata types.")
    else:
        st.dataframe(multi, width="stretch", hide_index=True)


def _render_drift() -> None:
    st.subheader("Metadata Type Drift")
    drift = q.try_df(
        """
        select metadata_table_name, status, is_modeled
        from gold.metadata_type_drift
        order by case status when 'unmodeled' then 0 else 1 end, metadata_table_name
        """,
        missing_msg="`gold.metadata_type_drift` is not available yet. Run dbt to materialize it.",
    )
    if not drift.empty:
        st.dataframe(drift, width="stretch", hide_index=True)


def main():
    st.header("Data Quality & Load History")
    _render_kpis()
    st.divider()
    _render_load_timeline()
    st.divider()
    _render_errors()
    st.divider()
    left, right = st.columns([1, 1])
    with left:
        _render_multi_type()
    with right:
        _render_drift()


if __name__ in ("__main__", "__page__"):
    main()
