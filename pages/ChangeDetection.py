"""Change detection between consecutive batches: what appeared, what vanished."""

import altair as alt
import streamlit as st

from utils.queries import Query
from utils.st_widgets import metric_row

q = Query()

MART_MSG = (
    "`gold.mart_batch_changes` is not available yet. Run the dbt project to "
    "materialize it (it ships with this page)."
)


def main():
    st.header("Change Detection")
    st.caption(
        "Differences between consecutive batches of the same utility, by content hash. "
        "A utility's first batch is the baseline: everything in it counts as neither "
        "new nor disappeared."
    )

    changes = q.try_df(
        """
        select utility_id, _dlt_load_id, run_ts, batch_seq, sha256, change_type
        from gold.mart_batch_changes
        """,
        missing_msg=MART_MSG,
    )
    if changes.empty:
        st.info(
            "No batch-over-batch changes recorded. Load at least two batches for "
            "the same utility to see change detection."
        )
        return

    utilities = sorted(changes["utility_id"].unique().tolist())
    selected = st.selectbox("Utility", utilities)
    df = changes[changes["utility_id"] == selected]

    new_total = int((df["change_type"] == "new").sum())
    gone_total = int((df["change_type"] == "disappeared").sum())
    metric_row(
        {
            "Batches Compared": int(df["batch_seq"].nunique()),
            "New Files (all batches)": new_total,
            "Disappeared Files (all batches)": gone_total,
        }
    )

    per_batch = (
        df.groupby(["batch_seq", "run_ts", "change_type"])
        .size()
        .reset_index(name="files")
    )
    per_batch["signed_files"] = per_batch.apply(
        lambda r: r["files"] if r["change_type"] == "new" else -r["files"], axis=1
    )
    chart = (
        alt.Chart(per_batch)
        .mark_bar()
        .encode(
            x=alt.X("batch_seq:O", title="Batch #"),
            y=alt.Y("signed_files:Q", title="Files (new ↑ / disappeared ↓)"),
            color=alt.Color(
                "change_type:N",
                scale=alt.Scale(
                    domain=["new", "disappeared"], range=["#63C987", "#E8694C"]
                ),
                legend=alt.Legend(title="Change"),
            ),
            tooltip=["batch_seq", "run_ts:T", "change_type", "files"],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Change Details")
    batch_options = sorted(df["batch_seq"].unique().tolist())
    selected_batch = st.selectbox(
        "Batch #", batch_options, index=len(batch_options) - 1
    )

    detail = q.try_df(
        """
        select
          m.change_type,
          any_value(f.filename) as filename,
          any_value(f.bytecount_human) as size,
          any_value(f.magic) as magic,
          m.sha256
        from gold.mart_batch_changes m
        left join gold.gold_files f on f.sha256 = m.sha256
        where m.utility_id = ? and m.batch_seq = ?
        group by m.change_type, m.sha256
        order by m.change_type, filename
        limit 500
        """,
        [selected, int(selected_batch)],
    )
    if detail.empty:
        st.info("No changes in the selected batch.")
    else:
        st.caption(
            "Filenames come from the latest observation of each hash; a disappeared "
            "hash keeps the name it had when last seen."
        )
        st.dataframe(detail, width="stretch", hide_index=True)


if __name__ in ("__main__", "__page__"):
    main()
