from utils.metadata_catalog import MetadataCatalog
from utils.queries import Query
from utils.st_widgets import metric_row, select_rows
from utils.utils import list_all_batches, load_me_some_data
from utils.config import settings
import streamlit as st

catalog = MetadataCatalog()
q = Query()



def main():
    st.header("EyeOn Summary")

    with st.expander("Loaded Data", expanded=True):
        # Hosts, labels, etc over time. Produces a constant vertical size, so its a good default for any size data set
        batches = q.try_df(
            "from gold.batch_summary order by utility_id",
            missing_msg="Batch summary is not available yet.",
        )

        if batches.empty:
            st.info("No batch summary rows found in `gold.batch_summary`.")
        else:
            tabs = st.tabs(["Dashboard", "Table"])

            total_utilities = int(batches["utility_id"].nunique())
            total_batches = int(batches["num_batches"].fillna(0).sum())
            total_obs = int(batches["num_rows"].fillna(0).sum())

            with tabs[0]:
                metric_row(
                    {
                        "Utilities": f"{total_utilities}",
                        "Batches": f"{total_batches}",
                        "Observations": f"{total_obs}",
                        "Metadata Types": ", ".join(catalog.loaded_type_names()),
                    },
                    weights=[0.1, 0.1, 0.1, 0.7],
                )

                left, right = st.columns([2, 1])
                with left:
                    st.subheader("By Utility")
                    chart_tabs = st.tabs(
                        ["Observations", "Batches", "Metadata Count"]
                    )

                    with chart_tabs[0]:
                        obs_df = batches[["utility_id", "num_rows"]].copy()
                        obs_df["num_rows"] = obs_df["num_rows"].fillna(0)
                        obs_df = obs_df.set_index("utility_id")
                        st.bar_chart(obs_df, height=260)

                    with chart_tabs[1]:
                        b_df = batches[["utility_id", "num_batches"]].copy()
                        b_df["num_batches"] = b_df["num_batches"].fillna(0)
                        b_df = b_df.set_index("utility_id")
                        st.bar_chart(b_df, height=260)

                    with chart_tabs[2]:
                        md_df = batches[["utility_id", "num_md_types"]].copy()
                        md_df["num_md_types"] = md_df["num_md_types"].fillna(0)
                        md_df = md_df.set_index("utility_id")
                        st.bar_chart(md_df, height=260)

                with right:
                    st.subheader("Utility Details")
                    utilities = batches["utility_id"].astype(str).tolist()
                    selected_utility = st.selectbox(
                        "Utility",
                        utilities,
                        index=0,
                        key="summary_selected_utility",
                    )
                    row = batches.loc[
                        batches["utility_id"].astype(str) == selected_utility
                    ]
                    if not row.empty:
                        r0 = row.iloc[0]
                        metric_row(
                            {
                                "Batches": f"{int(r0.get('num_batches', 0) or 0)}",
                                "Obs": f"{int(r0.get('num_rows', 0) or 0)}",
                                "MD Types": f"{int(r0.get('num_md_types', 0) or 0)}",
                            }
                        )

            with tabs[1]:
                st.dataframe(batches, width="stretch", hide_index=True)

    with st.expander("All Batches", expanded=True):
        batch_dirs = list_all_batches(settings.datasets.dataset_path)

        selected_rows = select_rows(batch_dirs, key="all_batches_df")

        if st.button(
            "Load Selected",
            disabled=len(selected_rows) == 0,
            help="Select one or more rows above to enable.",
        ):
            load_me_some_data(selected_rows)


if __name__ in ("__main__", "__page__"):
    main()
