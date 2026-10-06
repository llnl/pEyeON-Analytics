import common.dqautil as du
import streamlit as st
from utils.config import settings


def auth_filter():
    if not st.user.is_logged_in and not settings.env.mode == "dev":
        st.markdown("# Welcome to EyeOn!")
        st.button("Log in with OneID", on_click=st.login)
        st.stop()


def summary():
    # Main page columns
    col1, col2, col3, col4 = st.columns([0.2, 0.2, 0.2, 0.4])

    if du.getcon() is not None:
        # High level stats: hosts, data range
        with col1:
            st.metric(
                "Observations",
                "{:,}".format(du.get(du.getcon(), "observation_count")[0]),
            )

        with col2:
            st.metric(
                "Locations", "{:,}".format(du.get(du.getcon(), "location_count")[0])
            )

        with col3:
            st.metric("Signed", "{:,}".format(du.get(du.getcon(), "signed_count")[0]))

        with col4:
            (min, max) = du.get(du.getcon(), "data_range")
            st.metric("Data Range", f"{min:%m-%d-%Y} - {max:%m-%d-%Y}")
        st.markdown(f"_Current db: {st.session_state.filename}_")


def sig_features(con):
    st.header("Signature Feature Summary")
    features_df = du.getdatafor(du.getcon(), "sig_feature_summary", "cert_queries")
    st.dataframe(features_df, hide_index=True)


def md_features(con):
    st.header("Metadata Feature Summary")
    features_df = du.getdatafor(du.getcon(), "md_feature_summary", "metadata_queries")
    st.dataframe(features_df, hide_index=True)


def new_batches():
    st.header("New Data Batches")
    try:
        batches_df = du.getdatafor(du.getcon(), "new_batches")
        st.scatter_chart(batches_df, x="batch_id", y="num_rows")
        st.dataframe(batches_df, hide_index=True)
    except BaseException as e:
        st.error("Oops! No db?")
        st.exception(e)


def debug_info():
    with st.expander("Debug Info", expanded=False, icon=":material/frame_inspect:"):
        st.write("Session State:")
        st.json(st.session_state)
