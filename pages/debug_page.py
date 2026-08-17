import streamlit as st
import utils.db as db



def main():
    st.header("Streamlit Debugging Tools")

    with st.expander("Session State"):
        st.json(st.session_state)

    # Widgets update session_state using the "key" attribute. Awesome.
    # But, in multipage apps, they also clear out session_state when leaving the page!
    # Suggested hack is to persist the value in a shadow variable.
    # Ref: https://docs.streamlit.io/develop/concepts/multipage-apps/widgets

    # Initialize shadow for first execution
    if "_debug_sql" not in st.session_state:
        st.session_state._debug_sql = "summarize silver.raw_obs"

    def change_sql():
        st.session_state._debug_sql = st.session_state.debug_sql

    st.text_input(
        "SQL",
        value=st.session_state._debug_sql,
        key="debug_sql",
        on_change=change_sql,
    )
    try:
        st.dataframe(
            db.get_conn().sql(st.session_state.debug_sql).df(), height="stretch"
        )
    except Exception as e:
        st.error(e)

    # Shadow state, the app’s source of truth across pages
    if "_duckdb_ui" not in st.session_state:
        st.session_state["_duckdb_ui"] = False

    # Widget state, used only for the control
    if "duckdb_ui" not in st.session_state:
        st.session_state["duckdb_ui"] = st.session_state["_duckdb_ui"]

    def change_duckdb_ui():
        desired = st.session_state["duckdb_ui"]
        try:
            if desired:
                db.get_conn().sql("call start_ui()")
            else:
                db.get_conn().sql("call stop_ui_server()")
            # Commit desired into shadow only if side effect succeeded
            st.session_state["_duckdb_ui"] = desired
        except Exception:
            # Revert the widget to the last known good shadow state
            st.session_state["duckdb_ui"] = st.session_state["_duckdb_ui"]
            raise  # or st.error(...); returning silently can hide issues

    st.toggle("DuckDB UI", key="duckdb_ui", on_change=change_duckdb_ui)


if __name__ in ("__main__", "__page__"):
    main()
