import streamlit as st
import load_eyeon
import utils.db as db
from utils.st_widgets import shadow_init, shadow_sync


def main():
    st.header("Streamlit Debugging Tools")

    with st.expander("Session State"):
        st.json(st.session_state)

    st.text_input(
        "SQL",
        value=shadow_init("debug_sql", "summarize silver.raw_obs"),
        key="debug_sql",
        on_change=shadow_sync("debug_sql"),
    )
    try:
        st.dataframe(
            db.get_conn().sql(st.session_state.debug_sql).df(), height="stretch"
        )
    except Exception as e:
        st.error(e)

    st.session_state.setdefault("duckdb_ui", shadow_init("duckdb_ui", False))

    def change_duckdb_ui():
        desired = st.session_state["duckdb_ui"]
        try:
            if desired:
                db.get_conn().sql("call start_ui()")
            else:
                db.get_conn().sql("call stop_ui_server()")
            st.session_state["_duckdb_ui"] = desired
        except Exception:
            st.session_state["duckdb_ui"] = st.session_state["_duckdb_ui"]
            raise

    st.toggle("DuckDB UI", key="duckdb_ui", on_change=change_duckdb_ui)

    with st.expander("DLT State Doctor"):
        st.caption(
            "Compares the three stores of DLT state: the local pipeline "
            "dir, the _dlt_* metadata tables, and the physical tables. "
            "Same report as `load_eyeon.py --doctor`."
        )
        if st.button("Run doctor report"):
            try:
                st.code(load_eyeon.doctor_text(db.get_conn()), language=None)
            except Exception as e:
                st.error(f"Doctor report unavailable: {e}")


if __name__ in ("__main__", "__page__"):
    main()
