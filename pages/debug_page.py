import streamlit as st
import utils.db as db
from utils.st_widgets import shadow_init, shadow_sync



def main():
    st.header("Streamlit Debugging Tools")

    with st.expander("Session State"):
        st.json(st.session_state)

    # Widgets update session_state using the "key" attribute, but multipage
    # apps clear widget keys when leaving the page — persist via shadow
    # helpers (see utils.st_widgets).
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

    # Shadow state is the app's source of truth across pages; the widget key
    # is re-seeded from it on each page entry. on_change stays custom here
    # because toggling has a side effect that must succeed before committing.
    st.session_state.setdefault("duckdb_ui", shadow_init("duckdb_ui", False))

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
