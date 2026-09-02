import common.dqautil as du
import re
import streamlit as st
from utils.config import settings

# import pages.pages as pages


def get_allds():
    if "allds" not in st.session_state:
        st.session_state.allds = du.finddatasets(settings.datasets.dataset_path)
    return st.session_state.allds


def app_base_config():
    st.set_page_config(
        # Page_title actually sets the tab name
        page_title=settings.app.page_title,
        initial_sidebar_state="expanded",
    )
    # This content is generated as the "virtual welcome page" when a user first connects. There doesn't appear
    #  to be any way to get back to it once you navigate to another page.
    st.markdown("Welcome to EyeOn!")
    st.markdown("Select a page from the sidebar!")
    con, msgs = du.opendb()
    with st.container(height=160):
        st.write(msgs)
    # Attempt to switch pages, doesn't always work.
    st.switch_page("pages/EyeOnSummary.py")


def sidebar_config(pages, logo=settings.app.logo, logo_width=120):
    with st.sidebar:
        st.image(logo, width=logo_width)
        st.title(settings.app.page_title)
        st.header("Menu")
        # Add all pages to the sidebar. They'll be listed in the order added.
        #        st.page_link("main.py", label="Home")
        for page in pages:
            st.page_link(page.filename, label=page.label)
        # sidebar_db_chooser()
        st.divider()
        if settings.env.mode == "dev":
            st.markdown("**DEV MODE - No authentication**")
        else:
            # The spec REQUIREs sub, but it isn't usually as human friendly as NAME.
            try:
                st.markdown(f"_{st.user.name}_")
            except Exception:
                st.markdown(f"_{st.user.sub}_")
            st.button("Log out", on_click=st.logout)


def change_cur_ds():
    # Streamlit scopes widget bound keys to a page
    # we want the state maintained across the session, so sync here.
    st.session_state.curds = st.session_state._curds
    st.session_state._curdb = None
    st.session_state.curdb = None


def change_cur_db():
    # Streamlit scopes widget bound keys to a page
    # we want the state maintained across the session, so sync here.
    st.session_state.curdb = st.session_state._curdb
    if st.session_state.curdb is not None:
        print(f"on_change: opening db: {st.session_state.curdb}")
        du.opendb(get_allds()[st.session_state.curds].getdb(st.session_state.curdb))


def sidebar_db_chooser():
    with st.sidebar:
        # Index into options list. Default to None forces user to select one.
        curds_idx = 0
        if "curds" in st.session_state:
            # Get index to set as default selections
            curds_idx = list(get_allds()).index(st.session_state.curds)

        st.selectbox(
            "Dataset: ",
            get_allds().keys(),
            index=curds_idx,
            key="_curds",
            on_change=change_cur_ds,
        )

        # ensures the default selection populates correctly the first time
        if "curds" not in st.session_state and curds_idx is not None:
            change_cur_ds()
            change_cur_db()

        if "curds" in st.session_state:
            # With only 1 database don't bother with the selectbox, just use it and display as text
            if len(get_allds()[st.session_state.curds].databases) == 1:
                st.session_state._curdb = get_allds()[st.session_state.curds].databases[
                    0
                ]
                change_cur_db()
                st.markdown(f"*{st.session_state.curdb}*")
            else:
                curdb_idx = None
                if "curdb" in st.session_state and st.session_state.curdb is not None:
                    curdb_idx = list(
                        get_allds()[st.session_state.curds].databases
                    ).index(st.session_state.curdb)
                st.selectbox(
                    "Database: ",
                    get_allds()[st.session_state.curds].databases,
                    index=curdb_idx,
                    key="_curdb",
                    on_change=change_cur_db,
                )


# Widget state utilities
def store_keys(keys: list):
    for key in keys:
        st.session_state[key] = st.session_state["_" + key]


def load_keys(keys: list):
    for key in keys:
        if key in st.session_state:
            st.session_state["_" + key] = st.session_state[key]


def valid_uuid(uuid):
    # From: https://stackoverflow.com/questions/11384589/what-is-the-correct-regex-for-matching-values-generated-by-uuid-uuid4-hex
    regex = re.compile(
        "^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z",
        re.I,
    )
    match = regex.match(uuid)
    return bool(match)
