import streamlit as st

import utils.db as db
from utils.app_init import init_app_form
from utils.config import settings
from utils.sidebar import sidebar_db_chooser


def init_page():
    """Shown as the only page until a database exists."""
    st.markdown("# Initialize Database")
    init_app_form()


def main():
    st.set_page_config(
        # Page_title actually sets the tab name
        page_title=settings.app.page_title,
        page_icon=settings.app.logo,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.sidebar.image(settings.app.logo, width=120)
    st.sidebar.title(settings.app.page_title)

    if db.exists():
        pg = st.navigation(
            [
                st.Page("pages/EyeOnSummary.py", title="EyeOn Summary", default=True),
                st.Page("pages/certs.py", title="X509 Certificates"),
                st.Page("pages/ObservationHierarchy.py", title="Observation Hierarchy"),
                st.Page("pages/BrowseDltData.py", title="Browse/Search Observations"),
                st.Page("pages/Schema_Blame.py", title="Schema Inspector"),
                st.Page("pages/debug_page.py", title="Debug Tools"),
            ]
        )
        # Render the schema/root-table controls on every page, as before.
        sidebar_db_chooser()
    else:
        pg = st.navigation([st.Page(init_page, title="Initialize Database")])

    pg.run()


if __name__ == "__main__":
    main()
