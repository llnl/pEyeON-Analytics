"""Reusable Streamlit widget/fragment helpers shared across pages."""

import pandas as pd
import streamlit as st


def select_rows(df: pd.DataFrame, key: str, multi: bool = True) -> list[dict]:
    """Render a selectable dataframe and return the selected rows as dicts.

    Consolidates the event.selection -> iloc -> NA-scrub -> records dance
    previously duplicated by callers.
    """
    event = st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row" if multi else "single-row",
        key=key,
    )
    if not event.selection.rows:
        return []
    return (
        df.iloc[event.selection.rows]
        .copy()
        .replace({pd.NA: None})
        .to_dict(orient="records")
    )


def metric_row(metrics: dict, weights=None) -> None:
    """Render a row of st.metric values from a label -> value mapping."""
    cols = st.columns(weights or len(metrics))
    for col, (label, value) in zip(cols, metrics.items()):
        col.metric(label, value)


def shadow_init(key: str, default):
    """Cross-page widget persistence, step 1: initialize the shadow value.

    Streamlit clears widget-bound session_state keys when leaving a page;
    the documented workaround is a shadow variable (here `_<key>`).
    Ref: https://docs.streamlit.io/develop/concepts/multipage-apps/widgets
    """
    shadow = f"_{key}"
    if shadow not in st.session_state:
        st.session_state[shadow] = default
    return st.session_state[shadow]


def page_link(page: str, label: str) -> None:
    """st.page_link that degrades to a caption when the target page is not
    registered — i.e. when a page script is run directly instead of through
    the EyeOnData.py navigation entrypoint."""
    try:
        st.page_link(page, label=label)
    except Exception:
        st.caption(label)


def shadow_sync(key: str):
    """Cross-page widget persistence, step 2: on_change callback that copies
    the widget's value into its shadow key."""

    def _sync():
        st.session_state[f"_{key}"] = st.session_state[key]

    return _sync
