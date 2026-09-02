"""Variant clusters: same tool, different builds, via imphash/telfhash grouping."""

import streamlit as st

from utils.queries import Query
from utils.st_widgets import metric_row

q = Query()

GOLD_FILES_MSG = (
    "`gold.gold_files` is not available yet. Run the dbt project to materialize it."
)

# Scanner placeholder values that are not real hashes; grouping on them
# produces one giant meaningless cluster.
PLACEHOLDERS = {
    "imphash": ["", "N/A"],
    "telfhash": ["", "-", "tnull"],
}


def _clusters(hash_col: str):
    placeholders = ", ".join(f"'{p}'" for p in PLACEHOLDERS.get(hash_col, [""]))
    return q.try_df(
        f"""
        select
          {hash_col} as cluster_hash,
          count(distinct sha256) as variants,
          count(*) as files,
          list_sort(list(distinct utility_id)) as utilities,
          any_value(filename) as example
        from gold.gold_files
        where {hash_col} is not null and {hash_col} not in ({placeholders})
        group by {hash_col}
        having count(distinct sha256) > 1
        order by variants desc, files desc
        """,
        missing_msg=GOLD_FILES_MSG,
    )


def _render_cluster_section(title: str, hash_col: str, caption: str) -> None:
    st.subheader(title)
    st.caption(caption)
    clusters = _clusters(hash_col)
    if clusters.empty:
        st.info(f"No {hash_col} clusters with more than one distinct binary.")
        return

    metric_row(
        {
            "Clusters": len(clusters),
            "Largest Cluster (variants)": int(clusters["variants"].max()),
            "Files Involved": int(clusters["files"].sum()),
        }
    )
    st.dataframe(clusters, width="stretch", hide_index=True)

    options = clusters["cluster_hash"].tolist()
    selected = st.selectbox(
        f"Inspect a {hash_col} cluster",
        options,
        format_func=lambda h: f"{h[:24]}…" if len(str(h)) > 24 else str(h),
        key=f"cluster_select_{hash_col}",
    )
    members = q.try_df(
        f"""
        select filename, utility_id, bytecount_human as size, observation_ts,
               sha256, ssdeep
        from gold.gold_files
        where {hash_col} = ?
        order by utility_id, filename
        """,
        [selected],
    )
    st.caption(
        "Members share the same import/symbol profile but differ in content hash — "
        "compare ssdeep strings to judge how close the builds are (pairwise ssdeep "
        "scoring is a candidate future enhancement)."
    )
    st.dataframe(members, width="stretch", hide_index=True)


def main():
    st.header("Variant Clusters")
    st.caption(
        "Groups of distinct binaries (different sha256) that share a fuzzy/import "
        "hash — typically the same component at different versions or builds."
    )
    _render_cluster_section(
        "PE Variants (imphash)",
        "imphash",
        "PE files grouped by import-table hash.",
    )
    st.divider()
    _render_cluster_section(
        "ELF Variants (telfhash)",
        "telfhash",
        "ELF files grouped by telfhash (symbol-based fuzzy hash).",
    )


if __name__ in ("__main__", "__page__"):
    main()
