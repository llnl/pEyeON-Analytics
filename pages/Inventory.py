"""Software inventory views over gold.gold_files: what is deployed where."""

import altair as alt
import streamlit as st

from utils.queries import Query
from utils.st_widgets import metric_row

q = Query()

GOLD_FILES_MSG = (
    "`gold.gold_files` is not available yet. Run the dbt project to materialize it."
)


def _render_kpis() -> None:
    kpis = q.try_df(
        """
        select
          (select count(distinct sha256) from gold.gold_files where sha256 is not null)
            as unique_content,
          (select count(*) from gold.gold_files) as observed_files,
          (select count(*) from silver.raw_obs) as observations,
          (select count(distinct utility_id) from gold.gold_files) as utilities
        """,
        missing_msg=GOLD_FILES_MSG,
    )
    if kpis.empty:
        st.stop()
    r = kpis.iloc[0]
    unique_content = int(r["unique_content"] or 0)
    observed_files = int(r["observed_files"] or 0)
    dedup = f"{observed_files / unique_content:.2f}x" if unique_content else "n/a"
    metric_row(
        {
            "Unique Content (sha256)": f"{unique_content:,}",
            "Observed Files": f"{observed_files:,}",
            "Observations": f"{int(r['observations'] or 0):,}",
            "Dedup Factor": dedup,
            "Utilities": int(r["utilities"] or 0),
        }
    )
    st.caption(
        "Unique content counts distinct hashes; observed files count distinct "
        "file instances (uuids). The dedup factor is instances over content — "
        "how much of the inventory is copies."
    )


def _render_cross_utility_overlap() -> None:
    st.subheader("Cross-Utility Overlap")
    st.caption(
        "Binaries observed at more than one utility — shared components mean shared exposure."
    )
    shared = q.try_df(
        """
        select
          count(*) filter (utilities > 1) as shared_binaries,
          max(utilities) as max_utilities
        from (
          select sha256, count(distinct utility_id) as utilities
          from gold.gold_files
          where sha256 is not null
          group by sha256
        )
        """,
        missing_msg=GOLD_FILES_MSG,
    )
    if shared.empty:
        return
    r = shared.iloc[0]
    metric_row(
        {
            "Shared Binaries (2+ utilities)": int(r["shared_binaries"] or 0),
            "Widest Spread": int(r["max_utilities"] or 0),
        },
        weights=[0.3, 0.7],
    )

    detail = q.try_df(
        """
        select
          any_value(filename) as filename,
          count(distinct utility_id) as utilities,
          list_sort(list(distinct utility_id)) as seen_at,
          count(*) as copies,
          any_value(bytecount_human) as size,
          sha256
        from gold.gold_files
        where sha256 is not null
        group by sha256
        having count(distinct utility_id) > 1
        order by utilities desc, copies desc, filename
        limit 200
        """
    )
    if detail.empty:
        st.info("No binaries are currently shared across utilities.")
    else:
        st.dataframe(detail, width="stretch", hide_index=True)


def _render_filetype_composition() -> None:
    st.subheader("Filetype Composition")
    counts = q.try_df(
        "select filetype, file_count, distinct_files from gold.gold_filetype_counts",
        missing_msg="`gold.gold_filetype_counts` is not available yet. Run dbt to materialize it.",
    )
    if counts.empty:
        return
    counts["filetype"] = counts["filetype"].fillna("(untyped)")
    chart = (
        alt.Chart(counts)
        .mark_bar()
        .encode(
            x=alt.X("file_count:Q", title="Files"),
            y=alt.Y("filetype:N", sort="-x", title="Filetype"),
            tooltip=["filetype", "file_count", "distinct_files"],
        )
        .properties(height=max(200, len(counts) * 22))
    )
    st.altair_chart(chart, use_container_width=True)


def _render_top_artifacts() -> None:
    left, right = st.columns(2)
    with left:
        st.subheader("Largest Artifacts")
        largest = q.try_df(
            """
            select filename, bytecount_human as size, bytecount, utility_id, magic, sha256
            from gold.gold_files
            order by bytecount desc nulls last
            limit 20
            """
        )
        st.dataframe(
            largest.drop(columns=["bytecount"], errors="ignore"),
            width="stretch",
            hide_index=True,
        )
    with right:
        st.subheader("Most Duplicated Content")
        st.caption("Identical sha256 observed at multiple paths or utilities.")
        duplicated = q.try_df(
            """
            select
              any_value(filename) as filename,
              count(*) as copies,
              count(distinct utility_id) as utilities,
              any_value(bytecount_human) as size,
              sha256
            from gold.gold_files
            where sha256 is not null
            group by sha256
            having count(*) > 1
            order by copies desc, filename
            limit 20
            """
        )
        if duplicated.empty:
            st.info("No duplicated content found.")
        else:
            st.dataframe(duplicated, width="stretch", hide_index=True)


def _render_os_arch_matrix() -> None:
    st.subheader("OS / Architecture Matrix")
    st.caption("Executable inventory by format family, OS, and architecture.")
    matrix = q.try_df(
        """
        select 'PE' as family, coalesce(os, '(unknown)') as os,
               coalesce(pe_machine, '(unknown)') as arch, count(*) as files
        from silver.metadata_pe_file
        group by all
        union all
        select 'ELF', coalesce(os, '(unknown)'),
               coalesce(elf_human_arch, '(unknown)'), count(*)
        from silver.metadata_elf_file
        group by all
        order by files desc
        """,
        missing_msg="PE/ELF metadata tables are not available yet — load data first.",
    )
    if matrix.empty:
        st.info("No PE or ELF metadata loaded yet.")
        return
    chart = (
        alt.Chart(matrix)
        .mark_rect()
        .encode(
            x=alt.X("arch:N", title="Architecture"),
            y=alt.Y("family:N", title="Family"),
            color=alt.Color("files:Q", scale=alt.Scale(scheme="blues"), title="Files"),
            tooltip=["family", "os", "arch", "files"],
        )
        .properties(height=120)
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(matrix, width="stretch", hide_index=True)


def _render_containers_and_firmware() -> None:
    st.subheader("Containers & Firmware")
    counts = q.try_df(
        """
        select
          (select count(*) from silver.metadata_container_file) as containers,
          (select count(*) from silver.metadata_binwalk_file) as binwalk_scans,
          (select count(*) from silver.metadata_uimage_file) as uimage_images
        """,
        missing_msg="Container/firmware metadata tables are not available yet — load data first.",
    )
    children = q.try_df(
        """
        select
          count(*) as parents_with_children,
          sum(cnt) as extracted_children,
          max(cnt) as max_children,
          round(avg(cnt), 1) as avg_children
        from (
          select parent, count(*) as cnt
          from silver.raw_obs
          where parent is not null
          group by parent
        )
        """
    )
    depth = q.try_df(
        """
        with recursive d as (
          select uuid, 0 as depth from silver.raw_obs where parent is null
          union all
          select c.uuid, d.depth + 1
          from silver.raw_obs c
          join d on c.parent = d.uuid
        )
        select max(depth) as max_depth, round(avg(depth), 2) as avg_depth from d
        """
    )
    if not counts.empty and not children.empty:
        c, ch = counts.iloc[0], children.iloc[0]
        metrics = {
            "Containers": int(c["containers"] or 0),
            "Binwalk Scans": int(c["binwalk_scans"] or 0),
            "U-Boot Images": int(c["uimage_images"] or 0),
            "Extracted Children": int(ch["extracted_children"] or 0),
            "Max Children": int(ch["max_children"] or 0),
        }
        if not depth.empty:
            metrics["Max Depth"] = int(depth.iloc[0]["max_depth"] or 0)
        metric_row(metrics)

    top_parents = q.try_df(
        """
        select
          any_value(p.filename) as container,
          c.parent as parent_uuid,
          count(*) as children,
          any_value(p.magic) as magic
        from silver.raw_obs c
        join silver.raw_obs p on p.uuid = c.parent
        group by c.parent
        order by children desc
        limit 15
        """
    )
    if not top_parents.empty:
        st.markdown("**Top containers by extracted children** — drill into them on the Observation Hierarchy page.")
        st.dataframe(top_parents, width="stretch", hide_index=True)


def main():
    st.header("Software Inventory")
    _render_kpis()
    st.divider()
    _render_cross_utility_overlap()
    st.divider()
    left, right = st.columns([1, 1])
    with left:
        _render_filetype_composition()
    with right:
        _render_os_arch_matrix()
    st.divider()
    _render_top_artifacts()
    st.divider()
    _render_containers_and_firmware()


if __name__ in ("__main__", "__page__"):
    main()
