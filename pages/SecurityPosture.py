"""Security posture: code-signing status and certificate hygiene at a glance."""

import altair as alt
import streamlit as st

from utils.queries import Query
from utils.st_widgets import metric_row, page_link

q = Query()

CERTS_MSG = (
    "Certificate dbt models are not available yet. Run the dbt project so the "
    "certificate marts are materialized in the `gold` schema."
)


def _render_signing_posture() -> None:
    st.subheader("Code-Signing Posture (PE)")
    st.caption(
        "Authenticode verification per utility for PE executables. "
        "`OK` verified; problems are failed digests/signatures; the rest are unsigned or unchecked."
    )
    posture = q.try_df(
        """
        select
          utility_id,
          count(*) as pe_files,
          count(*) filter (authenticode_integrity = 'OK') as signed_ok,
          count(*) filter (
            authenticode_integrity is not null and authenticode_integrity != 'OK'
          ) as signature_problems,
          count(*) filter (authenticode_integrity is null) as unsigned_or_unchecked
        from gold.gold_files
        where list_contains(filetypes, 'PE')
        group by utility_id
        order by utility_id
        """,
        missing_msg="`gold.gold_files` is not available yet. Run dbt to materialize it.",
    )
    if posture.empty:
        st.info("No PE files in the current inventory.")
        return

    totals = posture[["pe_files", "signed_ok", "signature_problems", "unsigned_or_unchecked"]].sum()
    metric_row(
        {
            "PE Files": int(totals["pe_files"]),
            "Signed (OK)": int(totals["signed_ok"]),
            "Signature Problems": int(totals["signature_problems"]),
            "Unsigned / Unchecked": int(totals["unsigned_or_unchecked"]),
        }
    )

    chart_df = posture.melt(
        id_vars=["utility_id"],
        value_vars=["signed_ok", "signature_problems", "unsigned_or_unchecked"],
        var_name="status",
        value_name="files",
    )
    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X("utility_id:N", title="Utility"),
            y=alt.Y("files:Q", title="PE files"),
            color=alt.Color(
                "status:N",
                scale=alt.Scale(
                    domain=["signed_ok", "signature_problems", "unsigned_or_unchecked"],
                    range=["#63C987", "#E8694C", "#B0B7C3"],
                ),
                legend=alt.Legend(title="Signing status"),
            ),
            tooltip=["utility_id", "status", "files"],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)

    problems = q.try_df(
        """
        select filename, authenticode_integrity, utility_id, bytecount_human as size, sha256
        from gold.gold_files
        where authenticode_integrity is not null and authenticode_integrity != 'OK'
        order by utility_id, filename
        limit 100
        """
    )
    if not problems.empty:
        st.markdown("**Files with signature problems**")
        st.dataframe(problems, width="stretch", hide_index=True)


def _render_cert_hygiene() -> None:
    st.subheader("Certificate Hygiene")
    hygiene = q.try_df(
        """
        select
          count(*) as certs,
          count(*) filter (expires_on < current_timestamp) as expired,
          count(*) filter (
            expires_on >= current_timestamp
            and expires_on < current_timestamp + interval 12 month
          ) as expiring_12mo,
          count(*) filter (
            try_cast(regexp_extract(rsa_key_size, '[0-9]+') as int) < 2048
          ) as weak_rsa
        from gold.dim_certificates
        """,
        missing_msg=CERTS_MSG,
    )
    if hygiene.empty:
        return
    r = hygiene.iloc[0]

    in_use = q.try_df(
        """
        select count(distinct f.observation_uuid) as obs_with_expired
        from gold.fct_observation_certificates f
        join gold.dim_certificates d on d.cert_sha256 = f.cert_sha256
        where d.expires_on < current_timestamp
        """
    )
    obs_with_expired = int(in_use.iloc[0]["obs_with_expired"] or 0) if not in_use.empty else 0

    metric_row(
        {
            "Certificates": int(r["certs"] or 0),
            "Expired": int(r["expired"] or 0),
            "Expiring ≤ 12 mo": int(r["expiring_12mo"] or 0),
            "Weak RSA (<2048)": int(r["weak_rsa"] or 0),
            "Files w/ Expired Cert": obs_with_expired,
        }
    )
    page_link("pages/certs.py", "Full certificate analysis →")

    detail = q.try_df(
        """
        select
          case
            when expires_on < current_timestamp then 'expired'
            when expires_on < current_timestamp + interval 12 month then 'expiring ≤ 12 mo'
            else 'weak key'
          end as issue,
          subject_common_name,
          issuer_common_name,
          expires_on,
          rsa_key_size,
          is_ca,
          cert_sha256
        from gold.dim_certificates
        where expires_on < current_timestamp + interval 12 month
           or try_cast(regexp_extract(rsa_key_size, '[0-9]+') as int) < 2048
        order by expires_on nulls last
        limit 200
        """
    )
    if detail.empty:
        st.success("No expired, soon-expiring, or weak-key certificates found.")
    else:
        st.markdown("**Certificates needing attention**")
        st.dataframe(detail, width="stretch", hide_index=True)


def main():
    st.header("Security Posture")
    _render_signing_posture()
    st.divider()
    _render_cert_hygiene()


if __name__ in ("__main__", "__page__"):
    main()
