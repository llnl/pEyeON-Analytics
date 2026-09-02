from utils.queries import Query
from utils.st_widgets import metric_row
import pandas as pd
import streamlit as st
import altair as alt

q = Query()

REQUIRED_MODELS = [
    "dim_certificates",
    "fct_observation_certificates",
    "mart_cert_feature_summary",
    "mart_cert_locations",
    "mart_cert_key_sizes",
    "mart_cert_issue_years",
    "mart_cert_expiration_years",
    "mart_cert_subject_states",
    "mart_cert_organizations",
]


def _summary_metrics() -> dict[str, object]:
    summary = q.df(
        """
        select
          count(*) as certificate_count,
          count(distinct issuer_sha256) as issuer_count,
          sum(case when is_ca then 1 else 0 end) as ca_count
        from gold.dim_certificates
        """
    ).iloc[0]

    obs = q.df(
        """
        select
          count(*) as certificate_observation_rows,
          count(distinct observation_uuid) as signed_observation_count,
          count(distinct utility_id) as utility_count,
          min(observation_ts) as min_observation_ts,
          max(observation_ts) as max_observation_ts
        from gold.fct_observation_certificates
        """
    ).iloc[0]

    return {
        "certificate_count": int(summary["certificate_count"] or 0),
        "issuer_count": int(summary["issuer_count"] or 0),
        "ca_count": int(summary["ca_count"] or 0),
        "certificate_observation_rows": int(obs["certificate_observation_rows"] or 0),
        "signed_observation_count": int(obs["signed_observation_count"] or 0),
        "utility_count": int(obs["utility_count"] or 0),
        "min_observation_ts": obs["min_observation_ts"],
        "max_observation_ts": obs["max_observation_ts"],
    }


def _render_summary() -> None:
    metrics = _summary_metrics()
    metric_row(
        {
            "Certificates": f"{metrics['certificate_count']:,}",
            "Issuers": f"{metrics['issuer_count']:,}",
            "CA Certs": f"{metrics['ca_count']:,}",
            "Signed Observations": f"{metrics['signed_observation_count']:,}",
            "Utilities": f"{metrics['utility_count']:,}",
        }
    )

    min_ts = metrics["min_observation_ts"]
    max_ts = metrics["max_observation_ts"]
    if pd.notna(min_ts) and pd.notna(max_ts):
        st.caption(f"Observation Range: {min_ts:%Y-%m-%d} to {max_ts:%Y-%m-%d}")


def _render_feature_summary() -> None:
    st.subheader("Signature Feature Summary")
    features_df = q.df(
        """
        select
          is_ca,
          key_usage,
          ext_key_usage,
          rows
        from gold.mart_cert_feature_summary
        order by rows desc, is_ca desc, key_usage, ext_key_usage
        """
    )
    st.dataframe(features_df, hide_index=True, width="stretch")


def _render_observations_by_utility() -> None:
    st.subheader("Observations Containing Certificates")
    locations_df = q.df(
        """
        select
          location as utility_id,
          num_rows
        from gold.mart_cert_locations
        order by num_rows desc, utility_id
        """
    )
    if locations_df.empty:
        st.info("No certificate observation rows found.")
        return

    st.bar_chart(locations_df, x="utility_id", y="num_rows")
    st.dataframe(locations_df, hide_index=True, width="stretch")


def _render_key_sizes() -> None:
    st.subheader("RSA Key Sizes")
    key_sizes_df = q.df(
        """
        select
          coalesce(rsa_key_size, 'Unknown') as rsa_key_size,
          num_keys
        from gold.mart_cert_key_sizes
        order by num_keys desc, rsa_key_size
        """
    )
    if key_sizes_df.empty:
        st.info("No RSA key size data found.")
        return

    chart = (
        alt.Chart(key_sizes_df)
        .mark_arc()
        .encode(
            theta=alt.Theta("num_keys:Q"),
            color=alt.Color("rsa_key_size:N", title="RSA Key Size"),
            tooltip=["rsa_key_size", "num_keys"],
        )
    )
    st.altair_chart(chart, use_container_width=True)


def _render_issue_expiry_years() -> None:
    st.subheader("Certificate Issue and Expiry Dates")
    exp_years_df = q.df(
        """
        select expiry_year, expiring_certs
        from gold.mart_cert_expiration_years
        order by expiry_year
        """
    )
    issue_years_df = q.df(
        """
        select issue_year, issued_certs
        from gold.mart_cert_issue_years
        order by issue_year
        """
    )

    if exp_years_df.empty and issue_years_df.empty:
        st.info("No certificate date distribution data found.")
        return

    if not exp_years_df.empty:
        exp_years_df["year"] = pd.DatetimeIndex(exp_years_df["expiry_year"]).year
    else:
        exp_years_df = pd.DataFrame(columns=["year", "expiring_certs"])

    if not issue_years_df.empty:
        issue_years_df["year"] = pd.DatetimeIndex(issue_years_df["issue_year"]).year
    else:
        issue_years_df = pd.DataFrame(columns=["year", "issued_certs"])

    merged_df = pd.merge(
        issue_years_df[["year", "issued_certs"]],
        exp_years_df[["year", "expiring_certs"]],
        how="outer",
        on="year",
    ).fillna(0)
    merged_df["expiring_certs"] = merged_df["expiring_certs"] * -1
    chart_df = merged_df.melt("year", var_name="type", value_name="count")

    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X("year:O", axis=alt.Axis(format="d")),
            y=alt.Y("count:Q"),
            color=alt.Color("type:N", title="Metric"),
            tooltip=["year", "type", "count"],
        )
    )
    st.altair_chart(chart, use_container_width=True)


def _render_states_and_orgs() -> None:
    left, right = st.columns(2)

    with left:
        st.subheader("Certificate Subject States")
        states_df = q.df(
            """
            select state, num_rows
            from gold.mart_cert_subject_states
            order by num_rows desc, state
            """
        )
        if states_df.empty:
            st.info("No subject state data found.")
        else:
            st.bar_chart(states_df, x="state", y="num_rows")

    with right:
        st.subheader("Certificate Organizations")
        orgs_df = q.df(
            """
            select organization, num_rows
            from gold.mart_cert_organizations
            order by num_rows desc, organization
            limit 20
            """
        )
        if orgs_df.empty:
            st.info("No subject organization data found.")
        else:
            st.bar_chart(orgs_df, x="organization", y="num_rows")


def _render_certificate_details() -> None:
    st.subheader("Certificate Detail")
    detail_df = q.df(
        """
        select
          cert_sha256,
          issuer_common_name,
          subject_common_name,
          subject_org,
          subject_state,
          issued_on,
          expires_on,
          rsa_key_size,
          is_ca
        from gold.dim_certificates
        order by expires_on nulls last, subject_common_name, cert_sha256
        limit 250
        """
    )
    st.dataframe(detail_df, hide_index=True, width="stretch")



def main():
    st.header("Certificate Data Visualization")

    missing = q.missing_tables("gold", REQUIRED_MODELS)
    if missing:
        st.warning("Certificate dbt models are not available yet.")
        st.code(
            "Missing gold models:\n- " + "\n- ".join(missing),
            language="text",
        )
        st.caption(
            "Run the dbt project so the certificate marts are materialized in the `gold` schema."
        )
        return

    # Queried only after the preflight so a missing model can't crash the page.
    any_certs = q.scalar("select count(*) from gold.fct_observation_certificates")
    if any_certs == 0:
        st.warning("No certificates found on any observations")
        return

    _render_summary()
    st.divider()
    _render_feature_summary()
    st.divider()

    top_left, top_right = st.columns([1, 1])
    with top_left:
        _render_observations_by_utility()
    with top_right:
        _render_key_sizes()

    st.divider()
    _render_issue_expiry_years()
    st.divider()
    _render_states_and_orgs()
    st.divider()
    _render_certificate_details()


if __name__ in ("__main__", "__page__"):
    main()
