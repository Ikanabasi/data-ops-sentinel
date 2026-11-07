
import pandas as pd
import plotly.express as px
import streamlit as st

try:
    from dashboard.data_loader import (
        get_dashboard_data,
        get_latest_manifest,
    )
    from dashboard.history_loader import (
        get_history_data,
        get_history_manifest,
    )
except ModuleNotFoundError:
    from data_loader import (
        get_dashboard_data,
        get_latest_manifest,
    )
    from history_loader import (
        get_history_data,
        get_history_manifest,
    )


st.set_page_config(
    page_title="DataOps Sentinel",
    page_icon="🏦",
    layout="wide",
)

PRIORITIES = [
    "HIGH_REVIEW_PRIORITY",
    "MEDIUM_REVIEW_PRIORITY",
    "LOW_REVIEW_PRIORITY",
    "INSUFFICIENT_DATA",
    "NO_ALERT",
]

METRICS = {
    "Deposits (USD billions)": "DEP_USD_BILLIONS",
    "Assets (USD billions)": "ASSET_USD_BILLIONS",
    "Loans (USD billions)": "LNLSGR_USD_BILLIONS",
    "Return on Assets (%)": "ROA",
    "Return on Equity (%)": "ROE",
    "Noncurrent Loans Ratio (%)": "NCLNLSR",
    "Reported Capital Ratio (%)": "RBCRWAJ",
}

st.title("DataOps Sentinel")
st.caption(
    "Banking Risk & Operations Monitor | "
    "FDIC Financial Data"
)


def display_current_dashboard(df, manifest):
    """Show current-quarter banking monitoring."""

    date_label = pd.to_datetime(
        manifest["report_date"],
        format="%Y%m%d",
    ).strftime("%B %d, %Y")

    st.success(
        f"Validated snapshot loaded | "
        f"Reporting date: {date_label}"
    )

    st.caption(
        f"Published: {manifest['published_at']} | "
        f"Records: {len(df):,}"
    )

    st.subheader("Monitoring Filters")

    priorities = st.multiselect(
        "Investigation priority",
        PRIORITIES,
        default=PRIORITIES,
        key="current_priority_filter",
    )

    search = st.text_input(
        "Search by bank name or FDIC certificate",
        key="current_bank_search",
    )

    filtered = df[
        df["investigation_priority"].isin(priorities)
    ].copy()

    if search:
        name_match = (
            filtered["BANK_NAME"]
            .fillna("")
            .str.contains(
                search,
                case=False,
                regex=False,
            )
        )

        cert_match = (
            filtered["CERT"]
            .astype(str)
            .str.contains(
                search,
                case=False,
                regex=False,
            )
        )

        filtered = filtered[
            name_match | cert_match
        ]

    st.subheader("Executive Overview")

    high = (
        filtered["investigation_priority"]
        == "HIGH_REVIEW_PRIORITY"
    ).sum()

    medium = (
        filtered["investigation_priority"]
        == "MEDIUM_REVIEW_PRIORITY"
    ).sum()

    limitations = filtered[
        "has_data_limitation"
    ].sum()

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Banks Monitored", f"{len(filtered):,}")
    c2.metric("High Priority", f"{high:,}")
    c3.metric("Medium Priority", f"{medium:,}")
    c4.metric("Data Limitations", f"{limitations:,}")

    st.subheader("Investigation Priority Distribution")

    counts = (
        filtered["investigation_priority"]
        .value_counts()
        .reindex(PRIORITIES, fill_value=0)
        .reset_index()
    )

    counts.columns = ["Priority", "Banks"]

    fig = px.bar(
        counts,
        x="Priority",
        y="Banks",
        text="Banks",
    )

    fig.update_layout(
        xaxis_title=None,
        yaxis_title="Number of Banks",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.subheader("Bank Explorer")

    visible_columns = [
        "CERT",
        "BANK_NAME",
        "investigation_priority",
        "alert_count",
        "deposit_change_pct",
        "asset_change_pct",
        "current_roa",
        "credit_change_pp",
        "capital_change_pp",
        "has_data_limitation",
    ]

    st.dataframe(
        filtered[visible_columns],
        use_container_width=True,
        hide_index=True,
    )

    if filtered.empty:
        st.info("No banks match the current filters.")
        return

    st.subheader("Individual Bank Investigation")

    bank_options = (
        filtered["BANK_NAME"].astype(str)
        + " | "
        + filtered["CERT"].astype(str)
    ).tolist()

    selected = st.selectbox(
        "Select an institution",
        bank_options,
        key="current_bank_selector",
    )

    cert = selected.rsplit(" | ", 1)[-1]

    bank = filtered.loc[
        filtered["CERT"] == cert
    ].iloc[0]

    st.markdown(f"### {bank['BANK_NAME']}")

    st.write(
        "Investigation priority:",
        bank["investigation_priority"],
    )

    st.write(
        "Triggered indicators:",
        int(bank["alert_count"]),
    )

    indicator_columns = {
        "Deposit change (%)": "deposit_change_pct",
        "Asset change (%)": "asset_change_pct",
        "Return on assets (%)": "current_roa",
        "Credit change (pp)": "credit_change_pp",
        "Capital change (pp)": "capital_change_pp",
    }

    cols = st.columns(3)

    for i, (label, column) in enumerate(
        indicator_columns.items()
    ):
        value = pd.to_numeric(
            bank[column],
            errors="coerce",
        )

        display = (
            f"{value:.2f}"
            if pd.notna(value)
            else "N/A"
        )

        cols[i % 3].metric(label, display)


def display_historical_dashboard(df, manifest):
    """Display bank-specific financial history."""

    st.success(
        "Historical snapshot verified | "
        f"{manifest['quarters']} reporting quarters"
    )

    st.caption(
        f"Coverage: {manifest['first_report_date']} "
        f"to {manifest['latest_report_date']} | "
        f"Records: {len(df):,} | "
        f"Published: {manifest['published_at']}"
    )

    st.subheader("Historical Banking Overview")

    quarter_counts = (
        df.groupby(
            ["report_date", "quarter_label"],
            as_index=False,
        )
        .agg(banks=("CERT", "nunique"))
        .sort_values("report_date")
    )

    fig_overview = px.line(
        quarter_counts,
        x="quarter_label",
        y="banks",
        markers=True,
        title="Reporting Institutions by Quarter",
    )

    fig_overview.update_layout(
        xaxis_title="Reporting Quarter",
        yaxis_title="Number of Institutions",
    )

    st.plotly_chart(
        fig_overview,
        use_container_width=True,
    )

    st.divider()
    st.subheader("Individual Bank Historical Explorer")

    # A bank may have a different name in older quarters.
    # Use CERT as the stable identifier.
    latest_bank_names = (
        df.sort_values("quarter_date")
        .drop_duplicates("CERT", keep="last")
        .set_index("CERT")["BANK_NAME"]
        .to_dict()
    )

    bank_ids = sorted(
        latest_bank_names,
        key=lambda cert: (
            str(latest_bank_names[cert]).lower(),
            cert,
        ),
    )

    selected_cert = st.selectbox(
        "Select a bank",
        options=bank_ids,
        format_func=lambda cert: (
            f"{latest_bank_names[cert]} | {cert}"
        ),
        key="historical_bank_selector",
    )

    bank = (
        df.loc[df["CERT"] == selected_cert]
        .sort_values("quarter_date")
        .copy()
    )

    if bank.empty:
        st.info("No historical records available.")
        return

    st.markdown(
        f"### {latest_bank_names[selected_cert]}"
    )

    st.caption(
        f"FDIC Certificate: {selected_cert} | "
        f"Available quarters: {len(bank)}"
    )

    first = bank["quarter_date"].min()
    last = bank["quarter_date"].max()

    st.caption(
        f"Reporting coverage: "
        f"{first:%B %Y} – {last:%B %Y}"
    )

    latest = bank.iloc[-1]

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Latest ROA",
        (
            f"{latest['ROA']:.2f}%"
            if pd.notna(latest["ROA"])
            else "N/A"
        ),
    )

    c2.metric(
        "Latest Deposits",
        (
            f"${latest['DEP_USD_BILLIONS']:,.2f}B"
            if pd.notna(latest["DEP_USD_BILLIONS"])
            else "N/A"
        ),
    )

    c3.metric(
        "Latest Monitoring Alerts",
        f"{int(latest['alert_count'])}",
    )

    metric = st.selectbox(
        "Choose financial indicator",
        options=list(METRICS),
        key="historical_metric_selector",
    )

    selected_column = METRICS[metric]

    chart_data = bank.dropna(
        subset=[selected_column]
    )

    if chart_data.empty:
        st.warning(
            "No reported values available for this metric."
        )
    else:
        fig = px.line(
            chart_data,
            x="quarter_date",
            y=selected_column,
            markers=True,
            title=f"{metric} Over Time",
            hover_data=["quarter_label"],
        )

        fig.update_layout(
            xaxis_title="Reporting Quarter",
            yaxis_title=metric,
            hovermode="x unified",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    st.subheader("Deposits, Assets and Loans")

    balances = bank[
        [
            "quarter_date",
            "DEP_USD_BILLIONS",
            "ASSET_USD_BILLIONS",
            "LNLSGR_USD_BILLIONS",
        ]
    ].melt(
        id_vars="quarter_date",
        var_name="Indicator",
        value_name="USD Billions",
    )

    labels = {
        "DEP_USD_BILLIONS": "Deposits",
        "ASSET_USD_BILLIONS": "Assets",
        "LNLSGR_USD_BILLIONS": "Loans",
    }

    balances["Indicator"] = (
        balances["Indicator"].map(labels)
    )

    fig_balances = px.line(
        balances.dropna(subset=["USD Billions"]),
        x="quarter_date",
        y="USD Billions",
        color="Indicator",
        markers=True,
        title="Balance Sheet Trends",
    )

    st.plotly_chart(
        fig_balances,
        use_container_width=True,
    )

    st.subheader("Profitability Trends")

    profitability = bank[
        ["quarter_date", "ROA", "ROE"]
    ].melt(
        id_vars="quarter_date",
        var_name="Metric",
        value_name="Percent",
    )

    fig_profit = px.line(
        profitability.dropna(subset=["Percent"]),
        x="quarter_date",
        y="Percent",
        color="Metric",
        markers=True,
        title="ROA and ROE",
    )

    st.plotly_chart(
        fig_profit,
        use_container_width=True,
    )

    st.subheader("Historical Investigation Alerts")

    priority_history = bank[
        [
            "quarter_label",
            "alert_count",
            "investigation_priority",
            "deposit_change_pct",
            "asset_change_pct",
            "roa_change_pp",
            "credit_change_pp",
            "capital_change_pp",
        ]
    ].sort_values(
        "quarter_label",
        ascending=False,
    )

    st.dataframe(
        priority_history,
        use_container_width=True,
        hide_index=True,
    )

    st.info(
        "Reported capital ratios may be zero, missing, "
        "unusually high or not applicable for particular "
        "institutions. Monitoring indicators are exploratory "
        "review signals, not official regulatory assessments."
    )


@st.fragment(run_every="5min")
def render_dashboard():
    """Check for updated S3 manifests periodically."""

    if st.button("Check for new data"):
        get_latest_manifest.clear()
        get_history_manifest.clear()

    tab1, tab2 = st.tabs([
        "Executive Overview",
        "Historical Explorer",
    ])

    with tab1:
        try:
            current_df, current_manifest = (
                get_dashboard_data()
            )

            display_current_dashboard(
                current_df,
                current_manifest,
            )
        except Exception as error:
            st.error(
                "Unable to load the current-quarter snapshot."
            )
            st.code(str(error))

    with tab2:
        try:
            history_df, history_manifest = (
                get_history_data()
            )

            display_historical_dashboard(
                history_df,
                history_manifest,
            )
        except Exception as error:
            st.error(
                "Unable to load the historical snapshot."
            )
            st.code(str(error))


render_dashboard()

st.divider()
st.caption(
    "DataOps Sentinel | FDIC financial reporting data. "
    "Exploratory monitoring and analytics only. "
    "Not a regulatory risk rating or prediction of "
    "bank failure."
)
