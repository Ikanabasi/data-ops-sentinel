"""DataOps Sentinel — portfolio dashboard, UI refresh.

Uses the existing validated, versioned S3 snapshot loaders without changing
pipeline, data quality, or publication logic.
"""
from __future__ import annotations

from html import escape

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from dashboard.data_loader import get_dashboard_data, get_latest_manifest
    from dashboard.history_loader import get_history_data, get_history_manifest
except ModuleNotFoundError:
    from data_loader import get_dashboard_data, get_latest_manifest
    from history_loader import get_history_data, get_history_manifest

st.set_page_config(page_title="DataOps Sentinel | Banking Intelligence", page_icon="◈", layout="wide", initial_sidebar_state="expanded")

INK = "#EAF1FF"
MUTED = "#91A4C2"
BG = "#08111F"
CARD = "#101F33"
BORDER = "#243650"
TEAL = "#38D5C4"
BLUE = "#73A5FF"
AMBER = "#F6BD60"
RED = "#F37C8B"
PURPLE = "#B59BFF"

PRIORITIES = ["HIGH_REVIEW_PRIORITY", "MEDIUM_REVIEW_PRIORITY", "LOW_REVIEW_PRIORITY", "INSUFFICIENT_DATA", "NO_ALERT"]
PRIORITY_LABELS = {"HIGH_REVIEW_PRIORITY": "High priority", "MEDIUM_REVIEW_PRIORITY": "Medium priority", "LOW_REVIEW_PRIORITY": "Low priority", "INSUFFICIENT_DATA": "Limited data", "NO_ALERT": "No alert"}
PRIORITY_COLORS = {"HIGH_REVIEW_PRIORITY": RED, "MEDIUM_REVIEW_PRIORITY": AMBER, "LOW_REVIEW_PRIORITY": PURPLE, "INSUFFICIENT_DATA": "#647D9D", "NO_ALERT": TEAL}
METRICS = {
    "Deposits · USD millions": "DEP_USD_BILLIONS",
    "Assets · USD millions": "ASSET_USD_BILLIONS",
    "Loans · USD millions": "LNLSGR_USD_BILLIONS",
    "Return on assets · %": "ROA",
    "Return on equity · %": "ROE",
    "Noncurrent loans · %": "NCLNLSR",
    "Reported capital ratio · %": "RBCRWAJ",
}

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@400;500;600;700;800&display=swap');
:root {color-scheme:dark;}
html, body, [class*="css"] {font-family:'DM Sans',sans-serif;}
.stApp {background: radial-gradient(ellipse at 86% 0%, #132c42 0, #08111f 47%, #07101d 100%); color:#EAF1FF;}
[data-testid="stSidebar"] {background:#0B1627; border-right:1px solid #26374D;}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {color:#C9D7EB;}

/* FORCE A COMPLETE DARK THEME EVEN WHEN USER STREAMLIT SETTINGS ARE LIGHT */
html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"], [data-testid="stApp"] { color:#EAF1FF !important; }
[data-testid="stHeader"] { background:#08111F !important; color:#EAF1FF !important; }
[data-testid="stToolbar"] { color:#D9E8FA !important; }
[data-testid="stSidebar"] { background-color:#0B1627 !important; }
[data-testid="stSidebar"] *, [data-testid="stSidebar"] p,
[data-testid="stSidebar"] label, [data-testid="stSidebar"] span,
[data-testid="stSidebar"] [data-testid="stWidgetLabel"],
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] strong,
[data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 { color:#DAE8FA !important; }
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] *,
[data-testid="stSidebar"] small { color:#AFC2DD !important; }
/* Inputs: dark background and clearly visible text, labels and placeholders */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stSelectbox"] [data-baseweb="select"],
[data-testid="stMultiSelect"] [data-baseweb="select"],
[data-testid="stTextInput"] [data-baseweb="input"],
[data-testid="stDateInput"] [data-baseweb="input"] {
  background:#14263C !important; color:#F2F8FF !important;
  border:1px solid #456078 !important; border-radius:10px !important;
}
[data-testid="stTextInput"] input::placeholder,
[data-testid="stNumberInput"] input::placeholder { color:#A6BBD4 !important; opacity:1 !important; }
[data-testid="stSelectbox"] [data-baseweb="select"] *,
[data-testid="stMultiSelect"] [data-baseweb="select"] *,
[data-testid="stTextInput"] input { color:#F2F8FF !important; }
/* Keep selected pills vibrant with readable contrast */
[data-testid="stMultiSelect"] [data-baseweb="tag"] {
  background:#245B70 !important; border:1px solid #3D96A7 !important;
}
[data-testid="stMultiSelect"] [data-baseweb="tag"] * { color:#F8FDFF !important; }
/* The select popup is rendered outside the sidebar */
[data-baseweb="popover"] [role="listbox"],
[data-baseweb="popover"] [data-baseweb="menu"],
[data-baseweb="popover"] [role="option"] {
  background:#14263C !important; color:#F2F8FF !important;
}
[data-baseweb="popover"] [role="option"]:hover,
[data-baseweb="popover"] [aria-selected="true"] { background:#274963 !important; }
[data-testid="stButton"] button,
[data-testid="stDownloadButton"] button {
 background:#19334B !important; color:#F2F8FF !important;
 border:1px solid #52718D !important;
}
[data-testid="stButton"] button:hover,
[data-testid="stDownloadButton"] button:hover {
 background:#244864 !important; color:white !important; border-color:#45BFB6 !important;
}
[data-testid="stTabs"] button[role="tab"] { color:#C1D2EA !important; }
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] { color:#51E7D3 !important; }
[data-testid="stMain"] [data-testid="stMarkdownContainer"] p,
[data-testid="stMain"] [data-testid="stWidgetLabel"] *,
[data-testid="stMain"] [data-testid="stCaptionContainer"] * { color:#D5E2F4; }

.block-container {padding-top:1.3rem; padding-bottom:4rem; max-width:1530px;}
h1,h2,h3,h4 {font-family:'Manrope',sans-serif; letter-spacing:-.035em;}
h1 {font-weight:800!important; font-size:2.15rem!important;} h2 {font-weight:750!important;}
[data-testid="stMetric"] {background:linear-gradient(155deg,#14263B,#101D30); border:1px solid #2A3B54; padding:20px 20px 17px; border-radius:18px; min-height:130px;}
[data-testid="stMetricLabel"] {color:#A1B5D1;} [data-testid="stMetricValue"] {color:#F4FAFF; font-family:'Manrope',sans-serif; font-weight:800;}
[data-testid="stTabs"] [role="tablist"] {border-bottom:1px solid #293B53; gap:16px;}
[data-testid="stTabs"] button[role="tab"] {font-weight:700; padding:12px 18px; color:#92A9CA;}
[data-testid="stTabs"] button[aria-selected="true"] {color:#57DDCF;}
[data-testid="stPlotlyChart"], [data-testid="stDataFrame"] {border:1px solid #263950; border-radius:16px; overflow:hidden; background:#101D30;}
[data-testid="stTextInput"] input, [data-testid="stSelectbox"] > div, [data-testid="stMultiSelect"] > div {border-radius:12px;}
[data-testid="stButton"] button {border-radius:12px; border:1px solid #36516B; font-weight:650;}
hr {border-color:#293A51!important;}
.hero {background:linear-gradient(112deg,#15324A 0%,#12283D 46%,#101D32 100%); border:1px solid #2D4860; padding:25px 28px; border-radius:22px; margin-bottom:17px; position:relative;}
.eyebrow {color:#51DCCB; letter-spacing:.16em; text-transform:uppercase; font-size:.73rem; font-weight:800; margin-bottom:7px;}
.hero-title {font:800 2.25rem/1.16 'Manrope',sans-serif; color:#F3FAFF; letter-spacing:-.045em;}
.hero-sub {color:#A9C0D9; max-width:730px; margin-top:11px; font-size:.94rem;}
.pill {display:inline-block; background:#143B3B; color:#66EAD1; border:1px solid #2B6A67; font-size:.76rem; font-weight:750; border-radius:30px; padding:7px 12px; margin:5px 8px 0 0;}
.pill.slate {background:#1B2B41; color:#B7C9E1; border-color:#344B67;}
.section {font:800 1.11rem 'Manrope',sans-serif; letter-spacing:-.02em; color:#F2F7FF; margin:20px 0 10px;}
.info-card {background:#101F33; border:1px solid #2A3E56; border-radius:16px; padding:16px 18px; margin:6px 0;}
.card-kicker {color:#93A9C7; text-transform:uppercase; letter-spacing:.12em; font-size:.72rem; font-weight:800;}
.card-value {font:800 1.32rem 'Manrope',sans-serif; color:#F5FAFF; margin-top:6px;}
.card-note {color:#A3B4CC; font-size:.82rem; margin-top:5px;}
.notice {background:#142337; border-left:3px solid #69B6FF; border-radius:8px; padding:12px 16px; color:#B8CCE5; font-size:.85rem; margin:17px 0;}
.footer {font-size:.75rem; color:#7891B2; padding:25px 0 10px; text-align:center;}
</style>
""", unsafe_allow_html=True)


def fmt_date(raw):
    try:
        return pd.to_datetime(str(raw), format="%Y%m%d").strftime("%b %d, %Y")
    except (ValueError, TypeError):
        return str(raw)


def number(value, decimals=2, suffix=""):
    v = pd.to_numeric(value, errors="coerce")
    return f"{v:,.{decimals}f}{suffix}" if pd.notna(v) else "—"


def ncol(df, column, default=0):
    if column not in df.columns:
        return pd.Series(default, index=df.index)
    return pd.to_numeric(df[column], errors="coerce")


def plot_theme(fig, height=340, legend=True):
    fig.update_layout(
        paper_bgcolor=CARD, plot_bgcolor=CARD, font=dict(family="DM Sans", color=INK, size=12),
        margin=dict(l=22, r=22, t=26, b=24), height=height,
        xaxis=dict(gridcolor="#21334A", linecolor="#2D4662", zeroline=False, tickfont=dict(color=MUTED)),
        yaxis=dict(gridcolor="#21334A", linecolor="#2D4662", zeroline=False, tickfont=dict(color=MUTED)),
        legend=dict(orientation="h", y=1.12, x=0, font=dict(color=MUTED)), showlegend=legend,
        hoverlabel=dict(bgcolor="#1D3047", font_color=INK),
    )
    return fig


def header():
    st.markdown("""<div class="hero"><div class="eyebrow">DataOps Sentinel / Financial intelligence</div>
    <div class="hero-title">Banking Risk & Operations Monitor</div>
    <div class="hero-sub">Explore FDIC financial reporting, prioritize institutions for review and follow banking trends through a validated AWS data pipeline.</div>
    <span class="pill">● Validated snapshot monitoring</span><span class="pill slate">FDIC · AWS · Athena</span>
    </div>""", unsafe_allow_html=True)


def section(title, subtitle=None):
    st.markdown(f'<div class="section">{escape(title)}</div>', unsafe_allow_html=True)
    if subtitle:
        st.caption(subtitle)


def current_dashboard(df, manifest):
    date = fmt_date(manifest.get("report_date"))
    st.caption(f"Reporting period: {date}   ·   Published: {manifest.get('published_at', 'Unknown')}   ·   {len(df):,} validated records")
    with st.sidebar:
        st.markdown("### Monitoring filters")
        chosen = st.multiselect("Investigation priority", PRIORITIES, default=PRIORITIES, format_func=lambda x: PRIORITY_LABELS[x], key="ui_priority")
        search = st.text_input("Search institution or FDIC certificate", placeholder="Bank name or certificate", key="ui_search")
        st.caption("Filters apply to the executive overview and institution investigation.")
    filtered = df[df["investigation_priority"].isin(chosen)].copy()
    if search:
        query = search.strip()
        name = filtered["BANK_NAME"].fillna("").astype(str).str.contains(query, case=False, regex=False)
        cert = filtered["CERT"].astype(str).str.contains(query, case=False, regex=False)
        filtered = filtered[name | cert]
    priorities = filtered["investigation_priority"].value_counts()
    high = int(priorities.get("HIGH_REVIEW_PRIORITY", 0))
    med = int(priorities.get("MEDIUM_REVIEW_PRIORITY", 0))
    limitations = int(filtered["has_data_limitation"].fillna(False).astype(bool).sum())
    total = len(filtered)
    a,b,c,d = st.columns(4)
    a.metric("Institutions monitored", f"{total:,}")
    b.metric("High-priority reviews", f"{high:,}")
    c.metric("Medium-priority reviews", f"{med:,}")
    d.metric("Data limitations", f"{limitations:,}")
    section("Portfolio overview", "Priority classifications are review signals, not official bank distress ratings.")
    left,right = st.columns([1.15,1], gap="large")
    distribution = pd.DataFrame({"Priority":PRIORITIES, "Banks":[int(priorities.get(x,0)) for x in PRIORITIES]})
    distribution["Label"] = distribution["Priority"].map(PRIORITY_LABELS)
    with left:
        st.markdown("**Investigation priority breakdown**")
        fig=px.bar(distribution, x="Banks", y="Label", orientation="h", color="Priority", color_discrete_map=PRIORITY_COLORS, text="Banks")
        fig.update_layout(yaxis=dict(categoryorder="array", categoryarray=list(reversed([PRIORITY_LABELS[x] for x in PRIORITIES]))), xaxis_title=None, yaxis_title=None, bargap=.35)
        fig.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(plot_theme(fig, 330, False), use_container_width=True, config={"displayModeBar":False})
    with right:
        st.markdown("**Monitoring coverage**")
        fig=go.Figure(go.Pie(labels=["No data limitation", "Data limitation"], values=[max(0,total-limitations),limitations], hole=.76, marker=dict(colors=[TEAL,AMBER]), textinfo="percent", sort=False))
        fig.add_annotation(text=f"<b>{total:,}</b><br><span style='font-size:12px'>institutions</span>",x=.5,y=.5,showarrow=False,font=dict(color=INK,size=19))
        st.plotly_chart(plot_theme(fig,330,True), use_container_width=True, config={"displayModeBar":False})
    section("Institutions requiring attention", "Investigate flagged changes; filters above control this table.")
    top = filtered.sort_values(["alert_count","BANK_NAME"],ascending=[False,True]).copy()
    visible = ["BANK_NAME","CERT","investigation_priority","alert_count","deposit_change_pct","asset_change_pct","current_roa","has_data_limitation"]
    visible = [x for x in visible if x in top.columns]
    st.dataframe(top[visible],use_container_width=True,hide_index=True,height=305,column_config={
        "BANK_NAME":"Institution", "CERT":"FDIC certificate", "investigation_priority":"Review priority",
        "alert_count":st.column_config.NumberColumn("Signals",format="%d"),
        "deposit_change_pct":st.column_config.NumberColumn("Deposit Δ %",format="%.2f"),
        "asset_change_pct":st.column_config.NumberColumn("Asset Δ %",format="%.2f"),
        "current_roa":st.column_config.NumberColumn("ROA %",format="%.2f"),
        "has_data_limitation":"Data limitation",
    })
    st.download_button("Download filtered institution data", data=filtered.to_csv(index=False).encode("utf-8"), file_name=f"dataops_banks_{manifest.get('report_date','latest')}.csv", mime="text/csv")
    section("Institution investigation")
    if filtered.empty:
        st.info("No institutions match the selected filters.")
        return
    choices=filtered.copy()
    choices["cert_key"]=choices["CERT"].astype(str)
    choices["label"]=choices["BANK_NAME"].fillna("Unnamed bank").astype(str)+"  ·  "+choices["cert_key"]
    selected=st.selectbox("Choose an institution",choices["label"].tolist(),key="ui_current_bank")
    bank=choices.loc[choices["label"]==selected].iloc[0]
    st.markdown(f"<div class='info-card'><div class='card-kicker'>Institution profile</div><div class='card-value'>{escape(str(bank['BANK_NAME']))}</div><div class='card-note'>FDIC certificate {escape(str(bank['CERT']))} · {escape(PRIORITY_LABELS.get(str(bank['investigation_priority']),str(bank['investigation_priority'])))}</div></div>", unsafe_allow_html=True)
    cells=st.columns(5)
    indicators=[("Deposits Δ", "deposit_change_pct","%"),("Assets Δ","asset_change_pct","%"),("ROA","current_roa","%"),("Credit Δ","credit_change_pp"," pp"),("Capital Δ","capital_change_pp"," pp")]
    for cell,(title,col,unit) in zip(cells,indicators):
        cell.metric(title,number(bank.get(col),2,unit))
    st.caption(f"Monitoring alerts: {number(bank.get('alert_count'),0)} · Data limitations: {'Yes' if bool(bank.get('has_data_limitation')) else 'No'}")


def historical_dashboard(df, manifest):
    quarters=int(manifest.get("quarters",df["report_date"].nunique()))
    st.caption(f"{quarters} reporting quarters   ·   {fmt_date(manifest.get('first_report_date'))} – {fmt_date(manifest.get('latest_report_date'))}   ·   {len(df):,} historical records")
    section("Historical coverage", "Reporting institution counts across available FDIC quarters.")
    counts=(df.groupby(["report_date","quarter_label"],as_index=False).agg(banks=("CERT","nunique")).sort_values("report_date"))
    coverage=px.area(counts,x="quarter_label",y="banks",markers=True)
    coverage.update_traces(line=dict(color=TEAL,width=3),marker=dict(color=TEAL,size=6),fillcolor="rgba(56,213,196,.14)")
    coverage.update_layout(xaxis_title=None,yaxis_title="Reporting institutions")
    st.plotly_chart(plot_theme(coverage,285,False),use_container_width=True,config={"displayModeBar":False})
    section("Explore a bank", "Select an institution to inspect historical balances, profitability and monitoring signals.")
    latest_names=(df.sort_values("quarter_date").drop_duplicates("CERT",keep="last").set_index("CERT")["BANK_NAME"].to_dict())
    bank_ids=sorted(latest_names,key=lambda cert:(str(latest_names[cert]).lower(),str(cert)))
    if not bank_ids:
        st.info("No historical institutions available.")
        return
    chosen=st.selectbox("Institution",bank_ids,format_func=lambda cert:f"{latest_names[cert]} · {cert}",key="ui_history_bank")
    bank=df.loc[df["CERT"]==chosen].sort_values("quarter_date").copy()
    if bank.empty:
        st.info("No records available for this institution.")
        return
    latest=bank.iloc[-1]
    st.markdown(f"<div class='info-card'><div class='card-kicker'>Historical bank profile</div><div class='card-value'>{escape(str(latest_names[chosen]))}</div><div class='card-note'>FDIC certificate {escape(str(chosen))} · {len(bank)} quarters · {bank['quarter_date'].min():%b %Y} to {bank['quarter_date'].max():%b %Y}</div></div>",unsafe_allow_html=True)
    a,b,c,d=st.columns(4)
    a.metric("Latest deposits", ("$" + number(pd.to_numeric(latest.get("DEP_USD_BILLIONS"), errors="coerce") * 1_000, 1) + "M") if pd.notna(pd.to_numeric(latest.get("DEP_USD_BILLIONS"), errors="coerce")) else "—")
    b.metric("Latest assets", ("$" + number(pd.to_numeric(latest.get("ASSET_USD_BILLIONS"), errors="coerce") * 1_000, 1) + "M") if pd.notna(pd.to_numeric(latest.get("ASSET_USD_BILLIONS"), errors="coerce")) else "—")
    c.metric("Latest ROA",number(latest.get("ROA"),2,"%"))
    d.metric("Current alerts",number(latest.get("alert_count"),0))
    metric_name=st.selectbox("Focus metric",list(METRICS),key="ui_focus_metric")
    col=METRICS[metric_name]
    data=bank.dropna(subset=[col]).copy()
    is_amount = col in {"DEP_USD_BILLIONS", "ASSET_USD_BILLIONS", "LNLSGR_USD_BILLIONS"}
    if is_amount:
        data[col] = pd.to_numeric(data[col], errors="coerce") * 1_000
    left,right=st.columns([1.1,1],gap="large")
    with left:
        st.markdown("**Selected indicator over time**")
        if data.empty:
            st.info("This bank has no reported observations for the selected metric.")
        else:
            fig=px.area(data,x="quarter_date",y=col,markers=True,hover_data=["quarter_label"])
            fig.update_traces(line=dict(color=TEAL,width=3),marker=dict(color=TEAL,size=6),fillcolor="rgba(56,213,196,.13)")
            fig.update_layout(xaxis_title=None,yaxis_title=metric_name)
            if is_amount:
                fig.update_yaxes(tickprefix="$", ticksuffix="M", tickformat=",.0f")
                fig.update_traces(hovertemplate="%{x|%b %Y}<br>$%{y:,.1f}M<extra></extra>")
            st.plotly_chart(plot_theme(fig,320,False),use_container_width=True,config={"displayModeBar":False})
    with right:
        st.markdown("**Alerts over time**")
        alerts=bank.copy()
        alerts["alert_count"]=ncol(alerts,"alert_count").fillna(0)
        fig=px.bar(alerts,x="quarter_date",y="alert_count",color_discrete_sequence=[AMBER])
        fig.update_layout(xaxis_title=None,yaxis_title="Triggered signals")
        st.plotly_chart(plot_theme(fig,320,False),use_container_width=True,config={"displayModeBar":False})
    section("Financial performance")
    l,r=st.columns(2,gap="large")
    with l:
        st.markdown("**Balance sheet trends · USD millions**")
        names={"DEP_USD_BILLIONS":"Deposits","ASSET_USD_BILLIONS":"Assets","LNLSGR_USD_BILLIONS":"Loans"}
        data=bank[["quarter_date",*names]].melt(id_vars="quarter_date",var_name="Metric",value_name="USD millions").dropna(subset=["USD millions"])
        data["Metric"]=data["Metric"].map(names)
        # The history loader stores balances as USD billions. Convert for display only.
        data["USD millions"] = pd.to_numeric(data["USD millions"], errors="coerce") * 1_000
        fig=px.line(data,x="quarter_date",y="USD millions",color="Metric",markers=True,color_discrete_map={"Deposits":TEAL,"Assets":BLUE,"Loans":PURPLE})
        fig.update_layout(xaxis_title=None,yaxis_title="USD millions")
        fig.update_yaxes(tickprefix="$", ticksuffix="M", tickformat=",.0f")
        fig.update_traces(hovertemplate="%{x|%b %Y}<br>$%{y:,.1f}M<extra>%{fullData.name}</extra>")
        st.plotly_chart(plot_theme(fig,340,True),use_container_width=True,config={"displayModeBar":False})
    with r:
        st.markdown("**Profitability · %**")
        data=bank[["quarter_date","ROA","ROE"]].melt(id_vars="quarter_date",var_name="Metric",value_name="Percent").dropna(subset=["Percent"])
        fig=px.line(data,x="quarter_date",y="Percent",color="Metric",markers=True,color_discrete_map={"ROA":TEAL,"ROE":AMBER})
        fig.update_layout(xaxis_title=None,yaxis_title="Percent")
        st.plotly_chart(plot_theme(fig,340,True),use_container_width=True,config={"displayModeBar":False})
    section("Historical monitoring signals")
    columns=["quarter_label","alert_count","investigation_priority","deposit_change_pct","asset_change_pct","roa_change_pp","credit_change_pp","capital_change_pp"]
    st.dataframe(bank.sort_values("quarter_date",ascending=False)[columns],use_container_width=True,hide_index=True,height=300)
    st.download_button("Download selected bank history",bank.to_csv(index=False).encode("utf-8"),file_name=f"fdic_{chosen}_history.csv",mime="text/csv")
    st.markdown("<div class='notice'>Capital ratios may be zero, missing, unusually high or not applicable. These are exploratory monitoring signals, not regulatory assessments or predictions of bank failure.</div>",unsafe_allow_html=True)


header()
with st.sidebar:
    st.markdown("### ◈ DataOps Sentinel")
    st.caption("Banking intelligence workspace")
    st.divider()
    st.markdown("**DATA SOURCE**")
    st.caption("FDIC Financials · validated S3 snapshots")
    st.markdown("**REFRESH POLICY**")


@st.fragment(run_every="5min")
def render_dashboard():
    with st.sidebar:
        if st.button("↻ Check for updated snapshots",use_container_width=True):
            get_latest_manifest.clear()
            get_history_manifest.clear()
    tab1,tab2=st.tabs(["◈   Executive Overview", "⌁   Historical Explorer"])
    with tab1:
        try:
            current_df,current_manifest=get_dashboard_data()
            current_dashboard(current_df,current_manifest)
        except Exception as error:
            st.error("Couldn't load the current validated S3 snapshot. Check AWS credentials and manifest access.")
            with st.expander("Technical details"):
                st.code(str(error))
    with tab2:
        try:
            history_df,history_manifest=get_history_data()
            historical_dashboard(history_df,history_manifest)
        except Exception as error:
            st.error("Couldn't load the historical validated S3 snapshot. Check AWS credentials and manifest access.")
            with st.expander("Technical details"):
                st.code(str(error))

render_dashboard()
st.markdown("<div class='footer'>DATAOPS SENTINEL · FDIC financial reporting data · Research and exploratory monitoring only · Not a regulatory risk rating</div>",unsafe_allow_html=True)
