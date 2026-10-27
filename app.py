"""Smart Waste Management Analytics - Apache Spark (PySpark) + Streamlit dashboard."""
import hashlib
import os
import tempfile

import pandas as pd
import plotly.express as px
import streamlit as st

# ---- pick the engine: Spark if it can start (needs Java), else pandas fallback ----
ENGINE_NOTE = ""
try:
    import spark_engine as engine
    engine.get_spark()
except Exception as e:  # Java missing, low memory, etc.
    import pandas_engine as engine
    ENGINE_NOTE = f"Spark could not start here ({type(e).__name__}); running the pandas fallback."

st.set_page_config(page_title="Smart Waste Analytics", page_icon="♻️", layout="wide")
HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLE = os.path.join(HERE, "waste_data.csv")


@st.cache_resource(show_spinner="Processing data...")
def load(path, key):
    return engine.load_and_clean(path)


# ---------------- sidebar ----------------
st.sidebar.title("♻️ Controls")
up = st.sidebar.file_uploader("Upload waste CSV", type="csv")
if up is not None:
    content = up.getvalue()
    key = hashlib.md5(content).hexdigest()
    path = os.path.join(tempfile.gettempdir(), f"upload_{key}.csv")
    with open(path, "wb") as fh:
        fh.write(content)
    source = f"Uploaded: {up.name}"
else:
    path, key, source = SAMPLE, "sample", "Sample data (generated, not real municipal data)"

try:
    df, stats = load(path, key)
except Exception as e:
    st.error(f"Could not read this file: {e}")
    st.stop()

if stats["rows_out"] == 0:
    st.error("No valid rows left after cleaning. Check your CSV columns and values.")
    st.stop()

wards, types, dmin, dmax = engine.get_options(df)
date_range = st.sidebar.date_input("Date range", value=(dmin, dmax), min_value=dmin, max_value=dmax)
sel_wards = st.sidebar.multiselect("Area / Ward", wards, default=wards)
sel_types = st.sidebar.multiselect("Waste type", types, default=types)
threshold = st.sidebar.slider("Count a pickup as delayed after (minutes)", 5, 60, 15, step=5)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = date_range
else:
    start = end = date_range[0] if isinstance(date_range, tuple) else date_range

if not sel_wards or not sel_types:
    st.warning("Select at least one ward and one waste type.")
    st.stop()

res = engine.analyze(df, start, end, sel_wards, sel_types, threshold)
k = res["kpi"]

# ---------------- header ----------------
st.title("♻️ Smart Waste Management Dashboard")
st.caption(f"Processing engine: **{engine.NAME}** · Source: {source}")
if ENGINE_NOTE:
    st.warning(ENGINE_NOTE)
st.caption(f"Cleaning: {stats['rows_in']} rows read, {stats['rows_dropped']} invalid rows removed, "
           f"{stats['rows_out']} kept.")

if k["pickups"] == 0:
    st.info("No records match the current filters.")
    st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Waste", f"{k['total_kg']:,.0f} kg")
c2.metric("Recyclable Waste", f"{k['recyclable_kg']:,.0f} kg", f"{k['recycling_pct']:.1f}% of total", delta_color="off")
c3.metric("Areas Covered", k["areas"])
c4.metric("Delayed Pickups", k["delayed"], f"{k['delay_rate']:.1f}% of {k['pickups']} pickups", delta_color="off")

# ---------------- charts ----------------
t1, t2, t3, t4 = st.tabs(["Waste by area", "Waste types & recycling", "Trend", "Collection delays"])

with t1:
    long = res["by_ward"].melt(id_vars="ward", value_vars=["recyclable_kg", "non_recyclable_kg"],
                               var_name="kind", value_name="kg")
    long["kind"] = long["kind"].map({"recyclable_kg": "Recyclable", "non_recyclable_kg": "Non-recyclable"})
    fig = px.bar(long, x="ward", y="kg", color="kind", title="Waste collected per ward (kg)",
                 labels={"ward": "Ward", "kg": "kg", "kind": ""}, barmode="stack")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(res["by_ward"], use_container_width=True, hide_index=True)

with t2:
    a, b = st.columns(2)
    a.plotly_chart(px.pie(res["by_type"], names="waste_type", values="total_kg", hole=0.45,
                          title="Waste-type distribution"), use_container_width=True)
    b.plotly_chart(px.bar(res["by_type"], x="waste_type", y="recycling_pct",
                          title="Recycling % by waste type",
                          labels={"waste_type": "Waste type", "recycling_pct": "Recycling %"}),
                   use_container_width=True)
    st.metric("Overall recycling percentage", f"{k['recycling_pct']:.1f}%")

with t3:
    st.plotly_chart(px.line(res["by_date"], x="date", y="total_kg", markers=True,
                            title="Daily waste collected (kg)",
                            labels={"date": "Date", "total_kg": "kg"}), use_container_width=True)

with t4:
    st.subheader("Delay by ward")
    st.dataframe(res["ward_delay"], use_container_width=True, hide_index=True)
    st.subheader(f"Delayed pickups (more than {threshold} min late)")
    d = res["delays"].copy()
    if d.empty:
        st.success("No delayed pickups for the current filters.")
    else:
        d["delay_minutes"] = d["delay_minutes"].round(0).astype(int)
        st.dataframe(d, use_container_width=True, hide_index=True)

# ---------------- export ----------------
st.divider()
st.subheader("Download report")
summary = res["by_ward"].merge(res["ward_delay"], on="ward", how="left")
d1, d2 = st.columns(2)
d1.download_button("⬇️ Summary report (CSV)", summary.to_csv(index=False).encode(),
                   "waste_summary_report.csv", "text/csv")
d2.download_button("⬇️ Filtered cleaned data (CSV)", res["rows"].to_csv(index=False).encode(),
                   "waste_filtered_data.csv", "text/csv")
