import streamlit as st
from streamlit_dragdrop import dragdrop

st.set_page_config(layout="wide")
st.title("🧱 Native Drag-and-Drop ETL Pipeline Builder")

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("🟢 Sources")
    sources = dragdrop("sources", items=["Oracle", "S3", "Postgres"])

with col2:
    st.subheader("⚙️ Transforms")
    transforms = dragdrop("transforms", items=["Clean", "Filter", "Aggregate"])

with col3:
    st.subheader("📤 Destinations")
    destinations = dragdrop("sinks", items=["Snowflake", "Power BI", "BigQuery"])

st.write("### 📊 Pipeline Overview")
st.json({
    "sources": sources,
    "transforms": transforms,
    "sinks": destinations
})
