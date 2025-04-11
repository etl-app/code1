import streamlit as st
from streamlit_sortables import sort_items

st.set_page_config(layout="wide")
st.title("🧱 Streamlit Drag-and-Drop ETL Builder (Native)")

# Define initial pipeline items
source_items = ["Oracle", "S3", "Postgres"]
transform_items = ["Clean", "Filter", "Aggregate"]
sink_items = ["Snowflake", "Power BI", "BigQuery"]

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("🟢 Sources")
    sources = sort_items(source_items, key="sources")

with col2:
    st.subheader("⚙️ Transforms")
    transforms = sort_items(transform_items, key="transforms")

with col3:
    st.subheader("📤 Destinations")
    sinks = sort_items(sink_items, key="sinks")

st.write("### 📊 Pipeline Overview")
st.json({
    "sources": sources,
    "transforms": transforms,
    "sinks": sinks
})
