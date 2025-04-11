import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

st.set_page_config(layout="wide", page_title="ETL Builder: SQL Server to Synapse")

# Title
st.markdown("<h1 style='text-align:center;'>🔄 Low-Code ETL Builder</h1>", unsafe_allow_html=True)
st.markdown("<h4 style='text-align:center;'>Build your pipeline using palette components</h4>", unsafe_allow_html=True)
st.markdown("---")

# Define available components
component_palette = {
    "Sources": ["SQL Server", "MySQL", "S3"],
    "Transforms": ["No Transform", "Filter", "Aggregate"],
    "Sinks": ["Azure Synapse", "Snowflake", "BigQuery"]
}

# Select pipeline components from palette
st.sidebar.header("🧩 Build Your ETL Pipeline")

source_selected = st.sidebar.selectbox("Select Source", component_palette["Sources"], key="source_select")
transform_selected = st.sidebar.selectbox("Select Transform", component_palette["Transforms"], key="transform_select")
sink_selected = st.sidebar.selectbox("Select Sink", component_palette["Sinks"], key="sink_select")

# Visual Layout
st.subheader("📊 Pipeline Flow")
col1, col2, col3 = st.columns([1, 0.2, 1])
with col1:
    if source_selected == "SQL Server":
        st.image("https://img.icons8.com/color/96/000000/microsoft-sql-server.png", width=80)
    st.markdown(f"<h5 style='text-align:center;'>{source_selected}</h5>", unsafe_allow_html=True)

with col2:
    st.markdown("<h1 style='text-align:center;'>➡️</h1>", unsafe_allow_html=True)

with col3:
    if sink_selected == "Azure Synapse":
        st.image("https://img.icons8.com/color/96/000000/azure-synapse-analytics.png", width=80)
    st.markdown(f"<h5 style='text-align:center;'>{sink_selected}</h5>", unsafe_allow_html=True)

st.markdown("---")

# SQL Server configuration
if source_selected == "SQL Server":
    st.sidebar.subheader("🟢 SQL Server Config")
    sql_host = st.sidebar.text_input("Host", value="localhost", key="sql_host")
    sql_db = st.sidebar.text_input("Database", value="SourceDB", key="sql_db")
    sql_user = st.sidebar.text_input("Username", value="sa", key="sql_user")
    sql_pass = st.sidebar.text_input("Password", type="password", key="sql_pass")
    sql_table = st.sidebar.text_input("Source Table", value="dbo.SourceTable", key="sql_table")

# Synapse configuration
if sink_selected == "Azure Synapse":
    st.sidebar.subheader("📤 Synapse Config")
    syn_server = st.sidebar.text_input("Synapse Server", value="your-synapse.sql.azuresynapse.net", key="syn_server")
    syn_db = st.sidebar.text_input("Database", value="TargetDB", key="syn_db")
    syn_user = st.sidebar.text_input("Username", value="syn_user", key="syn_user")
    syn_pass = st.sidebar.text_input("Password", type="password", key="syn_pass")
    syn_table = st.sidebar.text_input("Target Table", value="dbo.TargetTable", key="syn_table")

# ETL execution functions
def get_sqlserver_engine():
    return create_engine(f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_host}/{sql_db}?driver=ODBC+Driver+17+for+SQL+Server")

def get_synapse_engine():
    return create_engine(f"mssql+pyodbc://{syn_user}:{syn_pass}@{syn_server}/{syn_db}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

# Run ETL
if st.button("🚀 Run ETL Pipeline"):
    if source_selected == "SQL Server" and sink_selected == "Azure Synapse":
        try:
            st.info("Connecting to SQL Server...")
            df = pd.read_sql(f"SELECT * FROM {sql_table}", get_sqlserver_engine())
            st.success(f"✅ Pulled {len(df)} rows from SQL Server")
            st.dataframe(df.head())

            st.info("Pushing to Synapse...")
            df.to_sql(
                name=syn_table.split('.')[-1],
                con=get_synapse_engine(),
                if_exists="replace",
                index=False,
                schema=syn_table.split('.')[0]
            )
            st.success("✅ Data successfully loaded into Synapse!")
        except Exception as e:
            st.error(f"❌ Error: {e}")
    else:
        st.warning("Pipeline execution is only available for SQL Server → Azure Synapse in this version.")
