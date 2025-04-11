import streamlit as st
from streamlit_sortables import sort_items
import pandas as pd
from sqlalchemy import create_engine

st.set_page_config(layout="wide", page_title="ETL Builder: SQL Server to Synapse")

# Title
st.markdown("<h1 style='text-align:center;'>🔄 Visual ETL Builder</h1>", unsafe_allow_html=True)
st.markdown("<h4 style='text-align:center;'>From SQL Server to Azure Synapse</h4>", unsafe_allow_html=True)
st.markdown("---")

# Visual Icons Flow
col1, col2, col3 = st.columns([1, 0.2, 1])
with col1:
    st.image("https://img.icons8.com/color/96/000000/microsoft-sql-server.png", width=80)
    st.markdown("<h5 style='text-align:center;'>SQL Server</h5>", unsafe_allow_html=True)
with col2:
    st.markdown("<h1 style='text-align:center;'>➡️</h1>", unsafe_allow_html=True)
with col3:
    st.image("https://img.icons8.com/color/96/000000/azure-synapse-analytics.png", width=80)
    st.markdown("<h5 style='text-align:center;'>Azure Synapse</h5>", unsafe_allow_html=True)

st.markdown("---")

# Drag-and-drop layout
st.subheader("📦 Configure Your Pipeline")

source_items = ["SQL Server"]
transform_items = ["No Transform"]
sink_items = ["Azure Synapse"]

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("#### 🟢 Source")
    sources = sort_items(source_items, key="sources")

with col2:
    st.markdown("#### ⚙️ Transform")
    transforms = sort_items(transform_items, key="transforms")

with col3:
    st.markdown("#### 📤 Sink")
    sinks = sort_items(sink_items, key="sinks")

# SQL Server configuration
if "SQL Server" in sources:
    st.sidebar.header("🟢 SQL Server Configuration")
    sql_host = st.sidebar.text_input("Host", value="localhost", key="sql_host")
    sql_db = st.sidebar.text_input("Database", value="SourceDB", key="sql_db")
    sql_user = st.sidebar.text_input("Username", value="sa", key="sql_user")
    sql_pass = st.sidebar.text_input("Password", type="password", key="sql_pass")
    sql_table = st.sidebar.text_input("Source Table", value="dbo.SourceTable", key="sql_table")

# Synapse configuration
if "Azure Synapse" in sinks:
    st.sidebar.header("📤 Synapse Configuration")
    syn_server = st.sidebar.text_input("Synapse Server", value="your-synapse.sql.azuresynapse.net", key="syn_server")
    syn_db = st.sidebar.text_input("Database", value="TargetDB", key="syn_db")
    syn_user = st.sidebar.text_input("Username", value="syn_user", key="syn_user")
    syn_pass = st.sidebar.text_input("Password", type="password", key="syn_pass")
    syn_table = st.sidebar.text_input("Target Table", value="dbo.TargetTable", key="syn_table")

# SQL Alchemy connection functions
def get_sqlserver_engine():
    return create_engine(f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_host}/{sql_db}?driver=ODBC+Driver+17+for+SQL+Server")

def get_synapse_engine():
    return create_engine(f"mssql+pyodbc://{syn_user}:{syn_pass}@{syn_server}/{syn_db}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

# ETL Execution
if st.button("🚀 Run ETL Pipeline"):
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
