import streamlit as st
from streamlit_sortables import sort_items
import pandas as pd
from sqlalchemy import create_engine

# --- UI Layout ---
st.set_page_config(layout="wide")
st.title("🧱 ETL Builder: SQL Server → Synapse")

source_items = ["SQL Server"]
transform_items = ["No Transform"]
sink_items = ["Azure Synapse"]

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

# --- Source Configuration ---
if "SQL Server" in sources:
    st.sidebar.subheader("SQL Server Config")
    sql_host = st.sidebar.text_input("Host", value="localhost")
    sql_db = st.sidebar.text_input("Database", value="SourceDB")
    sql_user = st.sidebar.text_input("User", value="sa")
    sql_pass = st.sidebar.text_input("Password", type="password")
    sql_table = st.sidebar.text_input("Source Table", value="dbo.SourceTable")

# --- Sink Configuration ---
if "Azure Synapse" in sinks:
    st.sidebar.subheader("Synapse Config")
    syn_server = st.sidebar.text_input("Synapse Server", value="your-synapse.sql.azuresynapse.net")
    syn_db = st.sidebar.text_input("Database", value="TargetDB")
    syn_user = st.sidebar.text_input("User", value="syn_user")
    syn_pass = st.sidebar.text_input("Password", type="password")
    syn_table = st.sidebar.text_input("Target Table", value="dbo.TargetTable")

# --- ETL Engine ---
def get_sqlserver_engine():
    return create_engine(f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_host}/{sql_db}?driver=ODBC+Driver+17+for+SQL+Server")

def get_synapse_engine():
    return create_engine(f"mssql+pyodbc://{syn_user}:{syn_pass}@{syn_server}/{syn_db}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

# --- Run ETL ---
if st.button("🚀 Run ETL Pipeline"):
    try:
        st.info("Connecting to SQL Server...")
        df = pd.read_sql(f"SELECT * FROM {sql_table}", get_sqlserver_engine())
        st.success(f"Fetched {len(df)} rows from SQL Server")

        st.info("Loading to Synapse...")
        df.to_sql(syn_table.split('.')[-1], get_synapse_engine(), if_exists="replace", index=False, schema=syn_table.split('.')[0])
        st.success("Data successfully loaded to Synapse!")

    except Exception as e:
        st.error(f"❌ Error: {e}")
