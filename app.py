import streamlit as st
from streamlit_sortables import sort_items
import pandas as pd
from sqlalchemy import create_engine

st.set_page_config(layout="wide")
st.title("🔄 Visual ETL: SQL Server → Synapse")

# --- Icon Row ---
st.markdown("### 👁️ Visual Flow")

col1, col2, col3 = st.columns([1, 0.2, 1])

with col1:
    st.image("https://img.icons8.com/color/96/000000/microsoft-sql-server.png", width=80)
    st.markdown("**SQL Server**")

with col2:
    st.markdown("<h1 style='text-align: center;'>➡️</h1>", unsafe_allow_html=True)

with col3:
    st.image("https://img.icons8.com/color/96/000000/azure-synapse-analytics.png", width=80)
    st.markdown("**Azure Synapse**")

st.divider()

# --- Drag and Drop Columns ---
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
    sql_host = st.sidebar.text_input("Host", value="localhost", key="sql_host")
    sql_db = st.sidebar.text_input("Database", value="SourceDB", key="sql_db")
    sql_user = st.sidebar.text_input("User", value="sa", key="sql_user")
    sql_pass = st.sidebar.text_input("Password", type="password", key="sql_pass")
    sql_table = st.sidebar.text_input("Source Table", value="dbo.SourceTable", key="sql_table")

# --- Sink Configuration ---
if "Azure Synapse" in sinks:
    st.sidebar.subheader("Synapse Config")
    syn_server = st.sidebar.text_input("Synapse Server", value="your-synapse.sql.azuresynapse.net", key="syn_server")
    syn_db = st.sidebar.text_input("Database", value="TargetDB", key="syn_db")
    syn_user = st.sidebar.text_input("User", value="syn_user", key="syn_user")
    syn_pass = st.sidebar.text_input("Password", type="password", key="syn_pass")
    syn_table = st.sidebar.text_input("Target Table", value="dbo.TargetTable", key="syn_table")

# --- Engine functions ---
def get_sqlserver_engine():
    return create_engine(f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_host}/{sql_db}?driver=ODBC+Driver+17+for+SQL+Server")

def get_synapse_engine():
    return create_engine(f"mssql+pyodbc://{syn_user}:{syn_pass}@{syn_server}/{syn_db}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

# --- Run ETL ---
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
        st.success("✅ Data loaded to Synapse!")
    except Exception as e:
        st.error(f"❌ Error: {e}")

