import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# --- CONFIGURATION ---
st.title("🔄 SQL Server to Synapse ETL")

with st.sidebar:
    st.header("SQL Server Source")
    sqlserver_host = st.text_input("Host", value="localhost")
    sqlserver_db = st.text_input("Database", value="SourceDB")
    sqlserver_user = st.text_input("Username", value="sa")
    sqlserver_pass = st.text_input("Password", type="password")
    sqlserver_table = st.text_input("Table", value="dbo.SourceTable")

    st.header("Azure Synapse Target")
    synapse_server = st.text_input("Synapse Server", value="your-synapse.sql.azuresynapse.net")
    synapse_db = st.text_input("Database", value="TargetDB")
    synapse_user = st.text_input("Username", value="synapse_user")
    synapse_pass = st.text_input("Password", type="password")
    target_table = st.text_input("Target Table", value="dbo.TargetTable")

# --- SQL Server connection ---
def get_sqlserver_engine():
    conn_str = f"mssql+pyodbc://{sqlserver_user}:{sqlserver_pass}@{sqlserver_host}/{sqlserver_db}?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(conn_str)

# --- Synapse connection ---
def get_synapse_engine():
    conn_str = f"mssql+pyodbc://{synapse_user}:{synapse_pass}@{synapse_server}/{synapse_db}?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(conn_str, fast_executemany=True)

# --- Run ETL ---
if st.button("Run ETL"):
    try:
        st.info("Connecting to SQL Server...")
        src_engine = get_sqlserver_engine()
        df = pd.read_sql(f"SELECT * FROM {sqlserver_table}", src_engine)
        st.success(f"Fetched {len(df)} records from SQL Server")

        # Optional transformation preview
        st.write("🔍 Data Preview", df.head())

        st.info("Connecting to Synapse...")
        dest_engine = get_synapse_engine()
        df.to_sql(target_table.split('.')[-1], dest_engine, if_exists="replace", index=False, schema=target_table.split('.')[0])
        st.success(f"Loaded data into Synapse: {target_table}")

    except Exception as e:
        st.error(f"❌ ETL Failed: {e}")
