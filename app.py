import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

st.set_page_config(layout="wide", page_title="ETL Builder: SQL Server to Synapse")

st.markdown("<h1 style='text-align:center;'>🔄 ETL Builder with Config Buttons</h1>", unsafe_allow_html=True)
st.markdown("---")

# Button section with icons
col1, col2, col3 = st.columns([1, 0.2, 1])
with col1:
    sql_clicked = st.button("🟢 SQL Server", key="sql_btn")
with col2:
    st.markdown("<h1 style='text-align:center;'>➡️</h1>", unsafe_allow_html=True)
with col3:
    synapse_clicked = st.button("📤 Azure Synapse", key="syn_btn")

# Bottom pane for SQL Server config
if sql_clicked:
    with st.expander("🔧 SQL Server Configuration", expanded=True):
        sql_host = st.text_input("Host", value="localhost", key="sql_host")
        sql_db = st.text_input("Database", value="SourceDB", key="sql_db")
        sql_user = st.text_input("Username", value="sa", key="sql_user")
        sql_pass = st.text_input("Password", type="password", key="sql_pass")
        sql_table = st.text_input("Source Table", value="dbo.SourceTable", key="sql_table")

# Bottom pane for Synapse config
if synapse_clicked:
    with st.expander("🔧 Synapse Configuration", expanded=True):
        syn_server = st.text_input("Synapse Server", value="your-synapse.sql.azuresynapse.net", key="syn_server")
        syn_db = st.text_input("Database", value="TargetDB", key="syn_db")
        syn_user = st.text_input("Username", value="syn_user", key="syn_user")
        syn_pass = st.text_input("Password", type="password", key="syn_pass")
        syn_table = st.text_input("Target Table", value="dbo.TargetTable", key="syn_table")

# ETL functions
def get_sqlserver_engine():
    return create_engine(f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_host}/{sql_db}?driver=ODBC+Driver+17+for+SQL+Server")

def get_synapse_engine():
    return create_engine(f"mssql+pyodbc://{syn_user}:{syn_pass}@{syn_server}/{syn_db}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

# Run ETL
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
