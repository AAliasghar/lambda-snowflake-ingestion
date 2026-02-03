import os
import pandas as pd
import pyodbc
import re
import unicodedata
from tabulate import tabulate
from sqlalchemy import create_engine, urllib

# --- LOCAL CONFIGURATION ---
DB_CONFIG = {
    "server": "localhost", 
    "database": "CarrierWarehouse",
    "driver": "ODBC Driver 17 for SQL Server" 
}

LOCAL_INPUT_FOLDER = "./input_data" # Put your Excel/CSV files here
LOCAL_SQL_FOLDER = "./sql_queries"   # Put your .sql files here

def get_sql_server_connection():
    """Establishes connection to local SSMS."""
    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

def normalize_column(col_name: str) -> str:
    """Your professional normalization logic kept intact."""
    s = str(col_name)
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('utf-8')
    s = s.replace('(', '').replace(')', '')
    s = re.sub(r'\W+', '_', s)
    return s.upper().strip('_')

def read_local_files():
    """Replaces Google Drive logic. Reads files from a local folder."""
    dfs = {}
    for file_name in os.listdir(LOCAL_INPUT_FOLDER):
        path = os.path.join(LOCAL_INPUT_FOLDER, file_name)
        if file_name.endswith(('.xlsx', '.xls')):
            # Keeps  'SourceData' sheet logic
            dfs["outbound"] = pd.read_excel(path, sheet_name="SourceData")
            print(f"✅ Loaded {file_name} (Excel)")
        elif file_name.endswith('.csv'):
            dfs["inbound"] = pd.read_csv(path, encoding='latin-1')
            print(f"✅ Loaded {file_name} (CSV)")
    return dfs

def load_to_sql_server(df, table_name):
    """Replaces Snowflake write_pandas with SQL Server insertion."""
    if df.empty: return

    # Clean the dataframe using your existing logic
    df.columns = [normalize_column(col) for col in df.columns]
    df = df.astype(object).where(pd.notnull(df), None)

    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Create dynamic Insert Statement
    columns = ", ".join([f"[{c}]" for c in df.columns])
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Convert DF to list of tuples for fast insertion
    values = [tuple(x) for x in df.values]
    
    try:
        cursor.fast_executemany = True # Speed boost for SQL Server
        cursor.executemany(sql, values)
        conn.commit()
        print(f"🚀 Successfully loaded {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()

def run_local_sql_scripts(params):
    """Replaces GitHub logic. Reads SQL from local folder."""
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    for file_name in os.listdir(LOCAL_SQL_FOLDER):
        if file_name.endswith('.sql'):
            with open(os.path.join(LOCAL_SQL_FOLDER, file_name), 'r') as f:
                sql = f.read()
                # Your existing logic for parameter replacement
                sql = sql.replace('"+context.invoice_month+"', params["invoice_month"])
                
                print(f"📜 Executing {file_name}...")
                cursor.execute(sql)
                conn.commit()
    conn.close()

# --- MAIN EXECUTION---
if __name__ == "__main__":
    # Mock 'event' parameters
    params = {
        "invoice_month": "202506",
        "carrier": "DHL_UK"
    }

    print("Starting Local ETL Pipeline...")

    # 1. Extract
    data_frames = read_local_files()

    # 2. Transform & Load
    mapping = [
        ("outbound", "lmc_data_outbound"),
        ("inbound", "lmc_data_inbound")
    ]

    for key, table in mapping:
        df = data_frames.get(key)
        if df is not None:
            load_to_sql_server(df, table)

    # 3. Run Post-Load SQL (DWH Logic)
    run_local_sql_scripts(params)

    print("✅ ETL Job Finished Locally.")