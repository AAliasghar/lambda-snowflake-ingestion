import os
import datetime
import pandas as pd
import pyodbc
import re
import unicodedata
import urllib  
from sqlalchemy import create_engine
from tabulate import tabulate
import sys
import random
import os
from datetime import datetime, timedelta
from datetime import datetime



# 
OUTPUT_PATH = r"C:\Users\Documents\Data_Engineering\projects\lambda-snowflake-ingestion\datasets\source_lmc"

def generate_random_date(month="2025-06"):
    start = datetime.strptime(f"{month}-01", "%Y-%m-%d")
    return (start + timedelta(days=random.randint(0, 28))).strftime("%Y-%m-%d")

def create_mock_data():
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)

    # 1. Generate Outbound Data
    outbound_data = []
    for i in range(1000):
        outbound_data.append({
            "SHIPMENTDATE": generate_random_date(),
            "BARCODE": f"BAR{random.randint(10000, 99999)}",
            "ACCOUNTNUMBER": f"ACC-{random.randint(100, 999)}",
            "INVOICE_NUMBER": f"INV-{random.randint(5000, 6000)}",
            "Consignment": f"CONS-{random.randint(1000, 2000)}",
            "REFERENCE": f"REF-{i:03d}",
            "WEIGHT": round(random.uniform(0.5, 30.0), 2),
            "VOLUMETRIC_WEIGHT": round(random.uniform(1.0, 35.0), 2),
            "LENGTH": random.randint(10, 100),
            "WIDTH": random.randint(10, 100),
            "HEIGHT": random.randint(10, 100),
            "PRODUCT_TYPE_NAME": random.choice(["Express", "Standard", "Economy"]),
            "Destination": random.choice(["London", "Manchester", "Birmingham", "Glasgow"]),
            "ZONE": random.choice(["Zone 1", "Zone 2", "Zone 3"]),
            "BASE_RATE": round(random.uniform(5.0, 50.0), 2),
            "FINAL_PRICE": round(random.uniform(10.0, 100.0), 2)
        })
    
    df_outbound = pd.DataFrame(outbound_data)
    outbound_file = os.path.join(OUTPUT_PATH, "dhl_outbound_sample.csv")
    df_outbound.to_csv(outbound_file, index=False)
    print(f"✅ Generated: {outbound_file}")

    # 2. Generate Inbound Data
    inbound_data = []
    for i in range(1000):
        inbound_data.append({
            "SHIPMENTDATE": generate_random_date(),
            "Consignment": f"IN-CONS-{random.randint(3000, 4000)}",
            "Customer": f"Client_{random.choice(['A', 'B', 'C'])}",
            "INVOICE": f"INV-IN-{random.randint(7000, 8000)}",
            "REFERENCE": f"IN-REF-{i:03d}",
            "WEIGHT": round(random.uniform(1.0, 50.0), 2),
            "ORIGIN": random.choice(["Paris", "Berlin", "Madrid"]),
            "DESTINATION": "Coventry Hub",
            "ZONE": "International",
            "VALUE": round(random.uniform(100.0, 5000.0), 2)
        })

    df_inbound = pd.DataFrame(inbound_data)
    inbound_file = os.path.join(OUTPUT_PATH, "dhl_inbound_sample.csv")
    df_inbound.to_csv(inbound_file, index=False)
    print(f"✅ Generated: {inbound_file}")

# --- LOCAL CONFIGURATION ---
DB_CONFIG = {
    "server": "LAPTOP-1V6T0S6A\SQLEXPRESS", 
    "database": "CarrierWarehouse",
    "driver": "ODBC Driver 17 for SQL Server" 
}

LOCAL_INPUT_FOLDER = r"C:\Users\Documents\Data_Engineering\projects\lambda-snowflake-ingestion\datasets\source_lmc" # Put your Excel/CSV files here
LOCAL_SQL_FOLDER = "./sql_queries"   # Put your .sql files here



# --- SQL SERVER CONNECTION ---
def get_sql_server_connection():
    """Establishes connection to local SSMS with status feedback."""
    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        "Trusted_Connection=yes;"
    )
    
    try:
        # Attempt to connect
        conn = pyodbc.connect(conn_str)
        
        # --- YOUR SUCCESS PRINT STATEMENT ---
        print(f"✅ CONNECTION SUCCESSFUL: Connected to SQL Server [{DB_CONFIG['server']}] "
              f"using Database [{DB_CONFIG['database']}] at {datetime.now().strftime('%H:%M:%S')}")
        
        return conn
        
    except Exception as e:
        # If it fails, we want to know exactly why (wrong driver? wrong DB name?)
        print(f"❌ CONNECTION FAILED: Could not connect to SQL Server.")
        print(f"Error Details: {e}")
        return None
    

# --- LOCAL FILE READING & SQL EXECUTION ---
def read_local_files():
    """Replaces Google Drive logic. Reads files from a local folder."""
    dfs = {}
    
    # Check if folder exists; if not, create it and stop
    if not os.path.exists(LOCAL_INPUT_FOLDER):
        os.makedirs(LOCAL_INPUT_FOLDER)
        print(f"⚠️ Created missing folder: {LOCAL_INPUT_FOLDER}. Please drop your Excel files there and run again.")
        print("🛑 Script stopping: Please add your files and restart.")
        sys.exit() # Stops the program
        return dfs

    files = os.listdir(LOCAL_INPUT_FOLDER)
    if not files:
        print(f"⚠️ Folder {LOCAL_INPUT_FOLDER} is empty. No data to process.")
        print("🛑 Stopping program execution.")
        sys.exit()
        return dfs
    
    for file_name in files:
        path = os.path.join(LOCAL_INPUT_FOLDER, file_name)
        fname_lower = file_name.lower()
        # Determine if it's Outbound or Inbound based on the NAME
        key = None
        if "outbound" in fname_lower:
            key = "outbound"
        elif "inbound" in fname_lower:
            key = "inbound"
            
      # If we found a valid key, load the data
        if key:
            try:
                if file_name.endswith(('.xlsx', '.xls')):
                    dfs[key] = pd.read_excel(path, sheet_name="SourceData")
                    print(f"✅ Loaded {file_name} as {key.upper()} (Excel)")
                elif file_name.endswith('.csv'):
                    dfs[key] = pd.read_csv(path, encoding='latin-1')
                    print(f"✅ Loaded {file_name} as {key.upper()} (CSV)")
            except Exception as e:
                print(f"❌ Error reading {file_name}: {e}")
        else:
            print(f"ℹ️ Skipping {file_name} (No 'inbound' or 'outbound' in name)")
            
    return dfs

# --- DATA CLEANING & NORMALIZATION ---
def normalize_column(col_name: str) -> str:
    """Your professional normalization logic kept intact."""
    s = str(col_name)
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('utf-8')
    s = s.replace('(', '').replace(')', '')
    s = re.sub(r'\W+', '_', s)
    return s.upper().strip('_')



# --- SQL SERVER LOADING & POST-LOAD LOGIC ---
def load_to_sql_server(df, table_name):
    """Replaces Snowflake write_pandas with SQL Server insertion."""
    if df.empty: return

    # Clean the dataframe using your existing logic
    df.columns = [normalize_column(col) for col in df.columns]
    df = df.astype(object).where(pd.notnull(df), None)

    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Create dynamic Insert Statement based on DF columns
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

# def run_local_sql_scripts(params):
#     """Replaces GitHub logic. Reads SQL from local folder."""
#     conn = get_sql_server_connection()
#     cursor = conn.cursor()

#     for file_name in os.listdir(LOCAL_SQL_FOLDER):
#         if file_name.endswith('.sql'):
#             with open(os.path.join(LOCAL_SQL_FOLDER, file_name), 'r') as f:
#                 sql = f.read()
#                 # Your existing logic for parameter replacement
#                 sql = sql.replace('"+context.invoice_month+"', params["invoice_month"])
                
#                 print(f"📜 Executing {file_name}...")
#                 cursor.execute(sql)
#                 conn.commit()
#     conn.close()

# --- MAIN EXECUTION---
if __name__ == "__main__":
    
     # Mock dataset creation for local testing 
    create_mock_data()
    
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
        ("outbound", "bronze.lmc_data_outbound"),
        ("inbound", "bronze.lmc_data_inbound")
    ]

    for key, table in mapping:
        df = data_frames.get(key)
        if df is not None:
            load_to_sql_server(df, table)

    # 3. Run Post-Load SQL (DWH Logic)
    # run_local_sql_scripts(params)

    print("✅ ETL Job Finished Locally.")