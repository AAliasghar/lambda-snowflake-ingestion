import os
import json

import boto3
from dotenv import load_dotenv
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

from src.logging_config import logger

load_dotenv()


def get_snowflake_credentials() -> dict:
    """Return Snowflake credentials from env vars or AWS Secrets Manager."""
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

    if all([account, user, password, database, schema, warehouse]):
        return {
            "account": account,
            "user": user,
            "password": password,
            "database": database,
            "schema": schema,
            "warehouse": warehouse,
        }

    secret_name = os.getenv("SNOWFLAKE_SECRET_NAME", "snowflake/prod")
    region = os.getenv("AWS_REGION", "us-east-1")
    client = boto3.client("secretsmanager", region_name=region)
    secret = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret["SecretString"])


def get_snowflake_connection():
    """Open a Snowflake connection."""
    creds = get_snowflake_credentials()
    conn = connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        database=creds["database"],
        schema=creds["schema"],
        warehouse=creds["warehouse"],
    )
    logger.info("Connected to Snowflake warehouse %s", creds.get("warehouse"))
    return conn


def write_dataframe_to_snowflake(df, table_name: str) -> int:
    """Write a pandas dataframe to a Snowflake table and return rows loaded."""
    if not table_name:
        raise ValueError("Target Snowflake table name is required")

    conn = get_snowflake_connection()
    try:
        write_pandas(
            conn,
            df,
            table_name=table_name,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            quote_identifiers=False,
            auto_create_table=False,
            overwrite=False,
        )
        logger.info("Successfully loaded %s rows to Snowflake table %s", len(df), table_name)
        return len(df)
    finally:
        conn.close()
