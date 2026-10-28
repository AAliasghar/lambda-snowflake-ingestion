import json
import os
from typing import Optional

import boto3
from dotenv import load_dotenv
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

from src.logging_config import logger

load_dotenv()


REQUIRED_SNOWFLAKE_ENV = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
]


def _missing_env_vars() -> list[str]:
    """Return a list of missing required Snowflake environment variables."""
    return [key for key in REQUIRED_SNOWFLAKE_ENV if not os.getenv(key)]


def get_snowflake_credentials() -> dict:
    """Return credentials from environment variables or AWS Secrets Manager."""
    missing = _missing_env_vars()

    if not missing:
        return {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        }

    secret_name = os.getenv("SNOWFLAKE_SECRET_NAME", "snowflake/prod")
    region = os.getenv("AWS_REGION", "us-east-1")
    client = boto3.client("secretsmanager", region_name=region)
    secret = client.get_secret_value(SecretId=secret_name)
    secret_payload = json.loads(secret["SecretString"])

    # Validate Secret Manager config before proceeding.
    required_secret_fields = [
        "account",
        "user",
        "password",
        "database",
        "schema",
        "warehouse",
    ]
    missing_secret_fields = [field for field in required_secret_fields if not secret_payload.get(field)]
    if missing_secret_fields:
        raise ValueError(
            "Missing required Snowflake secret fields: " + ", ".join(missing_secret_fields)
        )

    return {
        "account": secret_payload["account"],
        "user": secret_payload["user"],
        "password": secret_payload["password"],
        "database": secret_payload["database"],
        "schema": secret_payload["schema"],
        "warehouse": secret_payload["warehouse"],
    }


def get_snowflake_connection(creds: Optional[dict] = None):
    """Open and return a Snowflake connection."""
    if creds is None:
        creds = get_snowflake_credentials()
    return connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        database=creds["database"],
        schema=creds["schema"],
        warehouse=creds["warehouse"],
    )


def write_dataframe_to_snowflake(df, table_name: str) -> int:
    """Load a pandas dataframe into Snowflake and return rows inserted."""
    if not table_name:
        raise ValueError("Target Snowflake table name is required")

    if df is None or df.empty:
        raise ValueError("DataFrame is empty; cannot write to Snowflake")

    # Fetch credentials once: on the Secrets Manager path every call is an AWS API call.
    creds = get_snowflake_credentials()
    conn = get_snowflake_connection(creds)
    try:
        success, _num_chunks, num_rows, _output = write_pandas(
            conn,
            df,
            table_name=table_name,
            database=creds["database"],
            schema=creds["schema"],
            quote_identifiers=False,
            auto_create_table=False,
            overwrite=False,
        )
        if not success:
            raise RuntimeError(f"write_pandas reported failure loading table {table_name}")
        logger.info("Successfully loaded %s rows to table %s", num_rows, table_name)
        return num_rows
    finally:
        conn.close()
