"""
Lambda Handler Module for S3-to-Snowflake CSV Ingestion

This module provides AWS Lambda handlers for processing CSV files uploaded to S3
and ingesting them into Snowflake. It includes event parsing, data validation,
transformation, and loading functionality.

Functions:
    1. _extract_s3_event_details - Parse S3 event details from Lambda event payload
    2. lambda_handler - AWS Lambda entry point for S3-triggered ingestion
"""

import io
import json
import os
from urllib.parse import unquote_plus

import boto3
import pandas as pd

from src.logging_config import logger
from src.snowflake_connector import write_dataframe_to_snowflake
from src.transformations import normalize_dataframe
from src.validators import validate_event


# 1. _extract_s3_event_details
def _extract_s3_event_details(event: dict) -> dict:
    """Support both direct event payloads and AWS S3 event records.

    S3 notifications have no table name, so for those the table comes from
    the event (if a caller added one) or the DEFAULT_TABLE_NAME env var.
    """
    if isinstance(event, dict) and "Records" in event:
        try:
            s3_info = event["Records"][0]["s3"]
            bucket = s3_info["bucket"]["name"]
            # S3 URL-encodes object keys in events ("my file.csv" -> "my+file.csv").
            key = unquote_plus(s3_info["object"]["key"])
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError("Malformed S3 event record") from exc

        event = {
            "bucket": bucket,
            "key": key,
            "table_name": event.get("table_name") or os.getenv("DEFAULT_TABLE_NAME"),
        }

    return validate_event(event)


# 2. lambda_handler
def lambda_handler(event, context):
    """AWS Lambda entry point for S3-triggered CSV ingestion into Snowflake."""
    try:
        payload = _extract_s3_event_details(event)
        bucket = payload["bucket"]
        key = payload["key"]
        table_name = payload["table_name"]

        logger.info("Starting ingestion for bucket=%s key=%s table=%s", bucket, key, table_name)

        s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        df = pd.read_csv(io.BytesIO(body))

        if df.empty:
            raise ValueError(f"Downloaded object s3://{bucket}/{key} is empty")

        normalized_df = normalize_dataframe(df)
        rows_loaded = write_dataframe_to_snowflake(normalized_df, table_name)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data ingested successfully",
                "table_name": table_name,
                "rows_loaded": rows_loaded,
            }),
        }

    except ValueError as exc:
        logger.exception("Validation or input error during Lambda ingestion")
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Data ingestion failed",
                "error": str(exc),
            }),
        }
    except Exception as exc:
        logger.exception("Lambda ingestion failed")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Data ingestion failed",
                "error": str(exc),
            }),
        }


if __name__ == "__main__":
    sample_event = {
        "bucket": "example-bucket",
        "key": "carrier_data.csv",
        "table_name": "LMC_RAW_DATA",
    }
    print(lambda_handler(sample_event, None))
