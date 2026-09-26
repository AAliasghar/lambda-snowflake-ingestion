import io
import json
import os

import boto3
import pandas as pd

from src.logging_config import logger
from src.snowflake_connector import write_dataframe_to_snowflake
from src.transformations import normalize_dataframe
from src.validators import validate_event


def _extract_s3_event_details(event: dict) -> dict:
    """Support both direct event payloads and AWS S3 event records."""
    if "Records" in event:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    else:
        bucket = event.get("bucket") or event.get("Bucket")
        key = event.get("key") or event.get("Key")

    if not bucket or not key:
        raise ValueError("Missing S3 bucket or object key in event payload")

    table_name = event.get("table_name") or event.get("table") or event.get("TABLE_NAME")
    if not table_name:
        if "Records" in event:
            table_name = event.get("table_name") or os.getenv("DEFAULT_TABLE_NAME")
        if not table_name:
            raise ValueError("Missing target Snowflake table name in event payload")

    return {
        "bucket": bucket,
        "key": key,
        "table_name": table_name,
    }


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
