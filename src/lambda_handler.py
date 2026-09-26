import json
import os

import boto3
import pandas as pd

from src.logging_config import logger
from src.snowflake_connector import write_dataframe_to_snowflake
from src.transformations import normalize_dataframe
from src.validators import validate_event


def lambda_handler(event, context):
    """AWS Lambda entry point for processing CSV files from S3 into Snowflake."""
    try:
        payload = validate_event(event)
        bucket = payload["bucket"]
        key = payload["key"]
        table_name = payload["table_name"]

        logger.info("Starting ingestion for bucket=%s key=%s table=%s", bucket, key, table_name)

        s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
        df = pd.read_csv(body)

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
