import os
import json
import logging

from dotenv import load_dotenv

load_dotenv()


def configure_logging() -> logging.Logger:
    """Set up a logger that prints to CloudWatch-friendly stdout."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger("lambda_snowflake_ingestion")


logger = configure_logging()
