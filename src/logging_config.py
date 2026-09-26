import logging
import os

from dotenv import load_dotenv

load_dotenv()


def configure_logging() -> logging.Logger:
    """Configure a simple logger suitable for local runs and Lambda environments."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger("lambda_snowflake_ingestion")


logger = configure_logging()
