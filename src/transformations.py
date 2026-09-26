import os
import re
import unicodedata

import pandas as pd


def normalize_column(col_name: str) -> str:
    """Convert arbitrary column names into Snowflake-safe uppercase identifiers."""
    s = str(col_name)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = s.replace("(", "").replace(")", "")
    s = re.sub(r"\W+", "_", s)
    return s.upper().strip("_")


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe names, preserve values, and remove nulls in a Snowflake-friendly way."""
    if df is None or df.empty:
        raise ValueError("Received empty dataframe for ingestion")

    normalized = df.copy()
    normalized.columns = [normalize_column(col) for col in normalized.columns]
    normalized = normalized.where(pd.notnull(normalized), None)
    return normalized
