from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src import snowflake_connector

CREDS = {
    "account": "acct",
    "user": "user",
    "password": "pw",
    "database": "ANALYTICS",
    "schema": "LMC",
    "warehouse": "COMPUTE_WH",
}


@patch("src.snowflake_connector.write_pandas", return_value=(True, 1, 2, []))
@patch("src.snowflake_connector.connect")
@patch("src.snowflake_connector.get_snowflake_credentials", return_value=CREDS)
def test_write_dataframe_fetches_credentials_once(mock_creds, mock_connect, mock_write_pandas):
    df = pd.DataFrame({"A": [1, 2]})

    rows = snowflake_connector.write_dataframe_to_snowflake(df, "LMC_RAW_DATA")

    assert rows == 2
    mock_creds.assert_called_once()
    assert mock_write_pandas.call_args.kwargs["database"] == "ANALYTICS"
    assert mock_write_pandas.call_args.kwargs["schema"] == "LMC"
    mock_connect.return_value.close.assert_called_once()


@patch("src.snowflake_connector.write_pandas", return_value=(False, 1, 0, []))
@patch("src.snowflake_connector.connect")
@patch("src.snowflake_connector.get_snowflake_credentials", return_value=CREDS)
def test_write_dataframe_raises_when_write_pandas_fails(mock_creds, mock_connect, mock_write_pandas):
    with pytest.raises(RuntimeError):
        snowflake_connector.write_dataframe_to_snowflake(pd.DataFrame({"A": [1]}), "LMC_RAW_DATA")
    mock_connect.return_value.close.assert_called_once()


def test_write_dataframe_rejects_empty_dataframe():
    with pytest.raises(ValueError):
        snowflake_connector.write_dataframe_to_snowflake(pd.DataFrame(), "LMC_RAW_DATA")


def test_credentials_from_env(monkeypatch):
    for key, value in CREDS.items():
        monkeypatch.setenv(f"SNOWFLAKE_{key.upper()}", value)

    assert snowflake_connector.get_snowflake_credentials() == CREDS


def test_credentials_fall_back_to_secrets_manager(monkeypatch):
    for key in snowflake_connector.REQUIRED_SNOWFLAKE_ENV:
        monkeypatch.delenv(key, raising=False)

    fake_client = MagicMock()
    fake_client.get_secret_value.return_value = {"SecretString": '{"account": "acct"}'}
    with patch("src.snowflake_connector.boto3.client", return_value=fake_client):
        with pytest.raises(ValueError, match="Missing required Snowflake secret fields"):
            snowflake_connector.get_snowflake_credentials()
