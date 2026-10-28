import io
import json
from unittest.mock import patch

import pytest

from src.lambda_handler import _extract_s3_event_details, lambda_handler
from src.validators import validate_event


SAMPLE_CSV = b"Shipment Date,Weight (kg)\n2025-06-01,12.5\n2025-06-02,9.1\n"


def _s3_object(content: bytes) -> dict:
    """Shape of boto3's get_object response: Body has a .read() returning bytes."""
    return {"Body": io.BytesIO(content)}


# @patch applies bottom-up, so the mock arguments are in reverse decorator order.
@patch("src.lambda_handler.write_dataframe_to_snowflake", return_value=2)
@patch("src.lambda_handler.boto3.client")
def test_lambda_handler_success(mock_boto_client, mock_write):
    mock_boto_client.return_value.get_object.return_value = _s3_object(SAMPLE_CSV)

    event = {
        "bucket": "test-bucket",
        "key": "carrier_data.csv",
        "table_name": "LMC_RAW_DATA",
    }

    result = lambda_handler(event, None)

    assert result["statusCode"] == 200, result["body"]
    body = json.loads(result["body"])
    assert body["message"] == "Data ingested successfully"
    assert body["rows_loaded"] == 2
    mock_boto_client.return_value.get_object.assert_called_once_with(
        Bucket="test-bucket", Key="carrier_data.csv"
    )
    mock_write.assert_called_once()
    loaded_df, table_name = mock_write.call_args.args
    assert table_name == "LMC_RAW_DATA"
    # Columns were normalized before loading.
    assert list(loaded_df.columns) == ["SHIPMENT_DATE", "WEIGHT_KG"]
    assert len(loaded_df) == 2


@patch("src.lambda_handler.boto3.client")
def test_lambda_handler_returns_400_on_missing_table(mock_boto_client):
    result = lambda_handler({"bucket": "test-bucket", "key": "carrier_data.csv"}, None)

    assert result["statusCode"] == 400
    assert "table name" in json.loads(result["body"])["error"]
    mock_boto_client.assert_not_called()


@patch("src.lambda_handler.boto3.client")
def test_lambda_handler_returns_400_on_empty_file(mock_boto_client):
    # Header row only: parses to an empty DataFrame.
    mock_boto_client.return_value.get_object.return_value = _s3_object(b"Shipment Date,Weight\n")

    result = lambda_handler(
        {"bucket": "test-bucket", "key": "empty.csv", "table_name": "LMC_RAW_DATA"}, None
    )

    assert result["statusCode"] == 400
    assert "empty" in json.loads(result["body"])["error"]


def test_extract_s3_event_record_uses_default_table_and_decodes_key(monkeypatch):
    monkeypatch.setenv("DEFAULT_TABLE_NAME", "LMC_RAW_DATA")
    event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "test-bucket"},
                "object": {"key": "incoming/dhl+outbound%282025%29.csv"},
            }
        }]
    }

    assert _extract_s3_event_details(event) == {
        "bucket": "test-bucket",
        "key": "incoming/dhl outbound(2025).csv",
        "table_name": "LMC_RAW_DATA",
    }


def test_extract_s3_event_record_malformed_raises_value_error():
    with pytest.raises(ValueError, match="Malformed S3 event record"):
        _extract_s3_event_details({"Records": []})


def test_validate_event_requires_bucket_key_and_table():
    with pytest.raises(ValueError, match="table name"):
        validate_event({"bucket": "test-bucket", "key": "carrier_data.csv"})
