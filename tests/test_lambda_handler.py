from unittest.mock import MagicMock, patch

from src.lambda_handler import lambda_handler


@patch("src.lambda_handler.write_dataframe_to_snowflake", return_value=3)
@patch("src.lambda_handler.pd.read_csv")
@patch("src.lambda_handler.boto3.client")
def test_lambda_handler_success(mock_boto_client, mock_read_csv, mock_write):
    mock_boto_client.return_value.get_object.return_value = {"Body": MagicMock()}
    mock_read_csv.return_value = {
        "SHIPMENT_DATE": ["2025-06-01", "2025-06-02"],
        "WEIGHT": [12.5, 9.1],
    }

    event = {
        "bucket": "test-bucket",
        "key": "carrier_data.csv",
        "table_name": "LMC_RAW_DATA",
    }

    result = lambda_handler(event, None)

    assert result["statusCode"] == 200
    assert "Data ingested successfully" in result["body"]
    assert mock_write.called


def test_validate_event_requires_bucket_key_and_table():
    from src.validators import validate_event

    try:
        validate_event({"bucket": "test-bucket"})
        assert False, "Expected ValueError"
    except ValueError:
        pass
