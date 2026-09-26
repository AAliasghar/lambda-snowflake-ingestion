def validate_event(event: dict) -> dict:
    """Validate Lambda event payload before processing the file."""
    if not isinstance(event, dict):
        raise ValueError("Event payload must be a dictionary")

    bucket = event.get("bucket") or event.get("Bucket")
    key = event.get("key") or event.get("Key")
    table_name = event.get("table_name") or event.get("table") or event.get("TABLE_NAME")

    if not bucket:
        raise ValueError("Missing S3 bucket name in event payload")
    if not key:
        raise ValueError("Missing S3 object key in event payload")
    if not table_name:
        raise ValueError("Missing target Snowflake table name in event payload")

    return {
        "bucket": bucket,
        "key": key,
        "table_name": table_name,
    }
