# Lambda + Snowflake ingestion prototype

This repository is intended to support a logistics data ingestion workflow using AWS Lambda and Snowflake.

Current status:
- The project includes a local ETL prototype for SQL Server testing and validation.
- The production-facing path is designed for S3-triggered CSV ingestion into Snowflake.
- The repository is still evolving and should be treated as a working prototype rather than a fully deployed production system.

## What is included

- `src/lambda_handler.py` - AWS Lambda entry point
- `src/snowflake_connector.py` - Snowflake load logic
- `src/transformations.py` - column normalization and dataframe cleanup
- `src/validators.py` - event validation
- `src/logging_config.py` - logging setup
- `src/ingest_to_sql_server.py` - legacy/local SQL Server helper for local testing
- `sql/setup_snowflake.sql` - basic Snowflake table creation script
- `tests/test_lambda_handler.py` - basic validation tests

## Architecture

- Source data is expected in S3
- Lambda reads the file and validates the event payload
- Data is cleaned and normalized
- Data is loaded into Snowflake using the Snowflake connector
- Credentials can come from environment variables or AWS Secrets Manager

## Local setup

1. Create a virtual environment
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy the environment template:

```bash
cp .env.example .env
```

4. Edit `.env` with your Snowflake/AWS settings.

## Sample Lambda event

```json
{
  "bucket": "example-bucket",
  "key": "carrier_data.csv",
  "table_name": "LMC_RAW_DATA"
}
```

## Limitation

This repository is not yet a fully production-hardened deployment. It is meant to provide a clean starting point for event-driven ingestion into Snowflake with validation and modular code structure.

## Planned next steps

- add stronger validation rules
- add staging and archive patterns for S3 objects
- add automated CI checks
- add deployment infrastructure for Lambda and IAM
- add more comprehensive Snowflake schema definitions
