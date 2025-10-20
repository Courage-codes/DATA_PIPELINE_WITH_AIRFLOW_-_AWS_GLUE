# Music Streaming Data Pipeline Documentation

## Overview

A real-time music streaming data pipeline that validates, processes and  computes key performance indicators (KPIs), and archives processed files. The pipeline is triggered automatically by an AWS Lambda function when new files are uploaded to an S3 bucket. It consists of two AWS Glue jobs (`music_streaming_validation` and `music_streaming_etl_complete`), orchestrated by an Apache Airflow DAG (`music_streaming_realtime_pipeline`), and initiated by a Lambda function (`trigger_dag_lambda`). The system ensures data quality through validation, processes data using Spark, stores results in S3 and DynamoDB, and archives processed files.

## Architecture
![Architecture Diagram](./images/architecture_diagram.svg)

The pipeline follows a modular architecture with four main components:

1. **Trigger (AWS Lambda:** `trigger_dag_lambda`**)**

   - Monitors an S3 bucket for new CSV files.
   - Triggers the Airflow DAG with a unique `batch_id` derived from the S3 event.

2. **Data Validation (AWS Glue Job:** `music_streaming_validation`**)**

   - Validates raw CSV files in S3 for required columns.
   - Writes validation results to S3 as a JSON report.

3. **ETL Processing (AWS Glue Job:** `music_streaming_etl_complete`**)**

   - Processes raw streaming, song, and user data from S3.
   - Performs joins, computes daily KPIs, and writes results to DynamoDB and s3.
   - Archives processed files to a separate S3 bucket.
   - Supports task-based execution (`full_etl`, `dynamodb_only`, `archiving_only`).

4. **Orchestration (Airflow DAG:** `music_streaming_realtime_pipeline`**)**

   - Orchestrates the validation and ETL jobs.
   - Checks validation results and branches to either continue processing or terminate on failure.
   - Triggers DynamoDB writes and file archiving as separate tasks.
   - Uses Airflow Variables for configuration.

## Components

### 1. Trigger (`trigger_dag_lambda.py`)

**Purpose**: Automatically triggers the `music_streaming_realtime_pipeline` DAG when new CSV files are uploaded to the raw data S3 bucket.

**Input**:

- S3 event notification (e.g., `s3:ObjectCreated:*`) from the raw data bucket.
- Example event:

  ```json
  {
      "Records": [
          {
              "s3": {
                  "bucket": {"name": "<raw_data_bucket>"},
                  "object": {"key": "streams/data.csv"}
              }
          }
      ]
  }
  ```

**Output**:

- Triggers the Airflow DAG with a `batch_id` in the run configuration (e.g., `{"batch_id": "batch-<bucket>-streams-data.csv-<timestamp>"}`).
- Logs success or failure to CloudWatch Logs.

**Environment Variables**:

| Variable | Description | Example Value | Required |
| --- | --- | --- | --- |
| `MWAA_ENVIRONMENT_NAME` | Name of the MWAA environment | `MyAirflowEnvironment` | Yes |
| `DAG_ID` | ID of the Airflow DAG | `music_streaming_realtime_pipeline` | Yes |
| `AWS_REGION` | AWS region for MWAA | `us-west-2` | No (defaults to `us-east-1`) |

**Execution**:

- Generates a unique `batch_id` based on the S3 bucket, file key, and timestamp.
- Uses the MWAA CLI to trigger the DAG via a POST request to the `/aws_mwaa/cli` endpoint.
- Handles errors (e.g., invalid CLI token, permissions issues) and logs detailed responses.

**IAM Permissions**:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "airflow:CreateCliToken",
            "Resource": "arn:aws:airflow:<region>:<account-id>:environment/<mwaa-environment-name>"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
```

### 2. Data Validation (`music_streaming_validation.py`)

**Purpose**: Validates the schema of raw CSV files (`streams`, `songs`, `users`) in S3 to ensure required columns are present.

**Input**:

- Raw CSV files in `s3://{raw_data_bucket}/{dataset_type}/` (where `dataset_type` is `streams`, `songs`, or `users`).
- Required columns:
  - `streams`: `user_id`, `track_id`, `listen_time`
  - `songs`: `track_id`, `track_genre`, `track_name`, `artists`
  - `users`: `user_id`, `user_name`, `user_age`, `user_country`

**Output**:

- Validation report in JSON format at `s3://{processed_bucket}/validation_results/batch_id={batch_id}/validation_report.json`.
- CloudWatch metrics in the `DataValidation/Metrics` namespace.

**Parameters**:

| Parameter | Description | Required |  Default Value |
| --- | --- | --- | --- |
| `raw_data_bucket` | S3 bucket for raw CSV files | Yes | None |
| `processed_bucket` | S3 bucket for validation results | Yes | None |
| `aws_region` | AWS region for S3 and CloudWatch | No | `us-east-1` |
| `batch_id` | Unique identifier for the batch | No | `batch-YYYYMMDD-HHMMSS` (timestamp-based) |

**Execution**:

- Reads CSV files using Pandas.
- Checks for required columns.
- Saves validation results to S3.
- Publishes a `ValidationSuccess` metric to CloudWatch (1 for success, 0 for failure).
- Exits with a non-zero status code if validation fails.

### 3. ETL Processing (`etl_streaming_pipeline_complete.py`)

**Purpose**: Processes raw data, computes KPIs, stores results, and archives files. Supports three task types: `full_etl`, `dynamodb_only`, and `archiving_only`.

**Input**:

- Raw CSV files in `s3://{raw_data_bucket}/{dataset_type}/` (same datasets as validation).
- For `dynamodb_only`, reads Parquet files from `s3://{processed_bucket}/processed_data/date={processing_date}/batch_id={batch_id}/`.

**Output**:

- Processed data in Parquet format at `s3://{processed_bucket}/processed_data/date={processing_date}/batch_id={batch_id}/`, partitioned by `genre`.
- KPIs in DynamoDB table `{dynamodb_table_name}` and file tracking in `{dynamodb_table_name}_file_tracker`.
- Archived files in `s3://{archive_bucket}/archive/YYYY/MM/DD/{file_key}`.
- CloudWatch metrics in the `ETL/Metrics` namespace.

**Parameters**:

| Parameter | Description | Required | Default Value |
| --- | --- | --- | --- |
| `JOB_NAME` | Name of the Glue job | Yes | None |
| `batch_id` | Unique identifier for the batch | Yes | None |
| `processing_date` | Date for data processing (YYYY-MM-DD) | Yes | None |
| `raw_data_bucket` | S3 bucket for raw CSV files | Yes | None |
| `processed_bucket` | S3 bucket for processed Parquet files | Yes | None |
| `dynamodb_table_name` | DynamoDB table for KPIs and file tracking | Yes | None |


### 4. Airflow DAG (`music_streaming_realtime_pipeline.py`)

**Purpose**: Orchestrates the validation and ETL jobs, handling branching logic based on validation results.

**Tasks**:

1. `start`: Dummy task to initiate the pipeline.
2. `validate_data_quality`: Runs the `music_streaming_validation` Glue job.
3. `check_validation_status`: Checks validation results in S3 and branches to either `realtime_etl_processing` or `validation_failed_end`.
4. `realtime_etl_processing`: Runs the `music_streaming_etl_complete` Glue job with `full_etl` task type.
5. `write_to_dynamodb`: Runs the `music_streaming_etl_complete` Glue job with `dynamodb_only` task type.
6. `archive_processed_files`: Runs the `music_streaming_etl_complete` Glue job with `archiving_only` task type.
7. `success_end`: Dummy task for successful completion.
8. `validation_failed_end`: Logs validation failure and terminates the pipeline.

**Airflow Variables**:

| Variable Name | Description | Example Value |
| --- | --- | --- |
| `dag_owner` | Owner of the DAG | `data-engineering-team` |
| `notification_emails` | List of emails for notifications (JSON) | `["your.email@example.com"]` |
| `aws_region` | AWS region for Glue and S3 | `us-west-2` |
| `raw_data_bucket` | S3 bucket for raw data | `my-raw-data-bucket` |
| `processed_bucket` | S3 bucket for processed data | `my-processed-data-bucket` |
| `quarantine_bucket` | S3 bucket for quarantined files (optional) | `my-raw-data-bucket-quarantine` |
| `archive_bucket` | S3 bucket for archived files (optional) | `my-raw-data-bucket-archive` |
| `dynamodb_table_name` | DynamoDB table for KPIs | `music-streaming-kpis` |
| `validation_job_name` | Glue job name for validation | `music_streaming_validation` |
| `etl_job_name` | Glue job name for ETL | `music_streaming_etl_complete` |
| `dag_id` | DAG ID | `music_streaming_realtime_pipeline` |

**DAG Configuration**:

- **Schedule**: `None` (triggered by Lambda).
- **Max Active Runs**: 1.
- **Catchup**: Disabled.
- **Default Args**:
  - `retries`: 2
  - `retry_delay`: 5 minutes
  - `email_on_failure`: True

**Dynamic Parameters**:

- `batch_id`: Passed by the Lambda function (e.g., `batch-<bucket>-<key>-<timestamp>`); defaults to `{ds_nodash}-{ts_nodash}` if not provided.

## Setup Instructions

### Prerequisites

- **AWS Account** with:
  - S3 buckets for raw data, processed data, and archiving.
  - DynamoDB tables for KPIs and file tracking (created automatically if not present).
  - IAM roles for Lambda, Glue, and MWAA with appropriate permissions.
  - Managed Workflows for Apache Airflow (MWAA) environment.
- **Airflow Environment** with:
  - `airflow-provider-amazon` package installed.
  - Access to AWS Glue and S3 via IAM credentials or role.
  - Airflow Variables configured.

### Configuration

1. **Set Lambda Environment Variables**:

   ```bash
   aws lambda update-function-configuration \
       --function-name trigger_dag_lambda \
       --environment '{
           "Variables": {
               "MWAA_ENVIRONMENT_NAME": "MyAirflowEnvironment",
               "DAG_ID": "music_streaming_realtime_pipeline",
               "AWS_REGION": "us-west-2"
           }
       }'
   ```

2. **Configure S3 Event Notification**:

   ```bash
   aws s3api put-bucket-notification-configuration \
       --bucket my-raw-data-bucket \
       --notification-configuration '{
           "LambdaFunctionConfigurations": [{
               "LambdaFunctionArn": "arn:aws:lambda:us-west-2:<account-id>:function:trigger_dag_lambda",
               "Events": ["s3:ObjectCreated:*"],
               "Filter": {
                   "Key": {
                       "FilterRules": [
                           {"Name": "prefix", "Value": "streams/"},
                           {"Name": "prefix", "Value": "songs/"},
                           {"Name": "prefix", "Value": "users/"}
                       ]
                   }
               }
           }]
       }'
   ```

3. **Set Airflow Variables**:

   ```bash
   airflow variables set raw_data_bucket my-raw-data-bucket
   airflow variables set processed_bucket my-processed-data-bucket
   airflow variables set dynamodb_table_name music-streaming-kpis
   airflow variables set aws_region us-west-2
   airflow variables set validation_job_name music_streaming_validation
   airflow variables set etl_job_name music_streaming_etl_complete
   airflow variables set dag_owner data-engineering-team
   airflow variables set notification_emails '["your.email@example.com"]'
   airflow variables set dag_id music_streaming_realtime_pipeline
   ```

4. **Deploy Glue Scripts**:

   - Upload `music_streaming_validation.py` and `etl_job.py` to an S3 bucket accessible by Glue.
   - Create Glue jobs with names matching `validation_job_name` and `etl_job_name`.

5. **Deploy Lambda Function**:

   - Upload `trigger_dag_lambda.py` to AWS Lambda.
   - Configure the IAM role with `airflow:CreateCliToken` and CloudWatch Logs permissions.
   - Set the function timeout to at least 30 seconds.

6. **Deploy Airflow DAG**:

   - Place `music_streaming_realtime_pipeline.py` in the Airflow DAGs folder.
   - Ensure MWAA has the necessary AWS credentials or IAM role.

### Running the Pipeline

1. **Automatic Trigger**:

   - Upload a CSV file to `s3://<raw_data_bucket>/streams/`, `songs/`, or `users/`.
   - The Lambda function triggers the DAG with a unique `batch_id`.

2. **Manual Trigger (Optional)**:

   - Via Airflow UI: Trigger the DAG with an optional `batch_id`:

     ```json
     {"batch_id": "batch-20250626-0531"}
     ```
   - Via Airflow CLI:

     ```bash
     airflow dags trigger -c '{"batch_id": "batch-20250626-0531"}' music_streaming_realtime_pipeline
     ```

3. **Monitor Execution**:

   - Check Lambda logs in CloudWatch for trigger status.
   - Check Airflow logs for task status.
   - Monitor Glue job runs in the AWS Glue console.
   - Verify validation results in `s3://{processed_bucket}/validation_results/batch_id={batch_id}/`.
   - Check processed data in `s3://{processed_bucket}/processed_data/date={processing_date}/batch_id={batch_id}/`.
   - Verify KPIs in DynamoDB table `{dynamodb_table_name}`.
   - Check archived files in `s3://{archive_bucket}/archive/YYYY/MM/DD/`.

## Error Handling

- **Lambda Failure**: Errors (e.g., missing environment variables, MWAA CLI token issues) are logged to CloudWatch with the `batch_id`.
- **Validation Failure**: If the validation job fails (missing columns or unreadable files), the DAG terminates at `validation_failed_end` and logs the failure.
- **ETL Failure**: Glue job failures (e.g., data issues, AWS service errors) are logged, and metrics are published to CloudWatch (`JobFailures`, `FailedFiles`).
- **Airflow Retries**: Tasks retry up to 2 times with a 5-minute delay on failure.
- **Notifications**: Email notifications are sent to `notification_emails` on task failures.

## Monitoring

- **CloudWatch Metrics**:
  - Lambda: Monitor invocation errors and duration.
  - Validation: `DataValidation/Metrics` (e.g., `ValidationSuccess`).
  - ETL: `ETL/Metrics` (e.g., `ProcessedRecords`, `DynamoDBWrites`, `ArchivedFiles`).
- **Logs**:
  - Lambda logs in CloudWatch Logs.
  - Glue job logs in CloudWatch Logs (enabled via `--enable-continuous-cloudwatch-log`).
  - Airflow task logs in the Airflow UI or MWAA CloudWatch Logs.

## Maintenance

- **Update Parameters**: Modify Lambda environment variables or Airflow Variables for new environments.
- **Schema Changes**: Update `required_columns` in `music_streaming_validation.py` or schema expectations in `etl_job.py` if input schemas change.
- **Performance Tuning**: Adjust `max_records_per_partition` in the ETL job or Lambda timeout for large datasets.
- **Cleanup**: Monitor `archive_bucket` and DynamoDB tables for storage management.

## Troubleshooting

- **Lambda Fails**: Check CloudWatch logs for missing environment variables or MWAA permission issues.
- **S3 Trigger Fails**: Verify S3 event notification configuration and Lambda permissions.
- **Validation Fails**: Check S3 validation report for missing columns or file errors.
- **ETL Fails**: Review Glue job logs for specific errors (e.g., schema mismatches, S3 access issues).
- **DAG Fails**: Check Airflow logs and verify Airflow Variables.
- **Missing Data**: Ensure raw CSV files are in the correct S3 paths (`streams/`, `songs/`, `users/`).
