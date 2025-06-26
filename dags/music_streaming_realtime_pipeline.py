from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import boto3
import json
import logging
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': Variable.get('dag_owner', default_var='data-engineering-team'),
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 24),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': Variable.get('notification_emails', default_var=['default@example.com'], deserialize_json=True)
}

def run_glue_job(job_name, **context):
    aws_region = Variable.get('aws_region', default_var='us-east-1')
    glue_client = boto3.client('glue', region_name=aws_region)

    batch_id = context['dag_run'].conf.get('batch_id', context['ds_nodash'] + '-' + context['ts_nodash'])
    logging.info(f"Using batch_id: {batch_id}")
    context['ti'].xcom_push(key='batch_id', value=batch_id)

    # Common job arguments
    base_job_args = {
        '--job-bookmark-option': 'job-bookmark-disable',
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true'
    }

    # Specific job arguments based on job_name
    if job_name == Variable.get('validation_job_name', default_var='music_streaming_validation'):
        job_args = {
            **base_job_args,
            '--raw_data_bucket': Variable.get('raw_data_bucket'),
            '--processed_bucket': Variable.get('processed_bucket'),
            '--aws_region': aws_region,
            '--batch_id': batch_id
        }
    elif job_name == Variable.get('etl_job_name', default_var='music_streaming_etl_complete'):
        job_args = {
            **base_job_args,
            '--raw_data_bucket': Variable.get('raw_data_bucket'),
            '--processed_bucket': Variable.get('processed_bucket'),
            '--quarantine_bucket': Variable.get('quarantine_bucket', default_var=f"{Variable.get('raw_data_bucket')}-quarantine"),
            '--dynamodb_table_name': Variable.get('dynamodb_table_name'),
            '--aws_region': aws_region,
            '--batch_id': batch_id,
            '--processing_date': context['ds'],
            '--enable-glue-datacatalog': 'true'
        }
    else:
        raise ValueError(f"Unknown job name: {job_name}")

    logging.info(f"Glue job arguments for {job_name}: {job_args}")

    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=job_args
        )

        job_run_id = response['JobRunId']
        logging.info(f"Started Glue job {job_name} with run ID: {job_run_id}")

        while True:
            job_run = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            state = job_run['JobRun']['JobRunState']
            logging.info(f"Job {job_name} status: {state}")

            if state in ['SUCCEEDED']:
                logging.info(f"Job {job_name} completed successfully")
                return job_run_id
            elif state in ['FAILED', 'ERROR', 'TIMEOUT']:
                error_message = job_run['JobRun'].get('ErrorMessage', 'Unknown error')
                raise Exception(f"Job {job_name} failed with state {state}: {error_message}")

            import time
            time.sleep(30)

    except Exception as e:
        logging.error(f"Error running Glue job {job_name}: {str(e)}")
        raise

def check_validation_status(**context):
    aws_region = Variable.get('aws_region', default_var='us-east-1')
    s3_client = boto3.client('s3', region_name=aws_region)
    processed_bucket = Variable.get('processed_bucket')
    batch_id = context['dag_run'].conf.get('batch_id', context['ds_nodash'] + '-' + context['ts_nodash'])
    logging.info(f"Checking validation for batch_id: {batch_id}")

    validation_paths = [
        f"validation_results/batch_id={batch_id}/validation_report.json",
        f"validation_results/{batch_id}/validation_report.json",
        f"validation/{batch_id}/validation_report.json",
        f"validation_results/validation_report_{batch_id}.json"
    ]

    validation_results = None
    validation_found = False

    for validation_key in validation_paths:
        try:
            logging.info(f"Checking validation results at: s3://{processed_bucket}/{validation_key}")
            response = s3_client.get_object(Bucket=processed_bucket, Key=validation_key)
            validation_results = json.loads(response['Body'].read())
            validation_found = True
            logging.info(f"Found validation results at: {validation_key}")
            break
        except s3_client.exceptions.NoSuchKey:
            logging.info(f"Validation results not found at: {validation_key}")
        except Exception as e:
            logging.error(f"Error reading validation results from {validation_key}: {str(e)}")

    if not validation_found:
        logging.error("No validation results found in any expected location")
        try:
            response = s3_client.list_objects_v2(Bucket=processed_bucket, Prefix='validation')
            if 'Contents' in response:
                logging.info("Available validation files:")
                for obj in response['Contents']:
                    logging.info(f"  - {obj['Key']}")
            else:
                logging.info("No validation files found in bucket")
        except Exception as e:
            logging.error(f"Error listing validation files: {str(e)}")
        return 'validation_failed_end'

    try:
        overall_status = validation_results.get('overall_status', 'FAILED')
        logging.info(f"Validation status: {overall_status}")

        if 'validation_summary' in validation_results:
            logging.info(f"Validation summary: {validation_results['validation_summary']}")

        if overall_status == 'FAILED':
            logging.error("Validation failed - ending DAG")
            if 'failed_checks' in validation_results:
                logging.error(f"Failed checks: {validation_results['failed_checks']}")
            return 'validation_failed_end'
        else:
            logging.info("Validation passed - continuing to ETL")
            return 'realtime_etl_processing'

    except Exception as e:
        logging.error(f"Error processing validation results: {str(e)}")
        return 'validation_failed_end'

def log_validation_failure(**context):
    logging.error("Data validation failed. Pipeline terminated.")
    batch_id = context['dag_run'].conf.get('batch_id', context['ds_nodash'] + '-' + context['ts_nodash'])
    logging.error(f"Failed batch_id: {batch_id}")

def run_dynamodb_processing(**context):
    """Run DynamoDB processing as a separate Glue job task"""
    aws_region = Variable.get('aws_region', default_var='us-east-1')
    glue_client = boto3.client('glue', region_name=aws_region)
    
    batch_id = context['dag_run'].conf.get('batch_id', context['ds_nodash'] + '-' + context['ts_nodash'])
    
    job_args = {
        '--raw_data_bucket': Variable.get('raw_data_bucket'),
        '--processed_bucket': Variable.get('processed_bucket'),
        '--dynamodb_table_name': Variable.get('dynamodb_table_name'),
        '--aws_region': aws_region,
        '--batch_id': batch_id,
        '--processing_date': context['ds'],
        '--task_type': 'dynamodb_only',
        '--job-bookmark-option': 'job-bookmark-disable',
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true'
    }
    
    logging.info(f"Starting DynamoDB processing task with batch_id: {batch_id}")
    
    try:
        response = glue_client.start_job_run(
            JobName=Variable.get('etl_job_name', default_var='music_streaming_etl_complete'),
            Arguments=job_args
        )
        
        job_run_id = response['JobRunId']
        logging.info(f"Started DynamoDB processing with run ID: {job_run_id}")
        
        while True:
            job_run = glue_client.get_job_run(JobName=Variable.get('etl_job_name', default_var='music_streaming_etl_complete'), RunId=job_run_id)
            state = job_run['JobRun']['JobRunState']
            logging.info(f"DynamoDB processing status: {state}")
            
            if state in ['SUCCEEDED']:
                logging.info("DynamoDB processing completed successfully")
                return job_run_id
            elif state in ['FAILED', 'ERROR', 'TIMEOUT']:
                error_message = job_run['JobRun'].get('ErrorMessage', 'Unknown error')
                raise Exception(f"DynamoDB processing failed with state {state}: {error_message}")
            
            import time
            time.sleep(30)
            
    except Exception as e:
        logging.error(f"Error in DynamoDB processing: {str(e)}")
        raise

def run_archiving_task(**context):
    """Run file archiving as a separate Glue job task"""
    aws_region = Variable.get('aws_region', default_var='us-east-1')
    glue_client = boto3.client('glue', region_name=aws_region)
    
    batch_id = context['dag_run'].conf.get('batch_id', context['ds_nodash'] + '-' + context['ts_nodash'])
    
    job_args = {
        '--raw_data_bucket': Variable.get('raw_data_bucket'),
        '--archive_bucket': Variable.get('archive_bucket', default_var=f"{Variable.get('raw_data_bucket')}-archive"),
        '--aws_region': aws_region,
        '--batch_id': batch_id,
        '--processing_date': context['ds'],
        '--task_type': 'archiving_only',
        '--job-bookmark-option': 'job-bookmark-disable',
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true'
    }
    
    logging.info(f"Starting archiving task with batch_id: {batch_id}")
    
    try:
        response = glue_client.start_job_run(
            JobName=Variable.get('etl_job_name', default_var='music_streaming_etl_complete'),
            Arguments=job_args
        )
        
        job_run_id = response['JobRunId']
        logging.info(f"Started archiving task with run ID: {job_run_id}")
        
        while True:
            job_run = glue_client.get_job_run(JobName=Variable.get('etl_job_name', default_var='music_streaming_etl_complete'), RunId=job_run_id)
            state = job_run['JobRun']['JobRunState']
            logging.info(f"Archiving task status: {state}")
            
            if state in ['SUCCEEDED']:
                logging.info("Archiving task completed successfully")
                return job_run_id
            elif state in ['FAILED', 'ERROR', 'TIMEOUT']:
                error_message = job_run['JobRun'].get('ErrorMessage', 'Unknown error')
                raise Exception(f"Archiving task failed with state {state}: {error_message}")
            
            import time
            time.sleep(30)
            
    except Exception as e:
        logging.error(f"Error in archiving task: {str(e)}")
        raise

with DAG(
    dag_id=Variable.get('dag_id', default_var='music_streaming_realtime_pipeline'),
    default_args=default_args,
    description='Real-time music streaming pipeline with validation, DynamoDB, and archiving tasks',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['music-streaming', 'real-time', 'dynamodb', 'archiving']
) as dag:

    start = DummyOperator(task_id='start')

    validate_data_quality = PythonOperator(
        task_id='validate_data_quality',
        python_callable=lambda **context: run_glue_job(Variable.get('validation_job_name', default_var='music_streaming_validation'), **context)
    )

    check_validation = BranchPythonOperator(
        task_id='check_validation_status',
        python_callable=check_validation_status,
        provide_context=True
    )

    realtime_etl_processing = PythonOperator(
        task_id='realtime_etl_processing',
        python_callable=lambda **context: run_glue_job(Variable.get('etl_job_name', default_var='music_streaming_etl_complete'), **context)
    )

    write_to_dynamodb = PythonOperator(
        task_id='write_to_dynamodb',
        python_callable=run_dynamodb_processing
    )

    archive_files = PythonOperator(
        task_id='archive_processed_files',
        python_callable=run_archiving_task
    )

    success_end = DummyOperator(task_id='success_end')

    validation_failed_end = PythonOperator(
        task_id='validation_failed_end',
        python_callable=log_validation_failure
    )

    # Task dependencies
    start >> validate_data_quality >> check_validation
    check_validation >> realtime_etl_processing >> write_to_dynamodb >> archive_files >> success_end
    check_validation >> validation_failed_end