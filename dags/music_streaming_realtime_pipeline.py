# dags/music_streaming_realtime_pipeline.py
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import boto3
import json
import logging

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 24),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-engineering@company.com']
}

def check_validation_status(**context):
    """Check validation status and decide whether to continue or end DAG"""
    import boto3
    
    # Check validation results from S3
    s3_client = boto3.client('s3')
    batch_id = context['dag_run'].conf.get('batch_id', context['ds_nodash'] + '-' + context['ts_nodash'])
    
    try:
        # Read validation results from S3
        validation_key = f"validation_results/batch_id={batch_id}/validation_report.json"
        response = s3_client.get_object(Bucket='trial.data', Key=validation_key)
        validation_results = json.loads(response['Body'].read())
        
        overall_status = validation_results.get('overall_status', 'FAILED')
        logging.info(f"Validation status: {overall_status}")
        
        if overall_status == 'FAILED':
            logging.error("Validation failed - ending DAG")
            return 'validation_failed_end'
        else:
            logging.info("Validation passed - continuing to ETL and DynamoDB writing")
            return 'realtime_etl_processing'
            
    except Exception as e:
        logging.error(f"Error checking validation status: {str(e)}")
        return 'validation_failed_end'

def log_validation_failure(**context):
    """Log validation failure details"""
    logging.error("Data validation failed. Pipeline terminated.")
    # Could send notifications, update monitoring systems, etc.

def write_kpis_to_dynamodb(**context):
    """Dedicated task to write KPIs to DynamoDB after ETL processing"""
    import boto3
    from decimal import Decimal
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    s3_client = boto3.client('s3')
    
    batch_id = context['dag_run'].conf.get('batch_id', context['ds_nodash'] + '-' + context['ts_nodash'])
    processing_date = context['ds']
    
    try:
        # Read processed KPI results from S3 (saved by ETL job)
        kpi_results_key = f"processed_data/date={processing_date}/batch_id={batch_id}/kpi_results.json"
        
        try:
            response = s3_client.get_object(Bucket='trial.data', Key=kpi_results_key)
            kpi_data = json.loads(response['Body'].read())
        except:
            logging.warning("No KPI results file found, KPIs should have been written directly by ETL job")
            return
        
        table = dynamodb.Table('kpi.table')
        successful_writes = 0
        
        # Write genre metrics to DynamoDB
        for genre_metric in kpi_data.get('genre_metrics', []):
            try:
                # Generate partition key with hash prefix
                hash_prefix = str(hash(f"{genre_metric['genre']}{processing_date}") % 100).zfill(2)
                partition_key = f"{hash_prefix}#{genre_metric['genre']}#{processing_date}"
                
                # Write listen count (incremental)
                table.update_item(
                    Key={
                        'partition_key': partition_key,
                        'sort_key': f"listen_count#{processing_date}"
                    },
                    UpdateExpression='ADD #value :increment SET updated_at = :timestamp, batch_id = :batch, genre = :genre',
                    ExpressionAttributeNames={'#value': 'value'},
                    ExpressionAttributeValues={
                        ':increment': Decimal(str(genre_metric['listen_count'])),
                        ':timestamp': datetime.now().isoformat(),
                        ':batch': batch_id,
                        ':genre': genre_metric['genre']
                    }
                )
                
                # Write unique listeners (replace)
                table.put_item(
                    Item={
                        'partition_key': partition_key,
                        'sort_key': f"unique_listeners#{processing_date}",
                        'genre': genre_metric['genre'],
                        'date': processing_date,
                        'metric_type': 'unique_listeners',
                        'value': Decimal(str(genre_metric['unique_listeners'])),
                        'batch_id': batch_id,
                        'updated_at': datetime.now().isoformat()
                    }
                )
                
                successful_writes += 2
                
            except Exception as e:
                logging.error(f"Failed to write genre metric for {genre_metric['genre']}: {e}")
        
        # Write top songs
        for top_song in kpi_data.get('top_songs', []):
            try:
                hash_prefix = str(hash(f"{top_song['genre']}{processing_date}") % 100).zfill(2)
                partition_key = f"{hash_prefix}#{top_song['genre']}#{processing_date}"
                
                table.put_item(
                    Item={
                        'partition_key': partition_key,
                        'sort_key': f"top_song#{top_song['rank']}#{top_song['track_id']}",
                        'genre': top_song['genre'],
                        'date': processing_date,
                        'metric_type': 'top_song',
                        'track_id': top_song['track_id'],
                        'song_name': top_song['song_name'],
                        'play_count': Decimal(str(top_song['play_count'])),
                        'rank': top_song['rank'],
                        'batch_id': batch_id,
                        'updated_at': datetime.now().isoformat()
                    }
                )
                successful_writes += 1
                
            except Exception as e:
                logging.error(f"Failed to write top song: {e}")
        
        logging.info(f"Successfully wrote {successful_writes} items to DynamoDB")
        
    except Exception as e:
        logging.error(f"Error writing to DynamoDB: {str(e)}")
        raise

def archive_processed_files(**context):
    """Archive processed files after successful ETL and DynamoDB writing"""
    s3_client = boto3.client('s3')
    raw_bucket = 'rds.data'
    
    datasets = ['streams']  # Only archive stream files since we only wait for them
    archived_count = 0
    
    for dataset in datasets:
        try:
            response = s3_client.list_objects_v2(
                Bucket=raw_bucket,
                Prefix=f"{dataset}/"
            )
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    source_key = obj['Key']
                    if source_key.endswith('.csv'):
                        # Create archive path
                        archive_key = f"archive/{datetime.now().strftime('%Y/%m/%d')}/{source_key}"
                        
                        # Copy to archive
                        s3_client.copy_object(
                            Bucket=raw_bucket,
                            CopySource={'Bucket': raw_bucket, 'Key': source_key},
                            Key=archive_key
                        )
                        
                        archived_count += 1
                        logging.info(f"Archived {source_key} to {archive_key}")
                        
        except Exception as e:
            logging.error(f"Error archiving {dataset} files: {e}")
    
    logging.info(f"Archived {archived_count} files total")

# Create the DAG
with DAG(
    dag_id='music_streaming_realtime_pipeline',
    default_args=default_args,
    description='Real-time music streaming pipeline - waits only for stream files, writes to DynamoDB',
    schedule_interval=None,  # Event-driven
    max_active_runs=1,
    catchup=False,
    tags=['music-streaming', 'real-time', 'dynamodb']
) as dag:
    
    # Start task
    start = DummyOperator(task_id='start')
    
    # S3KeySensor ONLY for streaming files (REQUIRED)
    wait_for_streams = S3KeySensor(
        task_id='wait_for_streams_files',
        bucket_name='rds.data',
        bucket_key='streams/*.csv',
        wildcard_match=True,
        poke_interval=60,  # Check every 60 seconds
        timeout=1800,      # Wait up to 30 minutes
        aws_conn_id='aws_default',
        soft_fail=False    # Fail DAG if no streaming files
    )
    
    # Data validation using Python Shell Glue Job
    validate_data_quality = GlueJobOperator(
        task_id='validate_data_quality',
        job_name='music-streaming-validation',
        script_args={
            '--raw_data_bucket': 'rds.data',
            '--processed_bucket': 'trial.data',
            '--aws_region': 'us-east-1',
            '--batch_id': '{{ ds_nodash }}-{{ ts_nodash }}'
        },
        verbose=True,
        wait_for_completion=True
    )
    
    # Check validation results and branch
    check_validation = BranchPythonOperator(
        task_id='check_validation_status',
        python_callable=check_validation_status,
        provide_context=True
    )
    
    # ETL processing job (PySpark Glue Job) - processes data and computes KPIs
    realtime_etl_processing = GlueJobOperator(
        task_id='realtime_etl_processing',
        job_name='music-streaming-etl',
        script_args={
            '--raw_data_bucket': 'rds.data',
            '--processed_bucket': 'trial.data',
            '--quarantine_bucket': 'warehouse.bucket',
            '--dynamodb_table_name': 'kpi.table',
            '--aws_region': 'us-east-1',
            '--batch_id': '{{ ds_nodash }}-{{ ts_nodash }}',
            '--processing_date': '{{ ds }}'
        },
        verbose=True,
        wait_for_completion=True
    )
    
    # Dedicated DynamoDB writing task
    write_to_dynamodb = PythonOperator(
        task_id='write_kpis_to_dynamodb',
        python_callable=write_kpis_to_dynamodb
    )
    
    # Archive files after successful processing and DynamoDB writing
    archive_files = PythonOperator(
        task_id='archive_processed_files',
        python_callable=archive_processed_files
    )
    
    # Success end task
    success_end = DummyOperator(task_id='success_end')
    
    # Validation failure handling
    validation_failed_end = PythonOperator(
        task_id='validation_failed_end',
        python_callable=log_validation_failure
    )
    
    # Task dependencies - ONLY WAITS FOR STREAM FILES
    start >> wait_for_streams >> validate_data_quality >> check_validation
    
    # Branching paths
    check_validation >> realtime_etl_processing >> write_to_dynamodb >> archive_files >> success_end
    check_validation >> validation_failed_end

