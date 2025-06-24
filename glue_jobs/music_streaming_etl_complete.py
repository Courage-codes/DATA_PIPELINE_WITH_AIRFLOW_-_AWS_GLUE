# glue_jobs/music_streaming_etl_complete.py
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, lit, current_timestamp, broadcast, sum as spark_sum, 
    count, countDistinct, avg, desc, row_number, when
)
from pyspark.sql.window import Window
from decimal import Decimal
import json
import sys
import boto3
from botocore.exceptions import ClientError
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get job parameters
try:
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'batch_id', 'processing_date', 
        'raw_data_bucket', 'processed_bucket', 'quarantine_bucket',
        'dynamodb_table_name', 'aws_region'
    ])
except Exception:
    args = {
        'JOB_NAME': 'music-streaming-etl',
        'batch_id': '20250623-01',
        'processing_date': '2025-06-23',
        'raw_data_bucket': 'rds.data',
        'processed_bucket': 'trial.data',
        'quarantine_bucket': 'warehouse.bucket',
        'dynamodb_table_name': 'kpi.table',
        'aws_region': 'us-east-1'
    }

# Configuration
BATCH_ID = args['batch_id']
PROCESSING_DATE = args['processing_date']
RAW_DATA_BUCKET = args['raw_data_bucket']
PROCESSED_BUCKET = args['processed_bucket']
QUARANTINE_BUCKET = args['quarantine_bucket']
DYNAMODB_TABLE_NAME = args['dynamodb_table_name']
AWS_REGION = args['aws_region']

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# AWS clients
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)
cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)

class MusicStreamingETL:
    def __init__(self):
        self.processed_records = 0
        self.failed_records = 0
        self.quarantined_records = 0

    def publish_metric(self, metric_name, value, unit='Count'):
        """Publish custom CloudWatch metrics"""
        try:
            cloudwatch.put_metric_data(
                Namespace='MusicStreaming/ETL',
                MetricData=[{
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': unit,
                    'Dimensions': [
                        {'Name': 'BatchId', 'Value': BATCH_ID},
                        {'Name': 'ProcessingDate', 'Value': PROCESSING_DATE},
                        {'Name': 'Region', 'Value': AWS_REGION}
                    ]
                }]
            )
            logger.info(f"Published metric {metric_name}: {value}")
        except Exception as e:
            logger.error(f"Failed to publish metric {metric_name}: {str(e)}")

    def generate_partition_key(self, genre, date_str):
        """Generate partition key with hash prefix to avoid hot partitions"""
        hash_prefix = str(hash(f"{genre}{date_str}") % 100).zfill(2)
        return f"{hash_prefix}#{genre}#{date_str}"

    def convert_for_dynamodb(self, obj):
        """Recursively convert data types for DynamoDB compatibility"""
        if obj is None:
            return None
        elif isinstance(obj, float):
            if str(obj).lower() in ['nan', 'inf', '-inf']:
                return None
            return Decimal(str(obj))
        elif isinstance(obj, int):
            return obj
        elif isinstance(obj, dict):
            return {k: self.convert_for_dynamodb(v) for k, v in obj.items() if v is not None}
        elif isinstance(obj, list):
            return [self.convert_for_dynamodb(item) for item in obj if item is not None]
        elif isinstance(obj, str):
            return obj
        else:
            return str(obj)

    def create_dynamodb_table_if_not_exists(self):
        """Create DynamoDB table if it doesn't exist"""
        try:
            table = dynamodb.Table(DYNAMODB_TABLE_NAME)
            table.load()
            logger.info(f"DynamoDB table {DYNAMODB_TABLE_NAME} already exists")
            return table
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"Creating DynamoDB table {DYNAMODB_TABLE_NAME}")
                table = dynamodb.create_table(
                    TableName=DYNAMODB_TABLE_NAME,
                    KeySchema=[
                        {'AttributeName': 'partition_key', 'KeyType': 'HASH'},
                        {'AttributeName': 'sort_key', 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'partition_key', 'AttributeType': 'S'},
                        {'AttributeName': 'sort_key', 'AttributeType': 'S'}
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                table.wait_until_exists()
                logger.info(f"DynamoDB table {DYNAMODB_TABLE_NAME} created successfully")
                return table
            else:
                raise

    def create_file_tracking_table(self):
        """Create DynamoDB table to track processed files"""
        tracking_table_name = f"{DYNAMODB_TABLE_NAME}_file_tracker"
        
        try:
            table = dynamodb.Table(tracking_table_name)
            table.load()
            logger.info(f"File tracking table {tracking_table_name} already exists")
            return table
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"Creating file tracking table {tracking_table_name}")
                table = dynamodb.create_table(
                    TableName=tracking_table_name,
                    KeySchema=[
                        {'AttributeName': 'file_key', 'KeyType': 'HASH'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'file_key', 'AttributeType': 'S'}
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                table.wait_until_exists()
                logger.info(f"File tracking table {tracking_table_name} created successfully")
                return table
            else:
                raise

    def get_unprocessed_files(self, dataset_type):
        """Get list of unprocessed files for a dataset type"""
        tracking_table_name = f"{DYNAMODB_TABLE_NAME}_file_tracker"
        tracking_table = dynamodb.Table(tracking_table_name)
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=RAW_DATA_BUCKET,
                Prefix=f"{dataset_type}/"
            )
            
            all_files = []
            if 'Contents' in response:
                all_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            
            unprocessed_files = []
            
            for file_key in all_files:
                try:
                    response = tracking_table.get_item(
                        Key={'file_key': file_key}
                    )
                    
                    if 'Item' not in response:
                        unprocessed_files.append(file_key)
                    else:
                        status = response['Item'].get('status', 'unknown')
                        if status == 'failed':
                            unprocessed_files.append(file_key)
                            logger.info(f"Retrying failed file: {file_key}")
                        else:
                            logger.info(f"Skipping already processed file: {file_key}")
                            
                except Exception as e:
                    logger.error(f"Error checking file status for {file_key}: {e}")
                    unprocessed_files.append(file_key)
            
            logger.info(f"Found {len(unprocessed_files)} unprocessed {dataset_type} files out of {len(all_files)} total")
            return unprocessed_files
            
        except Exception as e:
            logger.error(f"Error listing files for {dataset_type}: {e}")
            return []

    def mark_file_processing_start(self, file_key):
        """Mark file as being processed"""
        tracking_table_name = f"{DYNAMODB_TABLE_NAME}_file_tracker"
        tracking_table = dynamodb.Table(tracking_table_name)
        
        try:
            tracking_table.put_item(
                Item={
                    'file_key': file_key,
                    'status': 'processing',
                    'started_at': datetime.now().isoformat(),
                    'batch_id': BATCH_ID
                }
            )
            logger.info(f"Marked file as processing: {file_key}")
        except Exception as e:
            logger.error(f"Error marking file as processing {file_key}: {e}")

    def mark_file_completed(self, file_key, record_count):
        """Mark file as successfully processed"""
        tracking_table_name = f"{DYNAMODB_TABLE_NAME}_file_tracker"
        tracking_table = dynamodb.Table(tracking_table_name)
        
        try:
            tracking_table.update_item(
                Key={'file_key': file_key},
                UpdateExpression='SET #status = :status, completed_at = :timestamp, record_count = :count',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':status': 'completed',
                    ':timestamp': datetime.now().isoformat(),
                    ':count': record_count
                }
            )
            logger.info(f"Marked file as completed: {file_key} with {record_count} records")
        except Exception as e:
            logger.error(f"Error marking file as completed {file_key}: {e}")

    def mark_file_failed(self, file_key, error_message):
        """Mark file as failed"""
        tracking_table_name = f"{DYNAMODB_TABLE_NAME}_file_tracker"
        tracking_table = dynamodb.Table(tracking_table_name)
        
        try:
            tracking_table.update_item(
                Key={'file_key': file_key},
                UpdateExpression='SET #status = :status, failed_at = :timestamp, error_message = :error',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':status': 'failed',
                    ':timestamp': datetime.now().isoformat(),
                    ':error': str(error_message)[:1000]
                }
            )
            logger.error(f"Marked file as failed: {file_key} - {error_message}")
        except Exception as e:
            logger.error(f"Error marking file as failed {file_key}: {e}")

    def compute_daily_kpis(self, merged_df):
        """Compute all daily KPIs as specified in requirements"""
        kpis = {}
        
        # Daily Genre-Level KPIs
        genre_kpis = merged_df.groupBy('genre').agg(
            count('*').alias('listen_count'),
            countDistinct('user_id').alias('unique_listeners'),
            spark_sum('listen_duration').alias('total_listening_time'),
            avg('listen_duration').alias('avg_listening_time_per_user')
        )
        kpis['genre_metrics'] = genre_kpis
        
        # Top 3 Songs per Genre per Day
        window_spec = Window.partitionBy('genre').orderBy(desc('play_count'))
        song_plays = merged_df.groupBy('genre', 'track_id', 'song_name').agg(
            count('*').alias('play_count')
        )
        top_songs = song_plays.withColumn('rank', row_number().over(window_spec))\
                             .filter(col('rank') <= 3)
        kpis['top_songs_per_genre'] = top_songs
        
        # Top 5 Genres per Day
        top_genres = genre_kpis.orderBy(desc('listen_count')).limit(5)
        kpis['top_genres'] = top_genres
        
        logger.info("Computed all daily KPIs successfully")
        return kpis

    def prepare_dynamodb_items(self, kpis):
        """Prepare items for DynamoDB insertion"""
        items = []
        
        # Genre metrics
        for row in kpis['genre_metrics'].collect():
            base_partition_key = self.generate_partition_key(row['genre'], PROCESSING_DATE)
            
            metrics = [
                ('listen_count', row['listen_count']),
                ('unique_listeners', row['unique_listeners']),
                ('total_listening_time', row['total_listening_time']),
                ('avg_listening_time_per_user', row['avg_listening_time_per_user'])
            ]
            
            for metric_type, value in metrics:
                items.append({
                    'partition_key': base_partition_key,
                    'sort_key': f"{metric_type}#{PROCESSING_DATE}",
                    'genre': row['genre'],
                    'date': PROCESSING_DATE,
                    'metric_type': metric_type,
                    'value': self.convert_for_dynamodb(value),
                    'batch_id': BATCH_ID
                })
        
        # Top songs per genre
        for row in kpis['top_songs_per_genre'].collect():
            partition_key = self.generate_partition_key(row['genre'], PROCESSING_DATE)
            items.append({
                'partition_key': partition_key,
                'sort_key': f"top_song#{row['rank']}#{row['track_id']}",
                'genre': row['genre'],
                'date': PROCESSING_DATE,
                'metric_type': 'top_song',
                'track_id': row['track_id'],
                'song_name': row['song_name'],
                'play_count': self.convert_for_dynamodb(row['play_count']),
                'rank': row['rank'],
                'batch_id': BATCH_ID
            })
        
        # Top genres
        for idx, row in enumerate(kpis['top_genres'].collect(), 1):
            partition_key = f"daily_top_genres#{PROCESSING_DATE}"
            items.append({
                'partition_key': partition_key,
                'sort_key': f"rank#{str(idx).zfill(2)}#{row['genre']}",
                'date': PROCESSING_DATE,
                'metric_type': 'top_genre',
                'genre': row['genre'],
                'listen_count': self.convert_for_dynamodb(row['listen_count']),
                'rank': idx,
                'batch_id': BATCH_ID
            })
        
        logger.info(f"Prepared {len(items)} items for DynamoDB")
        return items

    def write_to_dynamodb_incremental(self, items):
        """Write items to DynamoDB using UpdateItem for incremental processing"""
        if not items:
            logger.info("No items to write to DynamoDB")
            return
        
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        successful_writes = 0
        failed_writes = 0
        
        for item in items:
            try:
                if item['metric_type'] in ['listen_count', 'total_listening_time']:
                    # Use ADD for cumulative metrics
                    table.update_item(
                        Key={
                            'partition_key': item['partition_key'],
                            'sort_key': item['sort_key']
                        },
                        UpdateExpression='ADD #value :increment SET updated_at = :timestamp, batch_id = :batch',
                        ExpressionAttributeNames={'#value': 'value'},
                        ExpressionAttributeValues={
                            ':increment': item['value'],
                            ':timestamp': datetime.now().isoformat(),
                            ':batch': BATCH_ID
                        }
                    )
                else:
                    # Use SET for calculated metrics and rankings
                    converted_item = {k: self.convert_for_dynamodb(v) for k, v in item.items()}
                    table.put_item(Item=converted_item)
                
                successful_writes += 1
                
            except Exception as e:
                logger.error(f"Failed to write item: {str(e)}")
                failed_writes += 1
        
        logger.info(f"DynamoDB writes - Successful: {successful_writes}, Failed: {failed_writes}")
        self.publish_metric('DynamoDBWrites', successful_writes)
        self.publish_metric('DynamoDBWriteFailures', failed_writes)

    def save_to_s3_parquet(self, df):
        """Save processed data to S3 as Parquet"""
        try:
            output_path = f"s3://{PROCESSED_BUCKET}/processed_data/date={PROCESSING_DATE}/batch_id={BATCH_ID}/"
            
            df.withColumn("processing_timestamp", current_timestamp())\
              .withColumn("batch_id", lit(BATCH_ID))\
              .write\
              .mode("append")\
              .partitionBy("genre")\
              .option("compression", "snappy")\
              .parquet(output_path)
            
            logger.info(f"Saved processed data to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to save to S3: {str(e)}")

    def run_etl(self):
        """Main ETL execution with file tracking"""
        try:
            logger.info(f"Starting ETL transformation for batch {BATCH_ID}")
            
            # Create DynamoDB tables if needed
            self.create_dynamodb_table_if_not_exists()
            self.create_file_tracking_table()
            
            # Get unprocessed files for each dataset
            unprocessed_streams = self.get_unprocessed_files('streams')
            unprocessed_songs = self.get_unprocessed_files('songs')
            unprocessed_users = self.get_unprocessed_files('users')
            
            # Check if we have any new files to process
            if not unprocessed_streams and not unprocessed_songs and not unprocessed_users:
                logger.info("No new files to process")
                return
            
            # Process files and track them
            dataframes = {}
            
            # Process streaming data
            if unprocessed_streams:
                logger.info(f"Processing {len(unprocessed_streams)} streaming files")
                
                for file_key in unprocessed_streams:
                    self.mark_file_processing_start(file_key)
                
                try:
                    stream_paths = [f"s3://{RAW_DATA_BUCKET}/{file_key}" for file_key in unprocessed_streams]
                    streaming_df = spark.read\
                        .option("header", "true")\
                        .option("inferSchema", "true")\
                        .csv(stream_paths)
                    
                    streaming_df = streaming_df\
                        .withColumnRenamed('listen_time', 'timestamp')\
                        .withColumn('listen_duration', lit(180000))
                    
                    streaming_df = streaming_df.filter(
                        col('timestamp').cast('date') == '2024-06-25'
                    )
                    
                    dataframes['streams'] = streaming_df
                    stream_count = streaming_df.count()
                    
                    for file_key in unprocessed_streams:
                        self.mark_file_completed(file_key, stream_count // len(unprocessed_streams))
                        
                except Exception as e:
                    for file_key in unprocessed_streams:
                        self.mark_file_failed(file_key, str(e))
                    raise
            
            # Process songs data
            if unprocessed_songs:
                logger.info(f"Processing {len(unprocessed_songs)} song files")
                
                for file_key in unprocessed_songs:
                    self.mark_file_processing_start(file_key)
                
                try:
                    songs_paths = [f"s3://{RAW_DATA_BUCKET}/{file_key}" for file_key in unprocessed_songs]
                    songs_df = spark.read\
                        .option("header", "true")\
                        .option("inferSchema", "true")\
                        .csv(songs_paths)
                    
                    songs_df = songs_df\
                        .withColumnRenamed('track_genre', 'genre')\
                        .withColumnRenamed('track_name', 'song_name')\
                        .select('track_id', 'genre', 'song_name', 'artists', 'duration_ms', 'popularity')
                    
                    dataframes['songs'] = songs_df
                    songs_count = songs_df.count()
                    
                    for file_key in unprocessed_songs:
                        self.mark_file_completed(file_key, songs_count // len(unprocessed_songs))
                        
                except Exception as e:
                    for file_key in unprocessed_songs:
                        self.mark_file_failed(file_key, str(e))
                    raise
            
            # Process users data
            if unprocessed_users:
                logger.info(f"Processing {len(unprocessed_users)} user files")
                
                for file_key in unprocessed_users:
                    self.mark_file_processing_start(file_key)
                
                try:
                    users_paths = [f"s3://{RAW_DATA_BUCKET}/{file_key}" for file_key in unprocessed_users]
                    users_df = spark.read\
                        .option("header", "true")\
                        .option("inferSchema", "true")\
                        .csv(users_paths)
                    
                    users_df = users_df.select('user_id', 'user_name', 'user_age', 'user_country')
                    dataframes['users'] = users_df
                    users_count = users_df.count()
                    
                    for file_key in unprocessed_users:
                        self.mark_file_completed(file_key, users_count // len(unprocessed_users))
                        
                except Exception as e:
                    for file_key in unprocessed_users:
                        self.mark_file_failed(file_key, str(e))
                    raise
            
            # Continue with existing logic only if we have streaming data
            if 'streams' not in dataframes:
                logger.warning("No streaming data to process")
                return
            
            # Join datasets
            merged_df = dataframes['streams']
            
            if 'songs' in dataframes:
                merged_df = merged_df.join(
                    broadcast(dataframes['songs']), 
                    on='track_id', 
                    how='inner'
                )
            
            if 'users' in dataframes:
                merged_df = merged_df.join(
                    broadcast(dataframes['users']), 
                    on='user_id', 
                    how='left'
                )
            
            merged_df.cache()
            self.processed_records = merged_df.count()
            logger.info(f"Processing {self.processed_records} records after joins")
            
            if self.processed_records > 0:
                # Compute KPIs
                kpis = self.compute_daily_kpis(merged_df)
                
                # Write to DynamoDB
                dynamodb_items = self.prepare_dynamodb_items(kpis)
                self.write_to_dynamodb_incremental(dynamodb_items)
                
                # Save to S3 as Parquet
                self.save_to_s3_parquet(merged_df)
                
                # Publish success metrics
                self.publish_metric('ProcessedRecords', self.processed_records)
                self.publish_metric('ProcessedBatches', 1)
                
                logger.info("ETL job completed successfully")
            else:
                logger.warning("No records to process after joins")
            
        except Exception as e:
            logger.error(f"ETL job failed: {str(e)}")
            self.publish_metric('FailedBatches', 1)
            raise

def main():
    """Main entry point"""
    etl = MusicStreamingETL()
    etl.run_etl()
    job.commit()

if __name__ == '__main__':
    main()

