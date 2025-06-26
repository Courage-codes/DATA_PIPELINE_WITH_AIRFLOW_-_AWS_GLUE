from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, lit, current_timestamp, broadcast, sum as spark_sum, 
    count, countDistinct, avg, desc, row_number
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
from contextlib import contextmanager
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_job_parameters():
    """Get job parameters from Glue job arguments with proper validation"""
    try:
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME', 'batch_id', 'processing_date', 
            'raw_data_bucket', 'processed_bucket',
            'archive_bucket', 'dynamodb_table_name', 'aws_region',
            'max_records_per_partition', 'task_type'
        ])
        
        # Validate required parameters
        required_params = ['JOB_NAME', 'batch_id', 'processing_date', 'raw_data_bucket', 'processed_bucket', 'dynamodb_table_name']
        for param in required_params:
            if not args.get(param):
                raise ValueError(f"Required parameter '{param}' is missing or empty")
        
        # Set defaults for optional parameters
        args.setdefault('archive_bucket', f"{args['raw_data_bucket']}-archive")
        args.setdefault('max_records_per_partition', '1000000')
        args.setdefault('task_type', 'full_etl')
        args.setdefault('aws_region', 'us-east-1')
        
        logger.info(f"Job parameters loaded: {args}")
        return args
        
    except Exception as e:
        logger.error(f"Failed to get job parameters: {str(e)}")
        raise

# Get job parameters
args = get_job_parameters()

# Configuration
BATCH_ID = args['batch_id']
PROCESSING_DATE = args['processing_date']
RAW_DATA_BUCKET = args['raw_data_bucket']
PROCESSED_BUCKET = args['processed_bucket']
ARCHIVE_BUCKET = args['archive_bucket']
DYNAMODB_TABLE_NAME = args['dynamodb_table_name']
AWS_REGION = args['aws_region']
MAX_RECORDS_PER_PARTITION = int(args['max_records_per_partition'])
TASK_TYPE = args['task_type']

# Initialize contexts with proper error handling and configuration
try:
    from pyspark.conf import SparkConf
    
    conf = SparkConf()
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true") 
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    sc = SparkContext(conf=conf)
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    logger.info("Spark contexts initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Spark contexts: {str(e)}")
    raise

# AWS clients
try:
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)
except Exception as e:
    logger.error(f"Failed to initialize AWS clients: {str(e)}")
    raise

@contextmanager
def spark_resource_manager():
    """Context manager for Spark resource cleanup"""
    try:
        yield
    finally:
        try:
            if 'spark' in globals():
                spark.catalog.clearCache()
                logger.info("Cleared Spark cache")
        except Exception as e:
            logger.warning(f"Error during Spark cleanup: {str(e)}")

class MusicStreamingETL:
    def __init__(self):
        self.processed_records = 0
        self.failed_records = 0
        self.quarantined_records = 0
        self.archived_files = []

    def publish_metric(self, metric_name, value, unit='Count'):
        """Publish custom CloudWatch metrics"""
        try:
            cloudwatch.put_metric_data(
                Namespace='ETL/Metrics',
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
        """Generate partition key with hash prefix"""
        hash_prefix = str(hash(f"{genre}{date_str}") % 100).zfill(2)
        return f"{hash_prefix}#{genre}#{date_str}"

    def convert_for_dynamodb(self, obj):
        """Convert data types for DynamoDB compatibility"""
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
                logger.info(f"DynamoDB table {DYNAMODB_TABLE_NAME} created")
                return table
            else:
                raise
    def create_file_tracking_table(self):
        """Create DynamoDB table for file tracking"""
        tracking_table_name = f"{DYNAMODB_TABLE_NAME}_file_tracker"
        try:
            table = dynamodb.Table(tracking_table_name)
            table.load()
            logger.info(f"File tracking table {tracking_table_name} exists")
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
                logger.info(f"File tracking table {tracking_table_name} created")
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
                all_files = [obj['Key'] for obj in response['Contents'] 
                           if obj['Key'].endswith('.csv') and obj['Size'] > 0]
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
                            logger.info(f"Skipping processed file: {file_key}")
                except Exception as e:
                    logger.error(f"Error checking file status for {file_key}: {e}")
                    unprocessed_files.append(file_key)
            logger.info(f"Found {len(unprocessed_files)} unprocessed {dataset_type} files")
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

    def archive_processed_files(self, processed_files):
        """Archive successfully processed files"""
        archived_count = 0
        failed_archives = 0
        for file_key in processed_files:
            try:
                archive_key = f"archive/{datetime.now().strftime('%Y/%m/%d')}/{file_key}"
                copy_source = {'Bucket': RAW_DATA_BUCKET, 'Key': file_key}
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=ARCHIVE_BUCKET,
                    Key=archive_key
                )
                s3_client.delete_object(Bucket=RAW_DATA_BUCKET, Key=file_key)
                self.archived_files.append(archive_key)
                archived_count += 1
                logger.info(f"Archived file: {file_key} -> {archive_key}")
            except Exception as e:
                logger.error(f"Failed to archive file {file_key}: {str(e)}")
                failed_archives += 1
        logger.info(f"Archived {archived_count} files, {failed_archives} failures")
        self.publish_metric('ArchivedFiles', archived_count)
        self.publish_metric('ArchiveFailures', failed_archives)

    def read_csv_with_schema_validation(self, file_paths, expected_schema=None):
        """Read CSV files with schema validation"""
        try:
            df = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .option("multiline", "true")\
                .option("escape", "\"")\
                .csv(file_paths)
            if expected_schema and df.columns != expected_schema:
                logger.warning(f"Schema mismatch. Expected: {expected_schema}, Got: {df.columns}")
            if df.count() == 0:
                logger.warning("Empty DataFrame after reading CSV files")
                return None
            return df
        except Exception as e:
            logger.error(f"Error reading CSV files: {str(e)}")
            raise

    def compute_daily_kpis(self, merged_df):
        """Compute daily KPIs"""
        try:
            if merged_df.count() == 0:
                logger.warning("Empty DataFrame for KPI computation")
                return {}
            kpis = {}
            genre_kpis = merged_df.filter(col('genre').isNotNull())\
                                 .groupBy('genre')\
                                 .agg(
                                     count('*').alias('listen_count'),
                                     countDistinct('user_id').alias('unique_listeners'),
                                     spark_sum('listen_duration').alias('total_listening_time'),
                                     avg('listen_duration').alias('avg_listening_time_per_user')
                                 )\
                                 .filter(col('listen_count') > 0)
            kpis['genre_metrics'] = genre_kpis
            window_spec = Window.partitionBy('genre').orderBy(desc('play_count'))
            song_plays = merged_df.filter(col('genre').isNotNull() & col('track_id').isNotNull())\
                                 .groupBy('genre', 'track_id', 'song_name')\
                                 .agg(count('*').alias('play_count'))\
                                 .filter(col('play_count') > 0)
            if song_plays.count() > 0:
                top_songs = song_plays.withColumn('rank', row_number().over(window_spec))\
                                     .filter(col('rank') <= 3)
                kpis['top_songs_per_genre'] = top_songs
            if 'genre_metrics' in kpis and genre_kpis.count() > 0:
                top_genres = genre_kpis.orderBy(desc('listen_count')).limit(5)
                kpis['top_genres'] = top_genres
            logger.info("Computed daily KPIs successfully")
            return kpis
        except Exception as e:
            logger.error(f"Error computing KPIs: {str(e)}")
            raise

    def prepare_dynamodb_items(self, kpis):
        """Prepare items for DynamoDB insertion"""
        items = []
        try:
            if 'genre_metrics' in kpis:
                for row in kpis['genre_metrics'].collect():
                    base_partition_key = self.generate_partition_key(row['genre'], PROCESSING_DATE)
                    metrics = [
                        ('listen_count', row['listen_count']),
                        ('unique_listeners', row['unique_listeners']),
                        ('total_listening_time', row['total_listening_time']),
                        ('avg_listening_time_per_user', row['avg_listening_time_per_user'])
                    ]
                    for metric_type, value in metrics:
                        if value is not None:
                            items.append({
                                'partition_key': base_partition_key,
                                'sort_key': f"{metric_type}#{PROCESSING_DATE}",
                                'genre': row['genre'],
                                'date': PROCESSING_DATE,
                                'metric_type': metric_type,
                                'value': self.convert_for_dynamodb(value),
                                'batch_id': BATCH_ID,
                                'created_at': datetime.now().isoformat()
                            })
            if 'top_songs_per_genre' in kpis:
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
                        'batch_id': BATCH_ID,
                        'created_at': datetime.now().isoformat()
                    })
            if 'top_genres' in kpis:
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
                        'batch_id': BATCH_ID,
                        'created_at': datetime.now().isoformat()
                    })
            logger.info(f"Prepared {len(items)} items for DynamoDB")
            return items
        except Exception as e:
            logger.error(f"Error preparing DynamoDB items: {str(e)}")
            raise
    
    def write_to_dynamodb_batch(self, items):
        """Write items to DynamoDB using batch operations"""
        if not items:
            logger.info("No items to write to DynamoDB")
            return
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        successful_writes = 0
        failed_writes = 0
        try:
            batch_size = 25
            with table.batch_writer() as batch:
                for i in range(0, len(items), batch_size):
                    batch_items = items[i:i + batch_size]
                    for item in batch_items:
                        try:
                            converted_item = {k: self.convert_for_dynamodb(v) 
                                            for k, v in item.items() if v is not None}
                            batch.put_item(Item=converted_item)
                            successful_writes += 1
                        except Exception as e:
                            logger.error(f"Failed to write batch item: {str(e)}")
                            failed_writes += 1
                    if i % (batch_size * 10) == 0:
                        logger.info(f"Processed {i + len(batch_items)} / {len(items)} items")
        except Exception as e:
            logger.error(f"Batch write operation failed: {str(e)}")
            failed_writes += len(items) - successful_writes
        logger.info(f"DynamoDB writes - Successful: {successful_writes}, Failed: {failed_writes}")
        self.publish_metric('DynamoDBWrites', successful_writes)
        self.publish_metric('DynamoDBWriteFailures', failed_writes)

    def process_and_write_to_dynamodb(self, merged_df):
        """Process KPIs and write to DynamoDB"""
        try:
            logger.info("Starting DynamoDB processing task")
            if merged_df is None:
                logger.error("merged_df is None")
                return False
            record_count = merged_df.count()
            logger.info(f"DynamoDB processing: {record_count} records")
            if record_count == 0:
                logger.warning("No records for DynamoDB processing")
                return False
            logger.info("DataFrame schema:")
            merged_df.printSchema()
            logger.info("Sample data (first 3 rows):")
            merged_df.show(3, truncate=False)
            logger.info("Computing KPIs...")
            kpis = self.compute_daily_kpis(merged_df)
            if kpis:
                logger.info("KPI Computation Results:")
                for kpi_name, kpi_df in kpis.items():
                    if hasattr(kpi_df, 'count'):
                        kpi_count = kpi_df.count()
                        logger.info(f"KPI {kpi_name}: {kpi_count} records")
                        if kpi_count > 0:
                            logger.info(f"Sample {kpi_name} data:")
                            kpi_df.show(3, truncate=False)
                logger.info("Preparing DynamoDB items...")
                dynamodb_items = self.prepare_dynamodb_items(kpis)
                if dynamodb_items:
                    logger.info(f"Writing {len(dynamodb_items)} items to DynamoDB")
                    logger.info("Sample DynamoDB items:")
                    for i, item in enumerate(dynamodb_items[:3]):
                        logger.info(f"Item {i+1}: {json.dumps(item, indent=2, default=str)}")
                    self.write_to_dynamodb_batch(dynamodb_items)
                    logger.info("DynamoDB processing completed")
                    return True
                else:
                    logger.warning("No DynamoDB items prepared")
                    return False
            else:
                logger.warning("No KPIs computed")
                return False
        except Exception as e:
            logger.error(f"DynamoDB processing failed: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def archive_files_task(self, processed_files_list):
        """Archive files as a separate task"""
        try:
            if processed_files_list:
                self.archive_processed_files(processed_files_list)
                logger.info(f"Archived {len(processed_files_list)} files")
            else:
                logger.info("No files to archive")
        except Exception as e:
            logger.error(f"Archiving task failed: {str(e)}")
            raise
    
    def save_to_s3_parquet(self, df):
        """Save processed data to S3 as Parquet"""
        try:
            output_path = f"s3://{PROCESSED_BUCKET}/processed_data/date={PROCESSING_DATE}/batch_id={BATCH_ID}/"
            df_with_metadata = df.withColumn("processing_timestamp", current_timestamp())\
                                .withColumn("batch_id", lit(BATCH_ID))\
                                .withColumn("processing_date", lit(PROCESSING_DATE))
            record_count = df_with_metadata.count()
            if record_count > MAX_RECORDS_PER_PARTITION:
                num_partitions = max(1, record_count // MAX_RECORDS_PER_PARTITION)
                df_with_metadata = df_with_metadata.repartition(num_partitions, "genre")
            df_with_metadata.write\
                .mode("append")\
                .partitionBy("genre")\
                .option("compression", "snappy")\
                .option("maxRecordsPerFile", str(MAX_RECORDS_PER_PARTITION))\
                .parquet(output_path)
            logger.info(f"Saved {record_count} records to {output_path}")
            self.publish_metric('RecordsSavedToS3', record_count)
        except Exception as e:
            logger.error(f"Failed to save to S3: {str(e)}")
            self.publish_metric('S3SaveFailures', 1)
            raise

    def run_etl(self):
        """Main ETL execution"""
        processed_files = []
        merged_df = None
        dataframes = {}
        try:
            logger.info(f"Starting ETL for batch {BATCH_ID}")
            self.create_dynamodb_table_if_not_exists()
            self.create_file_tracking_table()
            unprocessed_streams = self.get_unprocessed_files('streams')
            unprocessed_songs = self.get_unprocessed_files('songs')
            unprocessed_users = self.get_unprocessed_files('users')
            if not unprocessed_streams and not unprocessed_songs and not unprocessed_users:
                logger.info("No new files to process")
                return []
            if unprocessed_streams:
                logger.info(f"Processing {len(unprocessed_streams)} streaming files")
                for file_key in unprocessed_streams:
                    self.mark_file_processing_start(file_key)
                try:
                    stream_paths = [f"s3://{RAW_DATA_BUCKET}/{file_key}" for file_key in unprocessed_streams]
                    streaming_df = self.read_csv_with_schema_validation(
                        stream_paths, 
                        expected_schema=['user_id', 'track_id', 'timestamp']
                    )
                    if streaming_df is None:
                        raise ValueError("Failed to read streaming data")
                    streaming_df = streaming_df\
                        .withColumnRenamed('listen_time', 'timestamp')\
                        .withColumn('listen_duration', lit(180000))
                    streaming_df = streaming_df.filter(
                        col('timestamp').cast('date') == PROCESSING_DATE
                    )
                    stream_count = streaming_df.count()
                    if stream_count > 0 and stream_count < MAX_RECORDS_PER_PARTITION:
                        streaming_df.cache()
                    dataframes['streams'] = streaming_df
                    for file_key in unprocessed_streams:
                        self.mark_file_completed(file_key, stream_count // len(unprocessed_streams))
                        processed_files.append(file_key)
                except Exception as e:
                    for file_key in unprocessed_streams:
                        self.mark_file_failed(file_key, str(e))
                    raise
            if unprocessed_songs:
                logger.info(f"Processing {len(unprocessed_songs)} song files")
                for file_key in unprocessed_songs:
                    self.mark_file_processing_start(file_key)
                try:
                    songs_paths = [f"s3://{RAW_DATA_BUCKET}/{file_key}" for file_key in unprocessed_songs]
                    songs_df = self.read_csv_with_schema_validation(
                        songs_paths,
                        expected_schema=['track_id', 'track_genre', 'track_name', 'artists', 'duration_ms', 'popularity']
                    )
                    if songs_df is None:
                        raise ValueError("Failed to read songs data")
                    songs_df = songs_df\
                        .withColumnRenamed('track_genre', 'genre')\
                        .withColumnRenamed('track_name', 'song_name')\
                        .select('track_id', 'genre', 'song_name', 'artists', 'duration_ms', 'popularity')\
                        .filter(col('track_id').isNotNull() & col('genre').isNotNull())
                    songs_df = songs_df.dropDuplicates(['track_id'])
                    dataframes['songs'] = songs_df
                    songs_count = songs_df.count()
                    for file_key in unprocessed_songs:
                        self.mark_file_completed(file_key, songs_count // len(unprocessed_songs))
                        processed_files.append(file_key)
                except Exception as e:
                    for file_key in unprocessed_songs:
                        self.mark_file_failed(file_key, str(e))
                    raise
            if unprocessed_users:
                logger.info(f"Processing {len(unprocessed_users)} user files")
                for file_key in unprocessed_users:
                    self.mark_file_processing_start(file_key)
                try:
                    users_paths = [f"s3://{RAW_DATA_BUCKET}/{file_key}" for file_key in unprocessed_users]
                    users_df = self.read_csv_with_schema_validation(
                        users_paths,
                        expected_schema=['user_id', 'user_name', 'user_age', 'user_country']
                    )
                    if users_df is None:
                        raise ValueError("Failed to read users data")
                    users_df = users_df.select('user_id', 'user_name', 'user_age', 'user_country')\
                                      .filter(col('user_id').isNotNull())\
                                      .dropDuplicates(['user_id'])
                    dataframes['users'] = users_df
                    users_count = users_df.count()
                    for file_key in unprocessed_users:
                        self.mark_file_completed(file_key, users_count // len(unprocessed_users))
                        processed_files.append(file_key)
                except Exception as e:
                    for file_key in unprocessed_users:
                        self.mark_file_failed(file_key, str(e))
                    raise
            if 'streams' not in dataframes:
                logger.warning("No streaming data to process")
                return []
            streaming_df = dataframes['streams']
            if streaming_df.count() == 0:
                logger.warning("Empty streaming DataFrame")
                return []
            merged_df = streaming_df
            if 'songs' in dataframes and dataframes['songs'].count() > 0:
                logger.info("Joining with songs data")
                merged_df = merged_df.join(
                    broadcast(dataframes['songs']), 
                    on='track_id', 
                    how='inner'
                )
                after_join_count = merged_df.count()
                if after_join_count == 0:
                    logger.error("No records after songs join")
                    raise ValueError("Join with songs data resulted in empty DataFrame")
                logger.info(f"Records after songs join: {after_join_count}")
            if 'users' in dataframes and dataframes['users'].count() > 0:
                logger.info("Joining with users data")
                merged_df = merged_df.join(
                    broadcast(dataframes['users']), 
                    on='user_id', 
                    how='left'
                )
                after_join_count = merged_df.count()
                logger.info(f"Records after users join: {after_join_count}")
            merged_df.cache()
            self.processed_records = merged_df.count()
            logger.info(f"Processing {self.processed_records} records after joins")
            if self.processed_records > 0:
                logger.info("Processing KPIs and writing to DynamoDB")
                try:
                    success = self.process_and_write_to_dynamodb(merged_df)
                    if success:
                        logger.info("DynamoDB processing completed")
                    else:
                        logger.warning("DynamoDB processing failed, continuing with S3 save")
                except Exception as e:
                    logger.error(f"DynamoDB processing failed: {str(e)}")
                    self.publish_metric('DynamoDBProcessingFailures', 1)
                logger.info("Saving processed data to S3")
                try:
                    self.save_to_s3_parquet(merged_df)
                    logger.info("S3 save completed")
                except Exception as e:
                    logger.error(f"S3 save failed: {str(e)}")
                    self.publish_metric('S3SaveFailures', 1)
                self.publish_metric('ProcessedRecords', self.processed_records)
                self.publish_metric('ProcessedBatches', 1)
                self.publish_metric('ProcessedFiles', len(processed_files))
                logger.info(f"ETL completed. Processed {self.processed_records} records from {len(processed_files)} files")
                return processed_files
            else:
                logger.warning("No records to process after joins")
                return []
        except Exception as e:
            logger.error(f"ETL job failed: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            for file_key in processed_files:
                self.mark_file_failed(file_key, str(e))
            self.publish_metric('FailedBatches', 1)
            self.publish_metric('FailedFiles', len(processed_files))
            raise
        finally:
            try:
                if merged_df is not None and hasattr(merged_df, 'unpersist'):
                    merged_df.unpersist()
                    logger.info("Unpersisted merged DataFrame")
                for df_name, df in dataframes.items():
                    if hasattr(df, 'unpersist'):
                        df.unpersist()
                        logger.info(f"Unpersisted {df_name} DataFrame")
                spark.catalog.clearCache()
            except Exception as e:
                logger.warning(f"Error during cleanup: {str(e)}")

def main():
    """Main entry point with task-based execution"""
    etl = None
    try:
        with spark_resource_manager():
            etl = MusicStreamingETL()
            if TASK_TYPE == 'full_etl':
                logger.info("Running full ETL")
                processed_files = etl.run_etl()
                logger.info("Full ETL completed")
            elif TASK_TYPE == 'dynamodb_only':
                logger.info("Starting DynamoDB-only processing")
                input_path = f"s3://{PROCESSED_BUCKET}/processed_data/date={PROCESSING_DATE}/batch_id={BATCH_ID}/"
                try:
                    merged_df = spark.read.parquet(input_path)
                    success = etl.process_and_write_to_dynamodb(merged_df)
                    if success:
                        logger.info("DynamoDB processing completed")
                    else:
                        raise Exception("DynamoDB processing failed")
                except Exception as e:
                    logger.error(f"Error reading processed data for DynamoDB: {str(e)}")
                    raise
            elif TASK_TYPE == 'archiving_only':
                logger.info("Starting archiving-only processing")
                tracking_table_name = f"{DYNAMODB_TABLE_NAME}_file_tracker"
                tracking_table = dynamodb.Table(tracking_table_name)
                try:
                    response = tracking_table.scan(
                        FilterExpression='batch_id = :batch_id AND #status = :status',
                        ExpressionAttributeNames={'#status': 'status'},
                        ExpressionAttributeValues={
                            ':batch_id': BATCH_ID,
                            ':status': 'completed'
                        }
                    )
                    processed_files = [item['file_key'] for item in response['Items']]
                    etl.archive_files_task(processed_files)
                except Exception as e:
                    logger.error(f"Error in archiving task: {str(e)}")
                    raise
            else:
                raise ValueError(f"Unknown task type: {TASK_TYPE}")
        logger.info(f"Task '{TASK_TYPE}' completed")
        job.commit()
    except Exception as e:
        logger.error(f"Task '{TASK_TYPE}' failed: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        if etl:
            etl.publish_metric('JobFailures', 1)
        raise
    finally:
        try:
            if 'sc' in globals() and sc:
                sc.stop()
                logger.info("Spark context stopped")
        except Exception as e:
            logger.warning(f"Error stopping Spark context: {str(e)}")

if __name__ == '__main__':
    main()