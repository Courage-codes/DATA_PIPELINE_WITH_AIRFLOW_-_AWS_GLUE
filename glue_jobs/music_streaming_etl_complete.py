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
