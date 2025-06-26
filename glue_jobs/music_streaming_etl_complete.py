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
