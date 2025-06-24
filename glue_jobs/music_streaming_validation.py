# glue_jobs/music_streaming_validation.py
import sys
import boto3
import pandas as pd
import logging
from datetime import datetime
import json
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get job parameters
args = {}
for arg in sys.argv:
    if '=' in arg:
        key, value = arg.split('=', 1)
        args[key.replace('--', '')] = value

# Configuration
RAW_DATA_BUCKET = args.get('raw_data_bucket', 'rds.data')
PROCESSED_BUCKET = args.get('processed_bucket', 'trial.data')
AWS_REGION = args.get('aws_region', 'us-east-1')
BATCH_ID = args.get('batch_id', 'default-batch')

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=AWS_REGION)
cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)

class MusicStreamingDataValidator:
    def __init__(self):
        self.validation_results = {
            'batch_id': BATCH_ID,
            'validation_timestamp': datetime.now().isoformat(),
            'streams_validation': {},
            'songs_validation': {},
            'users_validation': {},
            'overall_status': 'PENDING'
        }
        
        # ONLY required columns - nothing else matters
        self.required_columns = {
            'streams': ['user_id', 'track_id', 'listen_time'],
            'songs': ['track_id', 'track_genre', 'track_name', 'artists'],
            'users': ['user_id', 'user_name', 'user_age', 'user_country']
        }
    
    def validate_dataset_files(self, dataset_type):
        """Check ONLY if required columns exist - nothing else"""
        validation_result = {
            'files': [],
            'total_files': 0,
            'passed_files': 0,
            'status': 'PENDING',
            'required_columns': self.required_columns[dataset_type],
            'missing_columns': []
        }
        
        try:
            # List files in S3
            response = s3_client.list_objects_v2(
                Bucket=RAW_DATA_BUCKET,
                Prefix=f"{dataset_type}/"
            )
            
            if 'Contents' not in response:
                validation_result['status'] = 'FAILED'
                validation_result['error'] = f"No files found for {dataset_type}"
                return validation_result
            
            csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            validation_result['total_files'] = len(csv_files)
            
            if not csv_files:
                validation_result['status'] = 'FAILED'
                validation_result['error'] = f"No CSV files found for {dataset_type}"
                return validation_result
            
            # Check each file for required columns only
            all_missing_columns = set()
            
            for file_key in csv_files:
                file_result = self.check_required_columns_only(file_key, dataset_type)
                validation_result['files'].append(file_result)
                
                if file_result['has_required_columns']:
                    validation_result['passed_files'] += 1
                else:
                    all_missing_columns.update(file_result['missing_columns'])
            
            validation_result['missing_columns'] = list(all_missing_columns)
            
            # Simple pass/fail based on required columns only
            if validation_result['passed_files'] == validation_result['total_files']:
                validation_result['status'] = 'PASSED'
            else:
                validation_result['status'] = 'FAILED'
            
            logger.info(f"{dataset_type}: {validation_result['status']} - Required columns: {self.required_columns[dataset_type]}")
            
        except Exception as e:
            validation_result['status'] = 'FAILED'
            validation_result['error'] = str(e)
            logger.error(f"Error validating {dataset_type}: {str(e)}")
        
        return validation_result
    
    def check_required_columns_only(self, file_key, dataset_type):
        """Check ONLY if required columns exist in file - ignore everything else"""
        file_result = {
            'file_key': file_key,
            'has_required_columns': False,
            'missing_columns': [],
            'file_readable': False
        }
        
        try:
            # Read file
            obj = s3_client.get_object(Bucket=RAW_DATA_BUCKET, Key=file_key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            file_result['file_readable'] = True
            
            # Get file columns
            file_columns = set(df.columns)
            required_columns = set(self.required_columns[dataset_type])
            
            # Check if ALL required columns are present
            missing_columns = required_columns - file_columns
            
            if not missing_columns:
                file_result['has_required_columns'] = True
                logger.info(f" {file_key}: All required columns present")
            else:
                file_result['missing_columns'] = list(missing_columns)
                logger.error(f"{file_key}: Missing required columns: {list(missing_columns)}")
            
        except Exception as e:
            logger.error(f"Error reading file {file_key}: {str(e)}")
            file_result['error'] = str(e)
        
        return file_result
    
    def save_validation_results(self):
        """Save validation results to S3"""
        try:
            results_key = f"validation_results/batch_id={BATCH_ID}/validation_report.json"
            
            s3_client.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=results_key,
                Body=json.dumps(self.validation_results, indent=2, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"Validation results saved to s3://{PROCESSED_BUCKET}/{results_key}")
            
        except Exception as e:
            logger.error(f"Failed to save validation results: {str(e)}")
    
    def publish_validation_metrics(self):
        """Publish simple validation metrics"""
        try:
            overall_success = 1 if self.validation_results['overall_status'] == 'PASSED' else 0
            
            cloudwatch.put_metric_data(
                Namespace='MusicStreaming/DataValidation',
                MetricData=[
                    {
                        'MetricName': 'ValidationSuccess',
                        'Value': overall_success,
                        'Unit': 'Count',
                        'Dimensions': [
                            {'Name': 'BatchId', 'Value': BATCH_ID}
                        ]
                    }
                ]
            )
            
            logger.info("Published validation metrics to CloudWatch")
            
        except Exception as e:
            logger.error(f"Failed to publish metrics: {str(e)}")
    
    def run_validation(self):
        """Main validation - CHECK REQUIRED COLUMNS ONLY"""
        try:
            logger.info(f"Starting REQUIRED COLUMNS validation for batch {BATCH_ID}")
            logger.info("=== CHECKING REQUIRED COLUMNS ONLY ===")
            
            # Log what we're checking for
            for dataset, columns in self.required_columns.items():
                logger.info(f"{dataset.upper()} required: {columns}")
            
            # Validate each dataset - REQUIRED COLUMNS ONLY
            for dataset_type in ['streams', 'songs', 'users']:
                logger.info(f"\n--- Checking {dataset_type} required columns ---")
                validation_result = self.validate_dataset_files(dataset_type)
                self.validation_results[f'{dataset_type}_validation'] = validation_result
            
            # Overall status - MUST have streams with required columns
            streams_status = self.validation_results.get('streams_validation', {}).get('status')
            
            if streams_status == 'PASSED':
                self.validation_results['overall_status'] = 'PASSED'
                logger.info("REQUIRED COLUMNS VALIDATION PASSED")
            else:
                self.validation_results['overall_status'] = 'FAILED'
                logger.error("REQUIRED COLUMNS VALIDATION FAILED")
            
            # Save results
            self.save_validation_results()
            self.publish_validation_metrics()
            
            # Summary
            logger.info(f"\n=== VALIDATION SUMMARY ===")
            logger.info(f"Overall Status: {self.validation_results['overall_status']}")
            for dataset in ['streams', 'songs', 'users']:
                dataset_result = self.validation_results.get(f'{dataset}_validation', {})
                status = dataset_result.get('status', 'NOT_PROCESSED')
                missing = dataset_result.get('missing_columns', [])
                logger.info(f"{dataset}: {status} {f'(Missing: {missing})' if missing else ''}")
            
            # Exit based on validation result
            if self.validation_results['overall_status'] == 'FAILED':
                logger.error("Required columns validation failed - stopping pipeline")
                sys.exit(1)
            else:
                logger.info("Required columns validation passed - pipeline continues")
            
        except Exception as e:
            logger.error(f"Validation job failed: {str(e)}")
            sys.exit(1)

def main():
    """Main entry point"""
    validator = MusicStreamingDataValidator()
    validator.run_validation()

if __name__ == '__main__':
    main()

