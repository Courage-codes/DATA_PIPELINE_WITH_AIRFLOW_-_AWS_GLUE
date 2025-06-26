import sys
import boto3
import pandas as pd
import logging
from datetime import datetime
import json
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parse Glue job parameters
def parse_args():
    args = {}
    for arg in sys.argv:
        if '=' in arg:
            key, value = arg.split('=', 1)
            args[key.replace('--', '')] = value
    return args

args = parse_args()
logger.info(f"Parsed Glue job arguments: {args}")

# Get parameters from arguments, no defaults
RAW_DATA_BUCKET = args['raw_data_bucket']
PROCESSED_BUCKET = args['processed_bucket']
AWS_REGION = args['aws_region']
BATCH_ID = args.get('batch_id', datetime.now().strftime('%Y%m%d%H%M%S'))
logger.info(f"Using batch_id: {BATCH_ID}")

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
        self.required_columns = {
            'streams': ['user_id', 'track_id', 'listen_time'],
            'songs': ['track_id', 'track_genre', 'track_name', 'artists'],
            'users': ['user_id', 'user_name', 'user_age', 'user_country']
        }

    def validate_dataset_files(self, dataset_type):
        result = {
            'files': [],
            'total_files': 0,
            'passed_files': 0,
            'status': 'PENDING',
            'required_columns': self.required_columns[dataset_type],
            'missing_columns': []
        }
        try:
            response = s3_client.list_objects_v2(Bucket=RAW_DATA_BUCKET, Prefix=f"{dataset_type}/")
            if 'Contents' not in response:
                result['status'] = 'FAILED'
                result['error'] = f"No files found for {dataset_type}"
                return result
            csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            result['total_files'] = len(csv_files)
            if not csv_files:
                result['status'] = 'FAILED'
                result['error'] = f"No CSV files found for {dataset_type}"
                return result

            all_missing = set()
            for file_key in csv_files:
                res = self.check_required_columns_only(file_key, dataset_type)
                result['files'].append(res)
                if res['has_required_columns']:
                    result['passed_files'] += 1
                else:
                    all_missing.update(res['missing_columns'])

            result['missing_columns'] = list(all_missing)
            result['status'] = 'PASSED' if result['passed_files'] == result['total_files'] else 'FAILED'
            logger.info(f"{dataset_type}: {result['status']}")
        except Exception as e:
            result['status'] = 'FAILED'
            result['error'] = str(e)
            logger.error(f"Error validating {dataset_type}: {str(e)}")
        return result

    def check_required_columns_only(self, file_key, dataset_type):
        res = {
            'file_key': file_key,
            'has_required_columns': False,
            'missing_columns': [],
            'file_readable': False
        }
        try:
            obj = s3_client.get_object(Bucket=RAW_DATA_BUCKET, Key=file_key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            res['file_readable'] = True
            required = set(self.required_columns[dataset_type])
            missing = required - set(df.columns)
            if not missing:
                res['has_required_columns'] = True
                logger.info(f"{file_key}: all required columns present")
            else:
                res['missing_columns'] = list(missing)
                logger.error(f"{file_key}: missing columns {missing}")
        except Exception as e:
            res['error'] = str(e)
            logger.error(f"Error reading {file_key}: {str(e)}")
        return res

    def save_validation_results(self):
        try:
            key = f"validation_results/batch_id={BATCH_ID}/validation_report.json"
            s3_client.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=key,
                Body=json.dumps(self.validation_results, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Saved validation results to s3://{PROCESSED_BUCKET}/{key}")
        except Exception as e:
            logger.error(f"Failed to save validation results: {str(e)}")

    def publish_validation_metrics(self):
        try:
            cloudwatch.put_metric_data(
                Namespace='MusicStreaming/DataValidation',
                MetricData=[{
                    'MetricName': 'ValidationSuccess',
                    'Value': 1 if self.validation_results['overall_status'] == 'PASSED' else 0,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'BatchId', 'Value': BATCH_ID}]
                }]
            )
            logger.info("Published CloudWatch metric")
        except Exception as e:
            logger.error(f"Metric publish failed: {str(e)}")

    def run_validation(self):
        try:
            logger.info(f"Validating batch: {BATCH_ID}")
            for dataset in ['streams', 'songs', 'users']:
                self.validation_results[f'{dataset}_validation'] = self.validate_dataset_files(dataset)

            if self.validation_results['streams_validation']['status'] == 'PASSED':
                self.validation_results['overall_status'] = 'PASSED'
            else:
                self.validation_results['overall_status'] = 'FAILED'

            self.save_validation_results()
            self.publish_validation_metrics()
            logger.info(f"Validation complete - overall status: {self.validation_results['overall_status']}")

            if self.validation_results['overall_status'] == 'FAILED':
                sys.exit(1)
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            sys.exit(1)

def main():
    validator = MusicStreamingDataValidator()
    validator.run_validation()

if __name__ == '__main__':
    main()