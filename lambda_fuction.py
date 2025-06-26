import json
import boto3
import urllib3
import base64
import ast
import time
from botocore.exceptions import ClientError
from botocore.config import Config
import os

def lambda_handler(event, context):
    # Configure boto3 with timeouts
    config = Config(
        connect_timeout=10,
        read_timeout=20,
        retries={'max_attempts': 2}
    )
    
    # Get configuration from environment variables
    mwaa_environment_name = os.environ.get('MWAA_ENVIRONMENT_NAME')
    dag_id = os.environ.get('DAG_ID')
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    
    # Validate required environment variables
    required_env_vars = ['MWAA_ENVIRONMENT_NAME', 'DAG_ID']
    for var in required_env_vars:
        if not os.environ.get(var):
            error_msg = f"Missing required environment variable: {var}"
            print(error_msg)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': error_msg})
            }

    mwaa_client = boto3.client('mwaa', region_name=aws_region, config=config)
    
    # Generate batch_id based on S3 event or timestamp
    batch_id = f"batch-{int(time.time())}"
    if 'Records' in event and event['Records']:
        record = event['Records'][0]
        if 's3' in record:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            batch_id = f"batch-{bucket}-{key.replace('/', '-')}-{int(time.time())}"
    
    # MWAA CLI command to trigger DAG with batch_id
    mwaa_cli_command = f'dags trigger {dag_id} -c \'{{"batch_id": "{batch_id}"}}\''
    
    # Create urllib3 PoolManager
    http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=10, read=15))

    try:
        print(f"Lambda timeout: {context.get_remaining_time_in_millis()} ms")
        print(f"Starting DAG trigger for: {dag_id} with batch_id: {batch_id}")
        
        # Get MWAA CLI token
        print("Getting CLI token...")
        start_time = time.time()
        
        try:
            response = mwaa_client.create_cli_token(Name=mwaa_environment_name)
            cli_token = response['CliToken']
            web_server_hostname = response['WebServerHostname']
            
            print(f"CLI token obtained successfully")
            print(f"Web Server Hostname: {web_server_hostname}")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"AWS Error getting CLI token: {error_code} - {e}")
            
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'Failed to get CLI token: {error_code}',
                    'details': str(e)
                })
            }

        if not cli_token or not web_server_hostname:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to obtain CLI token or hostname'})
            }

        # Use the correct MWAA CLI endpoint
        cli_endpoint = f"https://{web_server_hostname}/aws_mwaa/cli"
        
        headers = {
            'Authorization': f'Bearer {cli_token}',
            'Content-Type': 'text/plain'
        }

        print(f"Executing CLI command: {mwaa_cli_command}")
        print(f"CLI Endpoint: {cli_endpoint}")
        
        try:
            # Send the CLI command to MWAA
            response = http.request(
                'POST',
                cli_endpoint,
                body=mwaa_cli_command,
                headers=headers,
                timeout=urllib3.Timeout(connect=5, read=15)
            )
            
            print(f"CLI Response Status: {response.status}")
            
            if response.status == 200:
                # Parse the MWAA CLI response
                data = response.data.decode("UTF-8")
                print(f"Raw CLI Response: {data}")
                
                try:
                    # MWAA CLI returns a string representation of a dict
                    response_dict = ast.literal_eval(data)
                    
                    # Decode the base64 stdout content
                    stdout_content = base64.b64decode(response_dict['stdout']).decode('utf-8')
                    stderr_content = ''
                    
                    if 'stderr' in response_dict and response_dict['stderr']:
                        stderr_content = base64.b64decode(response_dict['stderr']).decode('utf-8')
                    
                    print(f"CLI stdout: {stdout_content}")
                    if stderr_content:
                        print(f"CLI stderr: {stderr_content}")
                    
                    # Check if DAG trigger was successful
                    success_indicators = [
                        'Created <DagRun',
                        'Triggered DAG',
                        '| queued',
                        '| running',
                        '| success'
                    ]
                    
                    is_success = any(indicator in stdout_content for indicator in success_indicators)
                    
                    # Extract DAG run ID from output if available
                    dag_run_id = 'N/A'
                    if 'manual__' in stdout_content:
                        lines = stdout_content.split('\n')
                        for line in lines:
                            if 'manual__' in line and dag_id.replace('_', '') in line.replace('_', ''):
                                parts = line.split('|')
                                if len(parts) > 2:
                                    dag_run_id = parts[2].strip()
                                    break
                    
                    if is_success:
                        return {
                            'statusCode': 200,
                            'body': json.dumps({
                                'message': f'DAG {dag_id} triggered successfully',
                                'dag_run_id': dag_run_id,
                                'batch_id': batch_id,
                                'status': 'queued' if '| queued' in stdout_content else 'triggered',
                                'timestamp': int(time.time()),
                                'note': 'DAG is now queued for execution in Airflow'
                            })
                        }
                    elif 'already exists' in stdout_content.lower():
                        return {
                            'statusCode': 200,
                            'body': json.dumps({
                                'message': f'DAG {dag_id} run already exists (normal for rapid calls)',
                                'dag_run_id': dag_run_id,
                                'batch_id': batch_id
                            })
                        }
                    else:
                        return {
                            'statusCode': 400,
                            'body': json.dumps({
                                'error': 'Could not confirm DAG trigger success',
                                'cli_output': stdout_content[:500],
                                'batch_id': batch_id,
                                'note': 'Check Airflow UI to verify DAG execution'
                            })
                        }
                    
                except (ValueError, SyntaxError) as e:
                    print(f"Failed to parse CLI response: {e}")
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'error': 'Failed to parse MWAA CLI response',
                            'raw_response': data[:500],
                            'batch_id': batch_id
                        })
                    }
                    
            elif response.status == 401:
                return {
                    'statusCode': 401,
                    'body': json.dumps({
                        'error': 'Unauthorized - CLI token invalid or expired',
                        'solution': 'Check MWAA environment permissions',
                        'batch_id': batch_id
                    })
                }
            elif response.status == 403:
                return {
                    'statusCode': 403,
                    'body': json.dumps({
                        'error': 'Forbidden - insufficient permissions',
                        'solution': 'Check IAM policy includes airflow:CreateCliToken',
                        'batch_id': batch_id
                    })
                }
            else:
                error_body = response.data.decode('utf-8')[:500]
                return {
                    'statusCode': response.status,
                    'body': json.dumps({
                        'error': f'CLI command failed with status {response.status}',
                        'response': error_body,
                        'batch_id': batch_id
                    })
                }
                
        except Exception as e:
            print(f"CLI request failed: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'CLI request failed',
                    'details': str(e)[:200],
                    'batch_id': batch_id
                })
            }

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Unexpected error: {str(e)}'[:200],
                'batch_id': batch_id
            })
        }
