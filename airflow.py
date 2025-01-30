from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.cloudwatch import CloudWatchHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def fetch_cloudwatch_metrics(**context):
    """Fetch metrics from CloudWatch"""
    cloudwatch = CloudWatchHook(aws_conn_id='aws_default')
    
    # Time range for metrics
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=6)
    
    # Query metrics
    metric_data = cloudwatch.conn.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "cpu",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EC2",
                        "MetricName": "CPUUtilization",
                        "Dimensions": [
                            {
                                "Name": "InstanceId",
                                "Value": "i-0604ed0c65018e348"
                            }
                        ]
                    },
                    "Period": 300,
                    "Stat": "Average"
                }
            },
            {
                "Id": "memory",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "CWAgent",
                        "MetricName": "Memory % Committed Bytes In Use",
                        "Dimensions": [
                            {
                                "Name": "InstanceId",
                                "Value": "i-0604ed0c65018e348"
                            }
                        ]
                    },
                    "Period": 300,
                    "Stat": "Average"
                }
            }
        ],
        StartTime=start_time,
        EndTime=end_time
    )
    
    return metric_data

def process_metrics(**context):
    """Process and transform metrics data"""
    ti = context['task_instance']
    metric_data = ti.xcom_pull(task_ids='fetch_metrics')
    
    # Transform to DataFrame
    processed_data = []
    for result in metric_data['MetricDataResults']:
        for timestamp, value in zip(result['Timestamps'], result['Values']):
            processed_data.append({
                'metric_id': result['Id'],
                'timestamp': timestamp,
                'value': value
            })
    
    df = pd.DataFrame(processed_data)
    return df.to_json()

def save_to_s3(**context):
    """Save processed metrics to S3"""
    ti = context['task_instance']
    processed_data = ti.xcom_pull(task_ids='process_metrics')
    
    # Get current timestamp for filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save to S3
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(
        string_data=processed_data,
        key=f'metrics/metrics_{timestamp}.json',
        bucket_name='your-bucket-name',
        replace=True
    )
    
    # Also save as latest
    s3_hook.load_string(
        string_data=processed_data,
        key='metrics/latest.json',
        bucket_name='your-bucket-name',
        replace=True
    )

# Create DAG
with DAG(
    'cloudwatch_metrics_collection',
    default_args=default_args,
    description='Collect CloudWatch metrics and store in S3',
    schedule_interval='0 */6 * * *',
    catchup=False
) as dag:

    fetch_metrics_task = PythonOperator(
        task_id='fetch_metrics',
        python_callable=fetch_cloudwatch_metrics
    )

    process_metrics_task = PythonOperator(
        task_id='process_metrics',
        python_callable=process_metrics
    )

    save_to_s3_task = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_to_s3
    )

    # Set task dependencies
    fetch_metrics_task >> process_metrics_task >> save_to_s3_task
