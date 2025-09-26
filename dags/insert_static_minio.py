from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.sensors.external_task import ExternalTaskSensor
import os
from urllib.parse import quote


default_args = {
    'owner': 'Sam',
    'start_date': datetime(2025, 9, 18),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': 'False'
}


dag = DAG(
    dag_id='static_files_to_minio',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)


def check_bucket():
    print("Starting bucket creation...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    if s3_hook.check_for_bucket('ttc-static-data'):
        print('bucket already exists, skipping creation!')
        return
    print("S3Hook initialized, creating bucket...")
    s3_hook.create_bucket('ttc-static-data')
    print('bucket created!')


def check_bucket_for_update():
    last_modified = Variable.get('StaticFileLastModified')
    etag = Variable.get('StaticFileETag')
    etag_clean = quote(etag.strip('"'), safe='')
    prefix = f"{etag_clean}-{last_modified}/"
    bucket_name = 'ttc-static-data'


    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    cond = s3_hook.check_for_prefix(
        bucket_name=bucket_name,
        prefix=prefix,
        delimiter='/'
    )

    if cond:
        print("prefix found. Static files not updated!")
        return False
    return True



def upload_files():
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    last_modified = Variable.get('StaticFileLastModified')
    etag = Variable.get('StaticFileETag')
    etag_clean = quote(etag.strip('"'), safe='')
    bucket_name = 'ttc-static-data'
    temp_bucket = 'ttc-temp-data'
    prefix = f"{etag_clean}-{last_modified}/"

    file_keys = s3_hook.list_keys(
        bucket_name=temp_bucket,
        prefix='statics/',
        delimiter='/'
    )

    for file in file_keys:
        filename = file.split('/')[-1]  
        s3_hook.copy_object(
            source_bucket_key=file,
            source_bucket_name=temp_bucket,
            dest_bucket_key=f"{prefix}{filename}", 
            dest_bucket_name=bucket_name
        )
    s3_hook.delete_objects(
        bucket=temp_bucket,
        keys=file_keys
    )


    print(f"Uploaded {len(file_keys)} files to bucket {bucket_name} with prefix {prefix}")


check_bucket_operator = PythonOperator(
    dag=dag,
    task_id='create_bucket',
    python_callable=check_bucket
)


check_bucket_for_update_operator = ShortCircuitOperator(
    dag=dag,
    task_id='check_bucket_for_update',
    python_callable=check_bucket_for_update
)


upload_files_operator = PythonOperator(
    dag=dag,
    task_id='upload_files',
    python_callable=upload_files,
)


check_bucket_operator >> check_bucket_for_update_operator >> upload_files_operator