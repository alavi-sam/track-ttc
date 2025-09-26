from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.operators.bash import Bash
import requests
from zipfile import ZipFile
from datetime import datetime, timedelta
import os
import io



url = 'https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/7795b45e-e65a-4465-81fc-c36b9dfff169/resource/cfb6b2b8-6191-41e3-bda1-b175c51148cb/download/TTC%20Routes%20and%20Schedules%20Data.zip'


default_args = {
    'owner': 'Sam',
    'start_date': datetime(2025, 9, 18),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}


dag = DAG(
    dag_id='fetch_ttc_static_data',
    default_args=default_args,
    schedule='@daily',
    catchup=False
)


def check_for_static_update():
    response = requests.get(url)
    response.raise_for_status()
    file_version = response.headers.get('ETag')
    prev_version = Variable.get('StaticFileETag', '')
    if file_version == prev_version:
        print('DAG aborted! No new file found!')
        return False
    return True


def download_zip():
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')

    if not s3_hook.check_for_bucket('ttc-temp-data'):
        s3_hook.create_bucket('ttc-temp-data')

    response = requests.get(url)
    response.raise_for_status()

    s3_hook.load_bytes(
        bytes_data=response.content,
        key='statics/static-ttc-file.zip',
        bucket_name='ttc-temp-data',
        replace=True
    )
    print("Uploaded zip file to MinIO")
    
    
def extract_zip():
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    zip_file = s3_hook.get_key(
        key='statics/static-ttc-file.zip',
        bucket_name='ttc-temp-data',
    )
    zip_content = zip_file.get()['Body'].read()
    with ZipFile(io.BytesIO(zip_content), 'r') as zf:
        file_list = []
        for file_info in zf.infolist():
            if not file_info.is_dir():
                file_content = zf.read(file_info.filename)
                s3_hook.load_bytes(
                    bytes_data=file_content,
                    key=f'statics/{file_info.filename}',
                    bucket_name='ttc-temp-data',
                    replace=True
                )
                file_list.append(file_info.filename)

        print('extracted files: ', file_list)

        

def clear_zip():
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    s3_hook.delete_objects(
        bucket='ttc-temp-data',
        keys='statics/static-ttc-file.zip'
    )
    print('deleted zip file!')


def update_etag():
    response = requests.get(url)
    response.raise_for_status()
    file_version = response.headers.get('ETag')
    last_modified = response.headers.get('Last-Modified')
    parsed_last_modified = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT')
    Variable.set('StaticFileLastModified', parsed_last_modified.strftime('%Y%m%d-%H%M%S'))
    Variable.set('StaticFileETag', file_version)
    print(f"Updated ETag to {file_version}")


download_zip_task = PythonOperator(
    dag=dag,
    task_id='download_static_file_zip',
    python_callable=download_zip
)

extract_zip_task = PythonOperator(
    dag=dag,
    task_id='extract_zip_file',
    python_callable=extract_zip
)

clear_zip_task = PythonOperator(
    dag=dag,
    task_id='clear_zip_file',
    python_callable=clear_zip
)


check_update_task = ShortCircuitOperator(
    dag=dag,
    task_id='check_for_update',
    python_callable=check_for_static_update
)


update_etag_task = PythonOperator(
    dag=dag,
    task_id='update_etag',
    python_callable=update_etag
)

check_update_task >> download_zip_task >> extract_zip_task >> update_etag_task >> clear_zip_task