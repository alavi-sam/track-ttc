from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from airflow.operators.bash import Bash
import requests
from zipfile import ZipFile
from datetime import datetime, timedelta
import os



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
    schedule_interval='@daily',
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
    data_dir = '/tmp/data'
    os.makedirs(data_dir, exist_ok=True)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    file_path = os.path.join(data_dir, 'static-ttc-file.zip')
    with open(file_path, 'wb') as zf:
        for chunk in response.iter_content(chunk_size=8192):
            zf.write(chunk)
    
    
def extract_zip():
    data_dir = '/tmp/data'
    extract_dir = os.path.join(data_dir, 'static-ttc-files')
    os.makedirs(extract_dir, exist_ok=True)
    file_path = os.path.join(data_dir, 'static-ttc-file.zip')
    with ZipFile(file_path, 'r') as zf:
        zf.extractall(extract_dir)
    print(os.listdir(extract_dir))


def clear_zip():
    data_dir = '/tmp/data'
    os.remove(os.path.join(data_dir, 'static-ttc-file.zip'))
    print('deleted zip file!')


def update_etag():
    response = requests.get(url)
    response.raise_for_status()
    file_version = response.headers.get('ETag')
    last_modified = response.headers.get('Last-Modified')
    Variable.set('StaticFileLastModified', last_modified)
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