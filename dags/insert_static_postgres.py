from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import DAG
from urllib.parse import quote
import tempfile
import os

default_args = {
    'owner': 'Sam',
    'start_date': datetime(2025, 9, 18),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

dag = DAG(
    dag_id='insert_static_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)


def create_static_tables():
    pg_hook = PostgresHook(postgres_conn_id='app_db')
    sql = """
    DROP TABLE IF EXISTS stop_times CASCADE;
    DROP TABLE IF EXISTS calendar_dates CASCADE;
    DROP TABLE IF EXISTS trips CASCADE;
    DROP TABLE IF EXISTS stops CASCADE;
    DROP TABLE IF EXISTS shapes CASCADE;
    DROP TABLE IF EXISTS routes CASCADE;
    DROP TABLE IF EXISTS calendar CASCADE;
    DROP TABLE IF EXISTS agency CASCADE;

    CREATE TABLE IF NOT EXISTS agency (
        agency_id INTEGER PRIMARY KEY,
        agency_name TEXT,
        agency_url TEXT,
        agency_timezone TEXT,
        agency_lang TEXT,
        agency_phone TEXT,
        agency_fare_url TEXT,
        agency_email TEXT
    );

    CREATE TABLE IF NOT EXISTS calendar (
        service_id INTEGER PRIMARY KEY,
        monday INTEGER,
        tuesday INTEGER,
        wednesday INTEGER,
        thursday INTEGER,
        friday INTEGER,
        saturday INTEGER,
        sunday INTEGER,
        start_date TEXT,
        end_date TEXT
    );

    CREATE TABLE IF NOT EXISTS routes (
        route_id INTEGER PRIMARY KEY,
        agency_id INTEGER REFERENCES agency(agency_id),
        route_short_name TEXT,
        route_long_name TEXT,
        route_desc TEXT,
        route_type INTEGER,
        route_url TEXT,
        route_color TEXT,
        route_text_color TEXT
    );

    CREATE TABLE IF NOT EXISTS shapes (
        shape_id TEXT,
        shape_pt_lat REAL,
        shape_pt_lon REAL,
        shape_pt_sequence INTEGER,
        shape_dist_traveled REAL,
        PRIMARY KEY (shape_id, shape_pt_sequence)
    );

    CREATE TABLE IF NOT EXISTS trips (
        route_id INTEGER REFERENCES routes(route_id),
        service_id INTEGER REFERENCES calendar(service_id),
        trip_id TEXT PRIMARY KEY,
        trip_headsign TEXT,
        direction_id INTEGER,
        block_id TEXT,
        shape_id TEXT,
        wheelchair_accessible INTEGER,
        trip_type INTEGER,
        bikes_allowed INTEGER
    );

    CREATE TABLE IF NOT EXISTS stops (
        stop_id INTEGER PRIMARY KEY,
        stop_code TEXT,
        stop_name TEXT,
        stop_desc TEXT,
        stop_lat REAL,
        stop_lon REAL,
        zone_id TEXT,
        stop_url TEXT,
        location_type INTEGER,
        parent_station TEXT,
        stop_timezone TEXT,
        wheelchair_boarding INTEGER
    );

    CREATE TABLE IF NOT EXISTS stop_times (
        trip_id TEXT REFERENCES trips(trip_id),
        arrival_time TEXT,
        departure_time TEXT,
        stop_id INTEGER REFERENCES stops(stop_id),
        stop_sequence INTEGER,
        stop_headsign TEXT,
        pickup_type INTEGER,
        drop_off_type INTEGER,
        shape_dist_traveled TEXT,
        PRIMARY KEY (trip_id, stop_sequence)
    );

    CREATE TABLE IF NOT EXISTS calendar_dates (
        service_id INTEGER,
        date TEXT,
        exception_type INTEGER
    );
    """
    pg_hook.run(sql)

def insert_static_data():
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    pg_hook = PostgresHook(postgres_conn_id='app_db')
    
    last_modified = Variable.get('StaticFileLastModified')
    etag = Variable.get('StaticFileETag')
    etag_clean = quote(etag.strip('"'), safe='')
    bucket_name = 'ttc-static-data'
    prefix = f"{etag_clean}-{last_modified}/"

    file_keys = s3_hook.list_keys(
        bucket_name=bucket_name,
        prefix=prefix,
        delimiter='/'
    )

    tables = [
        'agency', 'calendar', 'routes', 'shapes', 'stops', 'trips', 'calendar_dates', 'stop_times'
    ]

    pg_hook.run(f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE;")

    for table in tables:
        file_key = f"{prefix}{table}.txt"
        if file_key in file_keys:
            file_content = s3_hook.read_key(file_key, bucket_name)

            # Write to temporary file for copy_expert
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
                tmp.write(file_content)
                tmp_path = tmp.name

            try:
                pg_hook.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", tmp_path)
                print(f"Data inserted into {table}")
            finally:
                os.remove(tmp_path)

create_tables_task = PythonOperator(
    dag=dag,
    task_id='create_static_tables',
    python_callable=create_static_tables
)

insert_data_task = PythonOperator(
    dag=dag,
    task_id='insert_static_data',
    python_callable=insert_static_data
)

create_tables_task >> insert_data_task  
