from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import logging
from psycopg2.extras import execute_batch




@dag(
    dag_id='scan_bronze_layer_files_vehicle',
    schedule_interval="0 * * * *",
    default_args={
        'owner': 'Sam',
        'start_date': datetime(2025, 10, 23),
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False
    },
    catchup=False
)
def scan_vehicle_bronze_layer_files():
    
    BUCKET_NAME = 'vehicle-bronze-layer'
    TABLE_NAME = 'vehicle_bronze_files'



    @task
    def check_bucket(bucket_name: str):
        s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
        if not s3_hook.check_for_bucket(bucket_name):
            logging.error(f'bucket {bucket_name} does not exists!')
            raise AirflowException(f'bucket {bucket_name} not found!')
        return True

    @task
    def check_sql_table(table_name: str):
        pg_hook = PostgresHook(postgres_conn_id='app_db')
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            id SERIAL PRIMARY KEY,
            file_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            fetch_time_stamp TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )           
                            """)
                conn.commit()
        logging.info(f'table {table_name} created!')
        return True
    
    @task
    def scan_bucket(bucket_name: str, **kwargs):
        s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
        execution_datetime = kwargs['data_interval_start']
        year, month, day, hour = execution_datetime.year, execution_datetime.month, execution_datetime.day, execution_datetime.hour
        prefix = f"vehicle/year={year}/month={month}/day={day}/hour={hour}/"
        
        logging.info(f"scanning bucket {bucket_name} with prefix {prefix}")

        file_keys = s3_hook.list_keys(
            bucket_name=bucket_name,
            prefix=prefix,
        )
        logging.info(f"found {len(file_keys)} files in bucket {bucket_name} with prefix {prefix}")

        data_to_insert = list()
        for key in file_keys:
            if key.endswith('.bin'):
                data_to_insert.append((key, str(key.split('/')[-1].split('.')[0])))
        return data_to_insert
    
    @task
    def insert_keys(data_to_insert: list, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='app_db')
        
        if not data_to_insert:
            logging.info("No new files to insert.")
            return


        sql_query = f"""
        INSERT INTO {TABLE_NAME} (file_key, fetch_time_stamp)
        VALUES (%s, TO_TIMESTAMP(%s))
        ON CONFLICT (file_key) DO NOTHING;
        """


        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_batch(
                    cur,
                    sql_query,
                    data_to_insert,
                    page_size=1000
                )
            conn.commit()

    check_bucket_task = check_bucket(BUCKET_NAME)
    check_sql_table_task = check_sql_table(TABLE_NAME)
    file_keys = scan_bucket(BUCKET_NAME)
    insert_keys_task = insert_keys(file_keys)

    [check_bucket_task, check_sql_table_task] >> file_keys
    file_keys >> insert_keys_task

scan_vehicle_bronze_layer_files()
