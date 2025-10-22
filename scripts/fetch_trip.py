from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import requests
import boto3
from botocore.client import BaseClient
import os
from dotenv import load_dotenv
from io import BytesIO
from google.transit import gtfs_realtime_pb2
import logging


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


URL = 'https://bustime.ttc.ca/gtfsrt/trips'
BUCKET_NAME = 'trip-bronze-layer'


def fetch_trip_data(url):
    response = requests.get(url)
    try:
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.error(f"Error fetching data: {e}", exc_info=True)
        return
    

def get_s3_client() -> BaseClient:
    endpoint_url = os.getenv('MINIO_ENDPOINT_URL', f'http://localhost:{os.getenv("MINIO_API_PORT")}')
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
    )
    return s3_client


def check_for_bucket(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return
    except Exception as e:
        logger.warning(f"Bucket '{bucket_name}' does not exist: {str(e)}")
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' created successfully")
        return
    

def insert_trip_feed(s3_client, content, bucket_name):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)

    feed_timestamp = feed.header.timestamp
    feed_datetime = datetime.fromtimestamp(feed_timestamp)

    upload_timestamp = datetime.now().timestamp()

    obj_key = f"trip/year={feed_datetime.year}/month={feed_datetime.month}/day={feed_datetime.day}/hour={feed_datetime.hour}/{feed_timestamp}.bin"
    bin_file = BytesIO(content)

    s3_client.upload_fileobj(
        Fileobj=bin_file,
        Bucket=bucket_name,
        Key=obj_key,
        ExtraArgs={
            'Metadata': {
                'upload_timestamp': str(upload_timestamp)
            }
        }
    )


def fetch_and_insert():
    try:
        logger.info("Starting trip data fetch...")
        content = fetch_trip_data(URL)
        if not content:
            logger.error("Failed to fetch data from TTC")
            return
        s3_client = get_s3_client()
        check_for_bucket(s3_client, BUCKET_NAME)
        insert_trip_feed(s3_client, content, BUCKET_NAME)
        logger.info(f"Successfully stored trip data to '{BUCKET_NAME}'")
    except Exception as e:
        logger.error(f"Error in fetch_and_insert job: {e}", exc_info=True)


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(
        fetch_and_insert,
        'interval',
        seconds=30,
        id='fetch_trip_data_job',
        name='Fetch and store TTC trip data',
        replace_existing=True
    )

    logger.info("Scheduler started. Press Ctrl+C to exit")

    fetch_and_insert()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")