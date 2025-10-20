from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime 
import requests
import boto3
from botocore.client import BaseClient
import os
from dotenv import load_dotenv
from io import BytesIO
from google.transit import gtfs_realtime_pb2



load_dotenv()


URL = 'https://bustime.ttc.ca/gtfsrt/vehicles'


def fetch_vehicle_data(url):
    response = requests.get(url)
    try:
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"Error fetching data: {e}")
        return
    

def get_s3_client() -> BaseClient:
    s3_client = boto3.client(
        's3',
        endpoint_url=f'http://localhost:{os.getenv('MINIO_API_PORT')}',
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
    )
    return s3_client


def check_for_bucket(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return 
    except Exception as e:
        print('bucket does not exist')
        print(f"error: {str(e)}")
        s3_client.create_bucket(Bucket=bucket_name)
        print('bucket created!')
        return
        

def insert_vehicle_feed(content, bucket_name):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)    

    feed_timestamp = feed.header.timestamp
    feed_datetime = datetime.fromtimestamp(feed_timestamp)

    upload_timestamp = datetime.now().timestamp()

    obj_key = f"vehicle/year={feed_datetime.year}/month={feed_datetime.month}/day={feed_datetime.day}/hour={feed_datetime.hour}/{feed_timestamp}.bin"
    bin_file = BytesIO(content)

    s3_client = get_s3_client()
    s3_client.put_fileobj(
        Fileobj=bin_file,
        Bucket=bucket_name,
        Key=obj_key,
        metadata={
            'upload_timestamp': str(upload_timestamp)
        }
    )


