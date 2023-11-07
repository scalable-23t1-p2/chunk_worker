import boto3
import os
from dotenv import load_dotenv
from botocore.client import BaseClient

BUCKET = "toktikbucket"

def upload_s3_file(client: BaseClient, local_path: str, s3_path: str):
    client.upload_file(Filename=local_path, Bucket=BUCKET, Key=s3_path)


def download_s3_file(client: BaseClient, local_path: str, s3_path: str):
    client.download_file(Bucket=BUCKET, Key=s3_path, Filename=local_path)