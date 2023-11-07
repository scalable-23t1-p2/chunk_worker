import boto3
import botocore
import os
from dotenv import load_dotenv

BUCKET_NAME = 'toktikbucket' # replace with your bucket name
KEY = 'example_user/bahn.webm' # replace with your object key

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
)
try:
    s3.download_file(BUCKET_NAME,KEY, 'my_local_image.webm')
    # response = s3.list_buckets()
    # bucket_list = []
    # for bucket in response['Buckets']:
    #     bucket_list.append(bucket["Name"])
    # print(bucket_list)
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise