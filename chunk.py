import subprocess
import utils
import boto3
import os
from dotenv import load_dotenv
import s3utils
from celery import Celery

raw_video = "raw_video"
chunk_output = "chunk_output"
BROKER_URL = os.getenv("CELERY_BROKER_URL")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
celery_app = Celery('chunk', broker=BROKER_URL,
                    backend=RESULT_BACKEND)

def upload_playlist(client, filename: str):
    for i in os.listdir(chunk_output):
        if not i.startswith(filename):
            continue
        if ".m3u8" in i:
            s3utils.upload_s3_file(client, f"{chunk_output}/{i}", i)
        else:
            s3utils.upload_s3_file(
                client, f"{chunk_output}/{i}", f"{filename}/{i}"
            )

@celery_app.task(name="chunk")
def chunk():
    client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY")
    )
    utils.create_dir(raw_video)
    utils.create_dir(chunk_output)
    print("Chunking video into segments")
    file = 'bahn.mp4'
    filename, extension = utils.extract_ext(file)
    output_file = f"{filename}.m3u8"
    BUCKET_NAME = 'toktikbucket'
    S3_PATH = 'example_user/bahn.mp4'
    LOCAL_PATH=f"{raw_video}/{filename}"
    S3_UPLOAD_PATH = f'example_user/{output_file}'
    VIDEO_CHUNK_SEC = "5"
    client.download_file(Bucket=BUCKET_NAME, Key=S3_PATH, Filename=LOCAL_PATH)
    # s3utils.download_s3_file(client= client, local_path=LOCAL_PATH, s3_path=S3_PATH)
    subprocess.run(
                [
                    "ffmpeg",
                    "-i",
                    f"{LOCAL_PATH}",
                    "-codec:",
                    "copy",
                    "-start_number",
                    "0",
                    "-hls_time",
                    VIDEO_CHUNK_SEC,
                    "-hls_list_size",
                    "0",
                    "-f",
                    "hls",
                    f"{chunk_output}/{output_file}",
                ]
            )
    print("done chunking")
    upload_playlist(client, filename)
    print("done upload to s3")
    # utils.clean_dir(filename)

if __name__ == "__main__":
    chunk()




