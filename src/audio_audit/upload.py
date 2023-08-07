import boto3 
import os
from loguru import logger

s3 = boto3.resource("s3")
s3 = boto3.client("s3")

def upload_video_for_transcribe(file_path):
    try:
        s3.upload_file(
            Filename=file_path,
            Bucket="traceaudit",
            Key=os.path.basename(file_path),
        )
        return True
        
    except Exception as ex:
        logger.error(f"FAILED TO UPLOAD VIDEO IN S3 BUCKET: {ex}")
        return False