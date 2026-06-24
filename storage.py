import os
import shutil
import uuid
from typing import Optional
from fastapi import UploadFile
import boto3
from botocore.exceptions import NoCredentialsError

# Variables
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "camplog-public-media")
S3_REGION = os.getenv("AWS_REGION", "us-east-1")

LOCAL_UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
if ENVIRONMENT == "local":
    os.makedirs(LOCAL_UPLOAD_DIR, exist_ok=True)

s3_client = None
if ENVIRONMENT in ["staging", "prod"]:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=S3_REGION
    )

def handle_upload(file: UploadFile, folder: str = "profiles") -> Optional[str]:
    """
    Handles file upload depending on the environment.
    """
    ext = file.filename.split(".")[-1] if "." in file.filename else "bin"
    unique_filename = f"{folder}/{uuid.uuid4().hex}.{ext}"
    
    if ENVIRONMENT == "local":
        return _save_local(file, unique_filename)
    else:
        return _save_s3(file, unique_filename)

def _save_local(file: UploadFile, unique_filename: str) -> str:
    # Ensure folder structure exists locally
    local_path = os.path.join(LOCAL_UPLOAD_DIR, unique_filename)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # Reset file pointer
    file.file.seek(0)
    with open(local_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    return f"http://localhost:8000/uploads/{unique_filename}"

def _save_s3(file: UploadFile, unique_filename: str) -> Optional[str]:
    try:
        file.file.seek(0)
        s3_client.upload_fileobj(
            file.file, 
            S3_BUCKET_NAME, 
            unique_filename,
            ExtraArgs={"ContentType": file.content_type, "ACL": "public-read"}
        )
        return f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{unique_filename}"
    except Exception as e:
        print(f"[DATA PLATFORM] Erro ao enviar arquivo para S3: {e}")
        return None
