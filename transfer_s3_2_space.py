import boto3
import os
import time
from dotenv import load_dotenv
from pathlib import Path
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Load environment variables with explicit path
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

# AWS S3 credentials
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')

# DigitalOcean Spaces credentials
do_access_key = os.getenv('DO_ACCESS_KEY_ID')
do_secret_key = os.getenv('DO_SECRET_ACCESS_KEY')
do_space_name = os.getenv('DO_SPACE_NAME')
do_region = os.getenv('DO_REGION')

# Set up logging
log_filename = f'transfer_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def setup_clients():
    # Set up AWS S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    
    # Set up DigitalOcean Spaces client
    do_client = boto3.client(
        's3',
        endpoint_url=f'https://{do_region}.digitaloceanspaces.com',
        aws_access_key_id=do_access_key,
        aws_secret_access_key=do_secret_key
    )
    
    try:
        do_client.head_bucket(Bucket=do_space_name)
    except Exception as e:
        error_msg = f"[Error] DigitalOcean Space '{do_space_name}' is not accessible: {str(e)}"
        logging.error(error_msg)
        logging.info("Please check if:")
        logging.info("  - The Space name is correct")
        logging.info("  - The Space exists in the specified region")
        logging.info("  - Your DO credentials have proper permissions")
        print(error_msg)
        exit(1)
    
    return s3_client, do_client


log_lock = Lock()


def safe_log(message, level=logging.INFO, also_print=True):
    with log_lock:
        if level == logging.ERROR:
            logging.error(message)
        else:
            logging.info(message)
        if also_print:
            print(message)


def transfer_single_file(s3_client, do_client, key):
    try:
        safe_log(f"[S3] Starting download: {key}")
        
        # Download from S3
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=key)
        safe_log(f"[S3] Successfully downloaded: {key}")
        
        safe_log(f"[Spaces] Starting upload: {key}")
        # Upload to DigitalOcean Spaces
        content_type = response.get('ContentType', 'application/octet-stream')
        do_client.upload_fileobj(
            response['Body'],
            do_space_name,
            key,
            ExtraArgs={
                'ACL': 'private',
                'ContentType': content_type
            }
        )
        
        safe_log(f"[Spaces] Successfully uploaded: {key}")
        return True, key
        
    except Exception as e:
        error_msg = f"[Error] Failed transferring {key}: {str(e)}"
        safe_log(error_msg, level=logging.ERROR)
        return False, key


def transfer_files():
    start_time = time.time()
    s3_client, do_client = setup_clients()
    
    # List all objects in S3 bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    all_files = []
    
    # Collect all files first
    for page in paginator.paginate(Bucket=s3_bucket_name):
        if 'Contents' in page:
            all_files.extend([obj['Key'] for obj in page['Contents']])
    
    total_files = len(all_files)
    safe_log(f"[Info] Found {total_files} files to transfer")
    
    # Use ThreadPoolExecutor for parallel transfer
    successful_transfers = 0
    failed_transfers = 0
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all transfer tasks
        future_to_key = {
            executor.submit(transfer_single_file, s3_client, do_client, key): key 
            for key in all_files
        }
        
        # Process completed transfers
        for future in as_completed(future_to_key):
            success, key = future.result()
            if success:
                successful_transfers += 1
            else:
                failed_transfers += 1
            
            # Log progress
            total_processed = successful_transfers + failed_transfers
            progress = (total_processed / total_files) * 100
            safe_log(f"[Progress] {progress:.2f}% complete ({total_processed}/{total_files})")
    
    # Log final statistics
    end_time = time.time()
    execution_time = end_time - start_time
    
    safe_log("\n=== Transfer Summary ===")
    safe_log(f"Total files: {total_files}")
    safe_log(f"Successfully transferred: {successful_transfers}")
    safe_log(f"Failed transfers: {failed_transfers}")
    safe_log(f"Total execution time: {execution_time:.2f} seconds")
    safe_log(f"Average time per file: {execution_time/total_files:.2f} seconds")


if __name__ == "__main__":
    transfer_files()
