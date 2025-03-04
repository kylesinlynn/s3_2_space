import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 credentials
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')

# DigitalOcean Spaces credentials
do_access_key = os.getenv('DO_ACCESS_KEY_ID')
do_secret_key = os.getenv('DO_SECRET_ACCESS_KEY')
do_space_name = os.getenv('DO_SPACE_NAME')
do_region = os.getenv('DO_REGION', 'nyc3')  # Default region is NYC3


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
        print(f"[Error] DigitalOcean Space '{do_space_name}' is not accessible: {str(e)}")
        print("[Info] Please check if:")
        print("  - The Space name is correct")
        print("  - The Space exists in the specified region")
        print("  - Your DO credentials have proper permissions")
        exit(1)
    
    return s3_client, do_client


def transfer_files():
    s3_client, do_client = setup_clients()
    
    # List all objects in S3 bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=s3_bucket_name):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                print(f"[S3] Starting download: {key}")
                
                try:
                    # Download from S3
                    response = s3_client.get_object(Bucket=s3_bucket_name, Key=key)
                    print(f"[S3] Successfully downloaded: {key}")
                    
                    print(f"[Spaces] Starting upload: {key}")
                    # Upload to DigitalOcean Spaces with explicit content type
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
                    
                    print(f"[Spaces] Successfully uploaded: {key}")
                    
                except Exception as e:
                    print(f"[Error] Failed transferring {key}: {str(e)}")
                    print("[Info] Please verify your DO Space configuration in .env file:")
                    print(f"  - Space Name: {do_space_name}")
                    print(f"  - Region: {do_region}")

if __name__ == "__main__":
    transfer_files()
