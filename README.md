# S3 to DigitalOcean Spaces Transfer Tool

A Python script to transfer all contents from an AWS S3 bucket to a DigitalOcean Space.

## Prerequisites

- Python 3.x
- AWS S3 bucket access credentials
- DigitalOcean Spaces access credentials
- A DigitalOcean Space created and ready to receive files

## Installation

1. Clone this repository or download the script files
2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Create a `.env` file in the project root directory using the provided template:
```bash
cp .env.example .env
```

2. Fill in your credentials in the `.env` file:
```plaintext
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET_NAME=your_s3_bucket_name

DO_ACCESS_KEY_ID=your_do_access_key
DO_SECRET_ACCESS_KEY=your_do_secret_key
DO_SPACE_NAME=your_space_name
DO_REGION=your_region  # Optional, defaults to nyc3
```

## Usage

Run the transfer script:
```bash
python transfer_s3_2_space.py
```

The script will:
1. Connect to your S3 bucket and DigitalOcean Space
2. List all objects in the S3 bucket
3. Transfer each object to the DigitalOcean Space
4. Maintain the same file structure and names
5. Display progress information for each transfer

## Notes

- All files are transferred with 'private' ACL by default
- The script preserves the original content types of the files
- Large files are handled efficiently using streaming transfer
- Progress is shown for each individual file transfer
