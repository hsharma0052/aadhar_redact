"""
Configuration file for the image processing script.
Copy this file to config.py and fill in your credentials.
DO NOT commit config.py to version control.
"""

# AWS Credentials
AWS_ACCESS_KEY_ID = "your_aws_access_key_id"
AWS_SECRET_ACCESS_KEY = "your_aws_secret_access_key"
AWS_REGION = "your_aws_region"  # e.g., "ap-south-1"

# S3 Bucket Names
SOURCE_BUCKET = "your_source_bucket_name"
DESTINATION_BUCKET = "your_destination_bucket_name"

# API Credentials
API_USERNAME = "your_api_username"
API_PASSWORD = "your_api_password"

# Processing Configuration
MAX_WORKERS = 4  # Number of concurrent workers for processing 