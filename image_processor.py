import boto3
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import requests
from botocore.exceptions import ClientError
import json
import base64
import os
from requests_toolbelt.multipart.encoder import MultipartEncoder
import pandas as pd
import time
from typing import List, Set
import pickle
from pathlib import Path
from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    SOURCE_BUCKET,
    DESTINATION_BUCKET,
    API_USERNAME,
    API_PASSWORD,
    MAX_WORKERS
)
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('image_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ImageProcessor:
    def __init__(self, sheet_path: str, batch_size: int = 100):
        # Initialize S3 client with credentials
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        self.source_bucket = SOURCE_BUCKET
        self.destination_bucket = DESTINATION_BUCKET
        self.api_endpoint = "https://api.atlaskyc.com/v2/prod/aadhaar/mask"
        self.api_username = API_USERNAME
        self.api_password = API_PASSWORD
        self.max_workers = MAX_WORKERS
        self.sheet_path = sheet_path
        self.batch_size = batch_size
        
        # Initialize or load progress tracking
        self.progress_file = 'processing_progress.pkl'
        self.processed_folders: Set[str] = self.load_progress()
        
        self.stats = {
            'total_folders': 0,
            'processed_folders': 0,
            'failed_folders': set(),
            'total_images': 0,
            'processed_images': 0,
            'failed_images': 0
        }

    def load_progress(self) -> Set[str]:
        """Load the set of already processed folders from progress file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"Error loading progress file: {str(e)}")
                return set()
        return set()

    def save_progress(self):
        """Save the set of processed folders to progress file"""
        try:
            with open(self.progress_file, 'wb') as f:
                pickle.dump(self.processed_folders, f)
        except Exception as e:
            logger.error(f"Error saving progress file: {str(e)}")

    def get_folders_from_sheet(self) -> List[str]:
        """Read folder names from the sheet"""
        try:
            # Determine file type and read accordingly
            file_ext = Path(self.sheet_path).suffix.lower()
            if file_ext == '.csv':
                df = pd.read_csv(self.sheet_path)
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(self.sheet_path)
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
            # Assuming the folder names are in a column named 'folder' or the first column
            folder_column = 'folder' if 'folder' in df.columns else df.columns[0]
            folders = df[folder_column].astype(str).tolist()
            
            # Filter out already processed folders
            folders = [f for f in folders if f not in self.processed_folders]
            
            # Ensure folder names end with '/'
            folders = [f if f.endswith('/') else f + '/' for f in folders]
            
            logger.info(f"Found {len(folders)} folders to process")
            return folders
        except Exception as e:
            logger.error(f"Error reading sheet: {str(e)}")
            return []

    def process_batch(self, folders: List[str]):
        """Process a batch of folders"""
        for folder in tqdm(folders, desc="Processing folders"):
            try:
                if folder in self.processed_folders:
                    continue
                
                images = self.get_user_images(folder)
                if not images:
                    logger.warning(f"No images found in folder {folder}")
                    self.processed_folders.add(folder)
                    self.save_progress()
                    continue
                
                successful = 0
                failed = 0
                
                # Process one image at a time
                for image_key in tqdm(images, desc=f"Processing {folder}", leave=False):
                    if self.process_image(image_key):
                        successful += 1
                    else:
                        failed += 1
                        self.stats['failed_images'] += 1
                    # Add a small delay between requests
                    time.sleep(1)
                
                self.stats['processed_images'] += successful
                self.stats['total_images'] += len(images)
                
                if failed == 0:
                    self.processed_folders.add(folder)
                    self.save_progress()
                else:
                    self.stats['failed_folders'].add(folder)
                
                self.stats['processed_folders'] += 1
                
            except Exception as e:
                logger.error(f"Error processing folder {folder}: {str(e)}")
                self.stats['failed_folders'].add(folder)

    def run(self):
        """Main processing loop with batch processing"""
        folders = self.get_folders_from_sheet()
        self.stats['total_folders'] = len(folders)
        
        if not folders:
            logger.error("No folders to process")
            return
        
        # Process folders in batches
        for i in range(0, len(folders), self.batch_size):
            batch = folders[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1} of {(len(folders) + self.batch_size - 1)//self.batch_size}")
            self.process_batch(batch)
            
            # Save statistics after each batch
            self.save_statistics()
            
            # Optional: Add a delay between batches
            if i + self.batch_size < len(folders):
                logger.info("Waiting 5 seconds before next batch...")
                time.sleep(5)
        
        # Final statistics
        self.save_statistics()
        self.print_final_statistics()

    def save_statistics(self):
        """Save processing statistics to a JSON file"""
        stats = {
            'total_folders': self.stats['total_folders'],
            'processed_folders': self.stats['processed_folders'],
            'failed_folders': list(self.stats['failed_folders']),
            'total_images': self.stats['total_images'],
            'processed_images': self.stats['processed_images'],
            'failed_images': self.stats['failed_images'],
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open('processing_stats.json', 'w') as f:
            json.dump(stats, f, indent=2)

    def print_final_statistics(self):
        """Print final processing statistics"""
        logger.info(f"""
Processing complete!
Total folders: {self.stats['total_folders']}
Processed folders: {self.stats['processed_folders']}
Failed folders: {len(self.stats['failed_folders'])}
Total images: {self.stats['total_images']}
Successfully processed images: {self.stats['processed_images']}
Failed images: {self.stats['failed_images']}
        """)
        
        if self.stats['failed_folders']:
            logger.info("Failed folders:")
            for folder in sorted(self.stats['failed_folders']):
                logger.info(f"  {folder}")

    def get_user_images(self, user_folder: str):
        """Get list of all images in the specific folder"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            images = []
            
            for page in paginator.paginate(
                Bucket=self.source_bucket,
                Prefix=user_folder
            ):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
                            images.append(obj['Key'])
            
            logger.info(f"Found {len(images)} images in folder {user_folder}")
            return images
        except ClientError as e:
            logger.error(f"Error getting images for folder {user_folder}: {str(e)}")
            return []

    def get_basic_auth_header(self, username: str, password: str) -> str:
        """Generate Basic Auth header exactly like the Java implementation"""
        value_to_encode = f"{username}:{password}"
        encoded = base64.b64encode(value_to_encode.encode()).decode()
        auth_header = f"Basic {encoded}"
        logger.info(f"AUTHORIZATION IS: {auth_header}")
        return auth_header

    def process_image(self, image_key: str) -> bool:
        try:
            # Download image from S3
            response = self.s3_client.get_object(Bucket=self.source_bucket, Key=image_key)
            image_data = response['Body'].read()
            
            # Get just the filename without the path
            filename = os.path.basename(image_key)
            
            # Check if input is JSON and extract image data if it is
            try:
                json_data = json.loads(image_data)
                if isinstance(json_data, dict) and 'data' in json_data and 'file' in json_data['data']:
                    # Found base64 encoded image in JSON
                    logger.info(f"Found base64 encoded image in JSON for {image_key}")
                    try:
                        # Decode base64 image data
                        base64_image = json_data['data']['file']
                        if base64_image.startswith('data:image/'):
                            # Remove data URL prefix if present
                            base64_image = base64_image.split(',', 1)[1]
                        image_data = base64.b64decode(base64_image)
                        logger.info(f"Successfully decoded base64 image data for {image_key}")
                    except Exception as e:
                        logger.error(f"Failed to decode base64 image data for {image_key}: {str(e)}")
                        return False
            except json.JSONDecodeError:
                # Not JSON, proceed with raw image data
                logger.info(f"Input file {image_key} is raw image data")
            
            # Verify file format
            if not image_data.startswith(b'\xff\xd8\xff'):  # JPEG magic number
                logger.error(f"Input file {image_key} is not a valid JPEG image after processing")
                logger.error(f"First few bytes: {image_data[:20]}")
                return False
            
            # Create multipart form data
            files = {
                'file': (filename, image_data, 'image/jpeg', {'Content-Disposition': f'attachment; filename="{filename}"'})
            }
            
            # Get Basic Auth header
            auth_header = self.get_basic_auth_header(self.api_username, self.api_password)
            
            # Set headers
            headers = {
                'Authorization': auth_header,
                'Accept': 'image/jpeg'  # Explicitly request JPEG response
            }
            
            # Print detailed request information
            logger.info("=== Request Details ===")
            logger.info(f"URL: {self.api_endpoint}")
            logger.info("Headers:")
            for header, value in headers.items():
                logger.info(f"  {header}: {value}")
            logger.info("Files:")
            for file_key, file_info in files.items():
                logger.info(f"  {file_key}: {file_info[0]} (size: {len(file_info[1])} bytes, type: {file_info[2]})")
                logger.info(f"  Content-Disposition: {file_info[3]['Content-Disposition']}")
            logger.info("===================")
            
            # Make the API request
            api_response = requests.post(
                self.api_endpoint,
                files=files,
                headers=headers,
                timeout=30
            )
            
            # Log detailed response information
            logger.info("=== Response Details ===")
            logger.info(f"Status Code: {api_response.status_code}")
            logger.info("Response Headers:")
            for header, value in api_response.headers.items():
                logger.info(f"  {header}: {value}")
            logger.info(f"Response Content Length: {len(api_response.content)} bytes")
            logger.info(f"Response Content Type: {api_response.headers.get('content-type', 'unknown')}")
            
            # Save raw response to a local file for inspection
            debug_filename = f"debug_{filename}"
            with open(debug_filename, 'wb') as f:
                f.write(api_response.content)
            logger.info(f"Saved raw response to {debug_filename}")
            
            if api_response.status_code != 200:
                logger.error(f"API error for {image_key}: {api_response.status_code}")
                logger.error(f"API response: {api_response.text}")
                return False
            
            # Check response content type
            content_type = api_response.headers.get('content-type', '').lower()
            response_data = api_response.content
            
            # Handle different response types
            if 'application/json' in content_type:
                try:
                    json_response = json.loads(response_data)
                    if isinstance(json_response, dict) and 'data' in json_response and 'file' in json_response['data']:
                        # Found base64 encoded image in JSON response
                        logger.info("Found base64 encoded image in API response")
                        try:
                            # Decode base64 image data
                            base64_image = json_response['data']['file']
                            if base64_image.startswith('data:image/'):
                                # Remove data URL prefix if present
                                base64_image = base64_image.split(',', 1)[1]
                            response_data = base64.b64decode(base64_image)
                            logger.info("Successfully decoded base64 image from API response")
                        except Exception as e:
                            logger.error(f"Failed to decode base64 image from API response: {str(e)}")
                            return False
                    else:
                        logger.error("API returned JSON but no image data found")
                        logger.error(f"JSON response: {json_response}")
                        return False
                except json.JSONDecodeError:
                    logger.error("API returned invalid JSON")
                    return False
            
            # Verify the processed image is valid
            if not response_data.startswith(b'\xff\xd8\xff'):  # JPEG magic number
                logger.error(f"Processed image for {image_key} is not a valid JPEG")
                logger.error(f"First few bytes: {response_data[:20]}")
                return False
            
            # Upload processed image back to S3
            self.s3_client.put_object(
                Bucket=self.destination_bucket,
                Key=image_key,
                Body=response_data,
                ContentType='image/jpeg'  # Explicitly set content type
            )
            
            logger.info(f"Successfully processed and uploaded image: {image_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing image {image_key}: {str(e)}")
            return False

if __name__ == "__main__":
    # Validate required environment variables
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'SOURCE_BUCKET',
        'DESTINATION_BUCKET',
        'API_USERNAME',
        'API_PASSWORD'
    ]
    
    missing_vars = [var for var in required_vars if not globals().get(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file")
        exit(1)
    
    # Get sheet path from command line argument or use default
    import sys
    sheet_path = sys.argv[1] if len(sys.argv) > 1 else 'folders.xlsx'
    
    # Create processor with batch size of 100
    processor = ImageProcessor(sheet_path=sheet_path, batch_size=100)
    processor.run() 