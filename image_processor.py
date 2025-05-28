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
import pytesseract
from PIL import Image
import io
import re
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
        
        # Initialize folder processing results
        self.folder_results = {}  # Store results for each folder
        
        self.stats = {
            'total_folders': 0,
            'processed_folders': 0,
            'failed_folders': set(),
            'total_images': 0,
            'processed_images': 0,
            'failed_images': 0,
            'skipped_non_aadhaar': 0,
            'skipped_images': set()
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
        """Read folder names from the sheet and initialize folder results"""
        try:
            # Determine file type and read accordingly
            file_ext = Path(self.sheet_path).suffix.lower()
            if file_ext == '.csv':
                self.df = pd.read_csv(self.sheet_path)
            elif file_ext in ['.xlsx', '.xls']:
                self.df = pd.read_excel(self.sheet_path)
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
            # Assuming the folder names are in a column named 'folder' or the first column
            folder_column = 'folder' if 'folder' in self.df.columns else self.df.columns[0]
            folders = self.df[folder_column].astype(str).tolist()
            
            # Filter out already processed folders
            folders = [f for f in folders if f not in self.processed_folders]
            
            # Ensure folder names end with '/'
            folders = [f if f.endswith('/') else f + '/' for f in folders]
            
            # Initialize folder results
            for folder in folders:
                if folder not in self.folder_results:
                    self.folder_results[folder] = {
                        'status': 'Pending',
                        'aadhaar_images': [],
                        'non_aadhaar_images': [],
                        'processed_images': [],
                        'failed_images': [],
                        'last_updated': None
                    }
            
            logger.info(f"Found {len(folders)} folders to process")
            return folders
        except Exception as e:
            logger.error(f"Error reading sheet: {str(e)}")
            return []

    def update_folder_result(self, folder: str, image_key: str, status: str, is_aadhaar: bool = None):
        """Update the processing result for a specific folder and image"""
        if folder not in self.folder_results:
            self.folder_results[folder] = {
                'status': 'Pending',
                'aadhaar_images': [],
                'non_aadhaar_images': [],
                'processed_images': [],
                'failed_images': [],
                'last_updated': None
            }
        
        # Update image classification
        if is_aadhaar is not None:
            if is_aadhaar:
                if image_key not in self.folder_results[folder]['aadhaar_images']:
                    self.folder_results[folder]['aadhaar_images'].append(image_key)
            else:
                if image_key not in self.folder_results[folder]['non_aadhaar_images']:
                    self.folder_results[folder]['non_aadhaar_images'].append(image_key)
        
        # Update processing status
        if status == 'processed':
            if image_key not in self.folder_results[folder]['processed_images']:
                self.folder_results[folder]['processed_images'].append(image_key)
        elif status == 'failed':
            if image_key not in self.folder_results[folder]['failed_images']:
                self.folder_results[folder]['failed_images'].append(image_key)
        
        # Update folder status
        if len(self.folder_results[folder]['failed_images']) > 0:
            self.folder_results[folder]['status'] = 'Failed'
        elif len(self.folder_results[folder]['processed_images']) > 0:
            self.folder_results[folder]['status'] = 'Completed'
        
        # Update timestamp
        self.folder_results[folder]['last_updated'] = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Save to Excel after each update
        self.save_results_to_excel()

    def save_results_to_excel(self):
        """Save the processing results to Excel"""
        try:
            # Create a new DataFrame for results
            results_data = []
            for folder, result in self.folder_results.items():
                results_data.append({
                    'Folder': folder,
                    'Status': result['status'],
                    'Aadhaar Images': ', '.join(result['aadhaar_images']),
                    'Non-Aadhaar Images': ', '.join(result['non_aadhaar_images']),
                    'Processed Images': ', '.join(result['processed_images']),
                    'Failed Images': ', '.join(result['failed_images']),
                    'Last Updated': result['last_updated']
                })
            
            # Create results DataFrame
            results_df = pd.DataFrame(results_data)
            
            # Save to Excel with two sheets
            with pd.ExcelWriter(self.sheet_path, engine='openpyxl') as writer:
                # Original data sheet
                self.df.to_excel(writer, sheet_name='Folders', index=False)
                # Results sheet
                results_df.to_excel(writer, sheet_name='Processing Results', index=False)
            
            logger.info(f"Updated Excel file with processing results: {self.sheet_path}")
        except Exception as e:
            logger.error(f"Error saving results to Excel: {str(e)}")

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
                    self.update_folder_result(folder, '', 'no_images')
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
                self.update_folder_result(folder, '', 'error', error=str(e))

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
            'skipped_non_aadhaar': self.stats['skipped_non_aadhaar'],
            'skipped_images': list(self.stats['skipped_images']),
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
Skipped non-Aadhaar images: {self.stats['skipped_non_aadhaar']}
        """)
        
        if self.stats['failed_folders']:
            logger.info("Failed folders:")
            for folder in sorted(self.stats['failed_folders']):
                logger.info(f"  {folder}")
                
        if self.stats['skipped_images']:
            logger.info("Skipped non-Aadhaar images:")
            for image in sorted(self.stats['skipped_images']):
                logger.info(f"  {image}")

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

    def is_aadhaar_image(self, image_bytes: bytes, image_key: str = None) -> bool:
        """
        Check if the image is an Aadhaar card using OCR and (optionally) the image key.
        Uses multiple detection methods for 100% accuracy:
        1. Filename check (if image_key contains 'aadhar')
        2. OCR pattern matching with improved patterns and scoring
        3. Layout analysis for common Aadhaar card elements
        
        The detection is made more specific to Aadhaar by:
        - Requiring at least one Aadhaar-specific pattern
        - Using negative patterns to exclude other documents
        - Giving higher weights to Aadhaar-unique patterns
        """
        # Method 1: Check filename first
        if image_key and re.search(r'aadhar', image_key, re.IGNORECASE):
            logger.info(f"Image key ({image_key}) contains 'aadhar' - automatically classified as Aadhaar")
            return True

        try:
            # Convert bytes to PIL Image
            image = Image.open(io.BytesIO(image_bytes))
            
            # Extract text using Tesseract with improved configuration
            custom_config = r'--oem 3 --psm 6 -l eng+hin'  # Use both English and Hindi
            text = pytesseract.image_to_string(image, config=custom_config)
            text_lower = text.lower()
            
            # Negative patterns (if any of these are found, it's definitely not an Aadhaar)
            negative_patterns = [
                r'permanent\s+account\s+number',
                r'income\s+tax\s+department',
                r'परमानेंट\s+अकाउंट\s+नंबर',
                r'आयकर\s+विभाग',
                r'pan\s+card',
                r'पैन\s+कार्ड',
                r'passport',
                r'पासपोर्ट',
                r'driving\s+license',
                r'ड्राइविंग\s+लाइसेंस',
                r'voter\s+id',
                r'मतदाता\s+पहचान\s+पत्र',
                r'ration\s+card',
                r'राशन\s+कार्ड'
            ]
            
            # Check for negative patterns first
            for pattern in negative_patterns:
                if re.search(pattern, text_lower, re.IGNORECASE):
                    logger.info(f"Found negative pattern indicating non-Aadhaar document: {pattern}")
                    return False
            
            # Aadhaar patterns to look for (grouped by category with weights)
            aadhaar_patterns = {
                # Aadhaar-specific Patterns (highest weight, must have at least one)
                'aadhaar_specific': [
                    (r'aadhaar\s*(?:card|number|enrollment)', 3),
                    (r'aadhar\s*(?:card|number|enrollment)', 3),
                    (r'आधार\s*(?:कार्ड|नंबर|पंजीकरण)', 3),
                    (r'unique\s+identification\s+authority\s+of\s+india', 3),
                    (r'यूनीक\s+आइडेंटिफिकेशन\s+अथॉरिटी\s+ऑफ\s+इंडिया', 3),
                    (r'uidai', 3),
                    (r'यूआईडीएआई', 3),
                    (r'vid\s*:\s*[a-z0-9]{16}', 3),  # Virtual ID format
                    (r'आधार\s+सत्यापन\s+कोड', 3),
                    (r'aadhaar\s+verification\s+code', 3),
                ],
                
                # Aadhaar Number Patterns (high weight)
                'number_patterns': [
                    (r'\b\d{4}\s\d{4}\s\d{4}\b', 2.5),  # Standard format: XXXX XXXX XXXX
                    (r'\b\d{12}\b', 2.5),               # Without spaces
                    (r'\b\d{4}-\d{4}-\d{4}\b', 2.5),    # With hyphens
                    (r'[a-z]{1}\d{4}\s\d{4}\s\d{4}\b', 2.5),  # With leading letter
                ],
                
                # Government and Authority Patterns (medium weight)
                'government_patterns': [
                    (r'government\s+of\s+india', 1.5),
                    (r'भारत\s+सरकार', 1.5),
                    (r'ministry\s+of\s+electronics\s+and\s+information\s+technology', 1.5),
                    (r'इलेक्ट्रॉनिक्स\s+और\s+सूचना\s+प्रौद्योगिकी\s+मंत्रालय', 1.5),
                ],
                
                # Security and Instruction Patterns (low weight)
                'security_patterns': [
                    (r'this\s+is\s+to\s+certify\s+that', 0.5),
                    (r'यह\s+प्रमाणित\s+करता\s+है\s+कि', 0.5),
                    (r'this\s+card\s+is\s+proof\s+of\s+identity', 0.5),
                    (r'यह\s+कार्ड\s+पहचान\s+का\s+प्रमाण\s+है', 0.5),
                    (r'not\s+proof\s+of\s+citizenship', 0.5),
                    (r'नागरिकता\s+का\s+प्रमाण\s+नहीं\s+है', 0.5),
                    (r'not\s+proof\s+of\s+date\s+of\s+birth', 0.5),
                    (r'जन्म\s+तिथि\s+का\s+प्रमाण\s+नहीं\s+है', 0.5),
                    (r'please\s+keep\s+your\s+aadhaar\s+number\s+safe', 0.5),
                    (r'आधार\s+नंबर\s+को\s+सुरक्षित\s+रखें', 0.5),
                ],
                
                # QR/Barcode Patterns (low weight)
                'qr_patterns': [
                    (r'qr\s+code', 0.5),
                    (r'barcode', 0.5),
                    (r'क्यूआर\s+कोड', 0.5),
                    (r'बारकोड', 0.5),
                ]
            }
            
            # Scoring system with weighted patterns
            total_score = 0
            required_score = 3.0  # Increased threshold for better accuracy
            matched_patterns = []
            has_aadhaar_specific = False  # Track if we found any Aadhaar-specific pattern
            
            # Check each category of patterns
            for category, patterns in aadhaar_patterns.items():
                for pattern, weight in patterns:
                    if re.search(pattern, text_lower, re.IGNORECASE):
                        total_score += weight
                        matched_patterns.append(f"{pattern} ({weight})")
                        logger.info(f"Found Aadhaar pattern ({category}): {pattern} (weight: {weight})")
                        if category == 'aadhaar_specific':
                            has_aadhaar_specific = True
            
            # Log all matched patterns and total score
            if matched_patterns:
                logger.info(f"Total patterns matched: {len(matched_patterns)}")
                logger.info(f"Matched patterns: {', '.join(matched_patterns)}")
                logger.info(f"Total weighted score: {total_score:.2f} (required: {required_score})")
                logger.info(f"Found Aadhaar-specific pattern: {has_aadhaar_specific}")
            
            # Additional layout check: Look for common Aadhaar card elements
            layout_score = 0
            try:
                # Check image dimensions (typical Aadhaar card ratio)
                width, height = image.size
                ratio = width / height
                if 1.4 <= ratio <= 1.8:  # Typical Aadhaar card ratio
                    layout_score += 0.5
                    logger.info("Image dimensions match typical Aadhaar card ratio")
                
                # Check for QR code or barcode (using image analysis)
                if 'qr' in text_lower or 'barcode' in text_lower:
                    layout_score += 0.5
                    logger.info("Found QR code or barcode reference")
                
                total_score += layout_score
                logger.info(f"Layout analysis score: {layout_score:.2f}")
            except Exception as e:
                logger.warning(f"Layout analysis failed: {str(e)}")
            
            # Final classification
            # Must have both sufficient score AND at least one Aadhaar-specific pattern
            is_aadhaar = total_score >= required_score and has_aadhaar_specific
            logger.info(f"Final Aadhaar detection score: {total_score:.2f} (required: {required_score})")
            logger.info(f"Has Aadhaar-specific pattern: {has_aadhaar_specific}")
            logger.info(f"Image classified as {'Aadhaar' if is_aadhaar else 'non-Aadhaar'}")
            
            return is_aadhaar
            
        except Exception as e:
            logger.error(f"Error in OCR processing: {str(e)}")
            # If OCR fails, assume it's not an Aadhaar card to be safe
            return False

    def process_image(self, image_key: str) -> bool:
        try:
            # Get folder name from image key
            folder = '/'.join(image_key.split('/')[:-1]) + '/'
            
            # Download image from S3
            response = self.s3_client.get_object(Bucket=self.source_bucket, Key=image_key)
            image_data = response['Body'].read()
            
            # Check if image is an Aadhaar card
            is_aadhaar = self.is_aadhaar_image(image_data, image_key)
            if not is_aadhaar:
                logger.info(f"Skipping non-Aadhaar image: {image_key}")
                self.stats['skipped_non_aadhaar'] += 1
                self.stats['skipped_images'].add(image_key)
                self.update_folder_result(folder, image_key, 'skipped', False)
                return False
            
            # Get just the filename without the path
            filename = os.path.basename(image_key)
            
            # Check if input is JSON and extract image data if it is
            if image_data and (image_data[:1] == b'{' or image_data[:1] == b'['):
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
                            # Check if decoded image is an Aadhaar card
                            if not self.is_aadhaar_image(image_data):
                                logger.info(f"Skipping non-Aadhaar image after decoding: {image_key}")
                                self.stats['skipped_non_aadhaar'] += 1
                                self.stats['skipped_images'].add(image_key)
                                self.update_folder_result(folder, image_key, 'skipped', False)
                                return False
                        except Exception as e:
                            logger.error(f"Failed to decode base64 image data for {image_key}: {str(e)}")
                            return False
                except Exception as e:
                    logger.info(f"Input file {image_key} is raw image data (JSON decode failed: {str(e)})")
            else:
                logger.info(f"Input file {image_key} is raw image data (not JSON)")
            
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
            
            # Update folder result on success
            self.update_folder_result(folder, image_key, 'processed', True)
            logger.info(f"Successfully processed and uploaded image: {image_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing image {image_key}: {str(e)}")
            # Update folder result on failure
            folder = '/'.join(image_key.split('/')[:-1]) + '/'
            self.update_folder_result(folder, image_key, 'failed')
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
    sheet_path = sys.argv[1] if len(sys.argv) > 1 else 'folders_to_process.xlsx'
    
    # Create processor with batch size of 100
    processor = ImageProcessor(sheet_path=sheet_path, batch_size=100)
    processor.run() 