# S3 Image Processing Script

This script processes images stored in an S3 bucket by sending them through an API and saving the processed images back to S3.

## Features

- Processes images from S3 buckets
- Sends images to an API for processing
- Saves processed images back to S3
- Supports batch processing of multiple folders
- Maintains progress tracking
- Detailed logging and statistics

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Set up configuration:
   - Copy `config.example.py` to `config.py`
   - Fill in your credentials in `config.py`
   - DO NOT commit `config.py` to version control

5. Prepare your folder list:
   - Use the provided `create_sample_excel.py` to create a template
   - Replace the sample data in `folders_to_process.xlsx` with your folder names
   - Keep the 'folder' column name as is

## Usage

1. Run the script:
```bash
python image_processor.py
```

Or specify a custom Excel file:
```bash
python image_processor.py path/to/your/folders.xlsx
```

2. Monitor progress:
   - Check the terminal for progress bars
   - View `image_processing.log` for detailed logs
   - Check `processing_stats.json` for statistics

## File Structure

- `image_processor.py`: Main processing script
- `config.example.py`: Template for configuration
- `create_sample_excel.py`: Script to create folder list template
- `requirements.txt`: Python package dependencies
- `.gitignore`: Git ignore rules
- `README.md`: This documentation

## Notes

- The script processes folders in batches to manage memory usage
- Progress is saved after each batch
- If interrupted, the script can resume from where it left off
- Failed folders are logged and can be retried

## Security

- Never commit `config.py` containing credentials
- Keep your AWS and API credentials secure
- Use appropriate IAM roles and permissions

## Requirements

- Python 3.8+
- Required packages listed in `requirements.txt`
- AWS credentials with appropriate permissions
- API credentials for image processing 