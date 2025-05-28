import pandas as pd
import os

def create_sample_excel():
    # Sample folder data with just one folder
    data = {
        'folder': [
            '8078981654/'  # Single test folder
        ],
        'status': ['pending'],
        'notes': ['Test folder for Aadhaar processing']
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Create Excel file
    excel_path = 'folders_to_process.xlsx'
    df.to_excel(excel_path, index=False, sheet_name='Folders')
    
    # Get absolute path
    abs_path = os.path.abspath(excel_path)
    print(f"Sample Excel file created at: {abs_path}")
    print("\nFile structure:")
    print("Column 1: 'folder' - Contains folder name: 8078981654/")
    print("Column 2: 'status' - Status of processing")
    print("Column 3: 'notes' - Additional notes")
    print("\nThis is a test file with a single folder for testing the Aadhaar detection.")

if __name__ == "__main__":
    create_sample_excel() 