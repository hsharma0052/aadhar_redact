import pandas as pd
import os

def create_sample_excel():
    # Sample folder data
    data = {
        'folder': [
            '8078981654/',
            '8078981655/',
            '8078981656/',
            '8078981657/',
            '8078981658/'
        ],
        'status': ['pending'] * 5,  # Optional status column
        'notes': [''] * 5  # Optional notes column
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
    print("Column 1: 'folder' - Contains folder names (with trailing slash)")
    print("Column 2: 'status' - Optional status column")
    print("Column 3: 'notes' - Optional notes column")
    print("\nYou can replace this file with your own Excel file containing folder names.")
    print("Make sure to keep the 'folder' column name as is.")

if __name__ == "__main__":
    create_sample_excel() 