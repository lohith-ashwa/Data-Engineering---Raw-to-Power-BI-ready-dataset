import zipfile
import os
import duckdb
import glob

# Set up paths relative to Airflow directory
BASE_DIR = '/home/lohit/airflow'
DATA_DIR = os.path.join(BASE_DIR, 'data')
EXTRACTED_DIR = os.path.join(DATA_DIR, 'extracted_data_json')

def main():
    """Main function to extract zip file and set up database connection"""
    
    # Create DuckDB connection
    db_path = os.path.join(DATA_DIR, 'data_engineering_project.duckdb')
    con = duckdb.connect(db_path)
    
    # Extract zip file
    zip_path = os.path.join(DATA_DIR, 'all_json.zip')
    
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Zip file not found at {zip_path}")
    
    # Create extraction directory if it doesn't exist
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    
    print(f"Extracting {zip_path} to {EXTRACTED_DIR}")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTED_DIR)
    
    # Verify extraction
    extracted_files = glob.glob(os.path.join(EXTRACTED_DIR, '*.json'))
    print(f"Successfully extracted {len(extracted_files)} JSON files")
    
    con.close()
    return f"Extraction completed: {len(extracted_files)} files extracted"

if __name__ == "__main__":
    main()