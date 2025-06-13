import zipfile
import os
import duckdb
import glob

con = duckdb.connect('data_engineering_project.duckdb')


#Extract zip file
zip_path = 'all_json.zip'
extract_path = './extracted_data_json'

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)