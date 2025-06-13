import json
import os
from glob import glob
import pandas as pd
from collections import defaultdict, Counter
import numpy as np

# Set up paths relative to Airflow directory
BASE_DIR = '/home/lohit/airflow'
DATA_DIR = os.path.join(BASE_DIR, 'data')
EXTRACTED_DIR = os.path.join(DATA_DIR, 'extracted_data_json')

def explore_json_structure(file_path):
    """Explore structure of a single JSON file"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        structure_info = {
            'top_level_keys': list(data.keys()),
            'meta_structure': data.get('meta', {}),
            'has_innings': 'innings' in data,
            'innings_count': len(data.get('innings', [])) if 'innings' in data else 0
        }
        
        if 'innings' in data and data['innings']:
            structure_info['first_innings_team'] = data['innings'][0].get('team')
            structure_info['first_innings_overs'] = len(data['innings'][0].get('overs', []))
        
        return structure_info
    except Exception as e:
        print(f"Error exploring {file_path}: {e}")
        return None

def get_deep_schema_profile(file_paths, max_files=1000):
    """Analyze schema consistency across JSON files"""
    # Limit files for Airflow performance
    files_to_process = file_paths[:max_files] if len(file_paths) > max_files else file_paths
    
    top_level_schema = Counter()
    info_level_schema = Counter()
    match_types = Counter()
    errors = []
    
    print(f"Analyzing {len(files_to_process)} JSON files...")
    
    for i, file_path in enumerate(files_to_process):
        if i % 100 == 0 and i > 0:
            print(f"Processed {i}/{len(files_to_process)} files...")
            
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Track top-level keys
            top_level_schema[tuple(sorted(data.keys()))] += 1
            
            # Track info level keys if present
            if 'info' in data:
                info_level_schema[tuple(sorted(data['info'].keys()))] += 1
                
                # Track match type
                if 'match_type' in data['info']:
                    match_types[data['info']['match_type']] += 1
                
        except Exception as e:
            errors.append((file_path, str(e)))
    
    return {
        'total_files': len(files_to_process),
        'error_count': len(errors),
        'errors': errors[:10],
        'top_level_variations': dict(top_level_schema),
        'info_level_variations': dict(info_level_schema),
        'match_types': dict(match_types)
    }

def extract_match_metadata(file_paths, max_files=500):
    """Extract metadata from match files"""
    files_to_process = file_paths[:max_files] if len(file_paths) > max_files else file_paths
    
    metadata_list = []
    
    for file_path in files_to_process:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            match_id = os.path.basename(file_path).split('.')[0]
            meta = data.get('meta', {})
            info = data.get('info', {})
            
            metadata = {
                'match_id': match_id,
                'match_type': info.get('match_type'),
                'teams': '|'.join(info.get('teams', [])),
                'date': info.get('dates', [None])[0] if info.get('dates') else None,
                'city': info.get('city'),
                'venue': info.get('venue'),
                'has_innings_data': 'innings' in data and len(data['innings']) > 0,
                'innings_count': len(data.get('innings', [])),
                'total_deliveries': sum(sum(len(over.get('deliveries', [])) for over in innings.get('overs', [])) 
                                       for innings in data.get('innings', []))
            }
            
            metadata_list.append(metadata)
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    return pd.DataFrame(metadata_list)

def analyze_runs_and_overs(data_dir, max_files=500):
    """Analyze runs and overs distribution"""
    json_files = glob(os.path.join(data_dir, '*.json'))[:max_files]
    
    runs_per_delivery = []
    deliveries_per_over = defaultdict(list)
    
    for filename in json_files:
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                match_data = json.load(f)
            
            match_type = match_data.get('info', {}).get('match_type', 'unknown')
            
            if 'innings' in match_data:
                for innings in match_data['innings']:
                    if 'overs' in innings:
                        for over in innings['overs']:
                            if 'deliveries' in over:
                                # Count deliveries per over
                                num_deliveries = len(over['deliveries'])
                                deliveries_per_over[match_type].append(num_deliveries)
                                
                                # Analyze runs per delivery
                                for delivery in over['deliveries']:
                                    if 'runs' in delivery:
                                        total_runs = delivery['runs'].get('total', 0)
                                        runs_per_delivery.append(total_runs)
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    return {
        'runs_distribution': Counter(runs_per_delivery),
        'overs_by_match_type': dict(deliveries_per_over)
    }

def main():
    """Main function for quality assessment"""
    print("Starting pre-wrangling quality assessment...")
    
    # Check if extracted data exists
    if not os.path.exists(EXTRACTED_DIR):
        raise FileNotFoundError(f"Extracted data directory not found: {EXTRACTED_DIR}")
    
    # Get all JSON files
    json_pattern = os.path.join(EXTRACTED_DIR, '*.json')
    all_files = glob(json_pattern)
    
    if not all_files:
        raise FileNotFoundError(f"No JSON files found in {EXTRACTED_DIR}")
    
    print(f"Found {len(all_files)} JSON files")
    
    # Explore sample file structure
    if all_files:
        sample_structure = explore_json_structure(all_files[0])
        print(f"Sample file structure: {sample_structure}")
    
    # Get schema profile
    schema_profile = get_deep_schema_profile(all_files)
    print(f"Schema analysis complete:")
    print(f"- Files processed: {schema_profile['total_files']}")
    print(f"- Files with errors: {schema_profile['error_count']}")
    print(f"- Top-level schema variations: {len(schema_profile['top_level_variations'])}")
    print(f"- Match types found: {list(schema_profile['match_types'].keys())}")
    
    # Extract metadata
    metadata_df = extract_match_metadata(all_files)
    print(f"Extracted metadata for {len(metadata_df)} matches")
    
    # Save metadata
    metadata_path = os.path.join(DATA_DIR, 'cricket_match_metadata.csv')
    metadata_df.to_csv(metadata_path, index=False)
    print(f"Saved metadata to {metadata_path}")
    
    # Analyze runs and overs
    analysis_results = analyze_runs_and_overs(EXTRACTED_DIR)
    print(f"Runs distribution analysis complete:")
    print(f"- Most common runs per delivery: {analysis_results['runs_distribution'].most_common(5)}")
    
    # Summary statistics
    summary = {
        'total_files': len(all_files),
        'files_processed': schema_profile['total_files'],
        'match_types': len(schema_profile['match_types']),
        'matches_with_data': len(metadata_df),
        'matches_with_innings': len(metadata_df[metadata_df['has_innings_data'] == True])
    }
    
    print("Quality assessment summary:")
    for key, value in summary.items():
        print(f"- {key}: {value}")
    
    return summary

if __name__ == "__main__":
    main()