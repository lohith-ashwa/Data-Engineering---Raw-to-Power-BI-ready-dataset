import json
import os
from glob import glob
import pandas as pd
from tqdm import tqdm  # For progress bars
from fuzzywuzzy import fuzz
from itertools import combinations
import numpy as np
from collections import defaultdict

# Define path to your JSON files
data_path = "C:/Users/lohit/Desktop/Radboud University/Data Engineering (NWI-IMC073-2024)/Data & Codes/extracted_data_json/*.json"

# Function to explore a single JSON file in detail
def explore_json_structure(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    print("Top-level keys:", data.keys())
    print("\nMeta structure:", data.get('meta', {}))
    print("\nInfo structure:", json.dumps(data.get('info', {}), indent=2)[:500] + "...")
    
    # Explore innings structure if available
    if 'innings' in data:
        print(f"\nNumber of innings: {len(data['innings'])}")
        if data['innings']:
            print(f"First innings team: {data['innings'][0].get('team')}")
            print(f"First innings overs: {len(data['innings'][0].get('overs', []))}")
            
            # Sample one over
            if data['innings'][0].get('overs'):
                print("\nSample delivery structure:")
                print(json.dumps(data['innings'][0]['overs'][0], indent=2))

# Get sample file
sample_files = glob(data_path)[:5]  # Just get first 5 files
if sample_files:
    explore_json_structure(sample_files[0])

def get_deep_schema_profile(file_paths):
    """Analyze schema consistency across all JSON files at multiple levels"""
    # Track variations
    top_level_schema = Counter()
    info_level_schema = Counter()
    innings_level_schema = Counter()
    over_level_schema = Counter()
    delivery_level_schema = Counter()
    
    # Track match types
    match_types = Counter()
    
    # Error tracking
    errors = []
    
    # Process files
    total_files = len(file_paths)
    print(f"Analyzing {total_files} JSON files...")
    
    for i, file_path in enumerate(file_paths):
        # Print progress for large datasets
        if i % 1000 == 0 and i > 0:
            print(f"Processed {i}/{total_files} files...")
            
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
            
            # Track innings structure if present
            if 'innings' in data and data['innings']:
                for innings in data['innings']:
                    innings_level_schema[tuple(sorted(innings.keys()))] += 1
                    
                    # Track over structure
                    if 'overs' in innings and innings['overs']:
                        for over in innings['overs']:
                            over_level_schema[tuple(sorted(over.keys()))] += 1
                            
                            # Track delivery structure
                            if 'deliveries' in over and over['deliveries']:
                                for delivery in over['deliveries']:
                                    delivery_level_schema[tuple(sorted(delivery.keys()))] += 1
                
        except Exception as e:
            errors.append((file_path, str(e)))
    
    return {
        'total_files': total_files,
        'error_count': len(errors),
        'errors': errors[:10],  # First 10 errors only
        'top_level_variations': dict(top_level_schema),
        'info_level_variations': dict(info_level_schema),
        'innings_level_variations': dict(innings_level_schema),
        'over_level_variations': dict(over_level_schema),
        'delivery_level_variations': dict(delivery_level_schema),
        'match_types': dict(match_types)
    }

# Get all JSON files
all_files = glob(data_path)
schema_profile = get_deep_schema_profile(all_files)

# Print summary results
print(f"\nSchema analysis complete for {schema_profile['total_files']} files:")
print(f"Files with errors: {schema_profile['error_count']}")

print(f"\nTop-level schema variations: {len(schema_profile['top_level_variations'])}")
for schema, count in schema_profile['top_level_variations'].items():
    print(f"  {schema}: {count} files")

print(f"\nInfo-level schema variations: {len(schema_profile['info_level_variations'])}")
if len(schema_profile['info_level_variations']) <= 5:
    for schema, count in schema_profile['info_level_variations'].items():
        print(f"  {schema}: {count} files")
else:
    print(f"  Top 5 variations:")
    for schema, count in sorted(schema_profile['info_level_variations'].items(), 
                               key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {schema}: {count} files")

print(f"\nMatch types found:")
for match_type, count in schema_profile['match_types'].items():
    print(f"  {match_type}: {count} files")

print(f"\nInnings-level schema variations: {len(schema_profile['innings_level_variations'])}")
print(f"Over-level schema variations: {len(schema_profile['over_level_variations'])}")
print(f"Delivery-level schema variations: {len(schema_profile['delivery_level_variations'])}")

# Optional: Save detailed results to file for later analysis
import pickle
with open('schema_analysis_results.pkl', 'wb') as f:
    pickle.dump(schema_profile, f)

# Create a more detailed analysis of the delivery structure since it's the most important
if schema_profile['delivery_level_variations']:
    print("\nAnalyzing delivery structure variations in more detail...")
    
    # Count the frequency of each field in deliveries
    delivery_fields = Counter()
    for schema, count in schema_profile['delivery_level_variations'].items():
        for field in schema:
            delivery_fields[field] += count
            
    print("\nDelivery fields frequency:")
    for field, count in delivery_fields.most_common():
        percentage = (count / schema_profile['total_files']) * 100
        print(f"  {field}: {count} files ({percentage:.1f}%)")
    
    # Identify required vs optional fields
    required_threshold = 0.95  # Fields present in 95% of files are considered required
    required_fields = []
    optional_fields = []
    
    for field, count in delivery_fields.items():
        percentage = count / schema_profile['total_files']
        if percentage >= required_threshold:
            required_fields.append(field)
        else:
            optional_fields.append((field, percentage))
    
    print(f"\nRequired delivery fields (present in ≥95% of files):")
    for field in required_fields:
        print(f"  {field}")
        
    print(f"\nOptional delivery fields (with presence percentage):")
    for field, percentage in sorted(optional_fields, key=lambda x: x[1], reverse=True):
        print(f"  {field}: {percentage*100:.1f}%")



all_files = glob(data_path)

print(f"Total match files found: {len(all_files)}")

def extract_match_metadata(file_path):
    """Extract comprehensive metadata from a match file"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Get match ID from filename
        match_id = os.path.basename(file_path).split('.')[0]
        
        # Extract meta information
        meta = data.get('meta', {})
        info = data.get('info', {})
        
        # Extract basic metadata
        metadata = {
            'match_id': match_id,
            'data_version': meta.get('data_version'),
            'created_date': meta.get('created'),
            'revision': meta.get('revision'),
            
            # Match information
            'match_type': info.get('match_type'),
            'match_type_number': info.get('match_type_number'),
            'gender': info.get('gender'),
            'teams': '|'.join(info.get('teams', [])),
            'team_type': info.get('team_type'),
            'overs': info.get('overs'),
            'balls_per_over': info.get('balls_per_over'),
            
            # Date and location
            'date': info.get('dates', [None])[0] if info.get('dates') else None,
            'city': info.get('city'),
            'venue': info.get('venue'),
            'season': info.get('season'),
            
            # Event information
            'event_name': info.get('event', {}).get('name') if isinstance(info.get('event'), dict) else info.get('event'),
            'event_group': info.get('event', {}).get('group') if isinstance(info.get('event'), dict) else None,
            
            # Officials
            'umpires': '|'.join(info.get('officials', {}).get('umpires', [])) if 'officials' in info else None,
            'referees': '|'.join(info.get('officials', {}).get('referees', [])) if 'officials' in info and 'referees' in info['officials'] else None,
            
            # Toss information
            'toss_winner': info.get('toss', {}).get('winner') if 'toss' in info else None,
            'toss_decision': info.get('toss', {}).get('decision') if 'toss' in info else None,
            
            # Outcome information
            'outcome_winner': info.get('outcome', {}).get('winner') if 'outcome' in info and 'winner' in info['outcome'] else None,
            'outcome_result': info.get('outcome', {}).get('result') if 'outcome' in info and 'result' in info['outcome'] else None,
            'outcome_method': info.get('outcome', {}).get('method') if 'outcome' in info and 'method' in info['outcome'] else None,
            'outcome_by_runs': info.get('outcome', {}).get('by', {}).get('runs') if 'outcome' in info and 'by' in info['outcome'] and 'runs' in info['outcome']['by'] else None,
            'outcome_by_wickets': info.get('outcome', {}).get('by', {}).get('wickets') if 'outcome' in info and 'by' in info['outcome'] and 'wickets' in info['outcome']['by'] else None,
            
            # Player information
            'player_of_match': '|'.join(info.get('player_of_match', [])),
            'team1_players': len(info.get('players', {}).get(info.get('teams', [''])[0], [])) if info.get('teams') and info.get('players') else 0,
            'team2_players': len(info.get('players', {}).get(info.get('teams', ['', ''])[1], [])) if len(info.get('teams', [])) > 1 and info.get('players') else 0,
            'registry_count': len(info.get('registry', {}).get('people', {})) if 'registry' in info else 0,
            
            # Data completeness
            'missing_fields': '|'.join(info.get('missing', [])),
            'has_innings_data': 'innings' in data and len(data['innings']) > 0,
            'innings_count': len(data.get('innings', [])),
            'total_overs_recorded': sum(len(innings.get('overs', [])) for innings in data.get('innings', [])),
            'total_deliveries': sum(sum(len(over.get('deliveries', [])) for over in innings.get('overs', [])) 
                                   for innings in data.get('innings', []))
        }
                
        return metadata
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return {'match_id': os.path.basename(file_path).split('.')[0], 'error': str(e)}

# Process files in batches to manage memory usage
def process_files_in_batches(file_paths, batch_size=1000):
    """Process files in batches to manage memory usage"""
    num_batches = (len(file_paths) + batch_size - 1) // batch_size
    all_metadata = []
    
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(file_paths))
        batch_files = file_paths[start_idx:end_idx]
        
        print(f"Processing batch {batch_idx+1}/{num_batches} (files {start_idx+1}-{end_idx})")
        
        batch_metadata = []
        for file_path in tqdm(batch_files, desc=f"Batch {batch_idx+1}"):
            metadata = extract_match_metadata(file_path)
            batch_metadata.append(metadata)
        
        all_metadata.extend(batch_metadata)
        
        # Optional: Save intermediate results
        if batch_idx % 5 == 0:
            temp_df = pd.DataFrame(all_metadata)
            temp_df.to_csv(f'cricket_metadata_partial_{end_idx}.csv', index=False)
            print(f"Saved intermediate results up to file {end_idx}")
    
    return pd.DataFrame(all_metadata)

# Process all files in batches
match_metadata_df = process_files_in_batches(all_files, batch_size=1000)
print(f"Created metadata for {len(match_metadata_df)} matches")

# Save complete results
match_metadata_df.to_csv('cricket_match_metadata_complete.csv', index=False)

# Display basic statistics about the metadata
print("\nBasic statistics about the metadata:")
print(match_metadata_df.describe(include='all').transpose()[['count', 'unique', 'top', 'freq', 'mean', 'std', 'min', 'max']])

# Check for NULL values and completeness
null_counts = match_metadata_df.isnull().sum()
null_percentages = null_counts / len(match_metadata_df) * 100
null_analysis = pd.DataFrame({'null_count': null_counts, 'null_percentage': null_percentages})
print("\nNULL value analysis:")
print(null_analysis[null_analysis['null_count'] > 0].sort_values('null_count', ascending=False))

# Check for empty matches (no innings data)
empty_matches = match_metadata_df[match_metadata_df['has_innings_data'] == False]
print(f"\nMatches without innings data: {len(empty_matches)}")

# Analyze completeness by match type
match_type_analysis = match_metadata_df.groupby('match_type').agg({
    'match_id': 'count',
    'total_deliveries': 'sum',
    'innings_count': 'mean',
    'city': lambda x: x.isnull().mean() * 100,
    'venue': lambda x: x.isnull().mean() * 100,
    'outcome_winner': lambda x: x.notnull().mean() * 100,
    'outcome_result': lambda x: x.notnull().mean() * 100,
    'player_of_match': lambda x: (x != '').mean() * 100,
    'missing_fields': lambda x: (x != '').mean() * 100
}).reset_index()

match_type_analysis.columns = ['match_type', 'count', 'total_deliveries', 'avg_innings', 
                              'pct_null_city', 'pct_null_venue', 'pct_with_winner', 
                              'pct_with_result', 'pct_with_pom', 'pct_with_missing_fields']

print("\nQuality analysis by match type:")
print(match_type_analysis)

# Analyze date distribution
match_metadata_df['date'] = pd.to_datetime(match_metadata_df['date'], errors='coerce')
date_distribution = match_metadata_df.groupby(match_metadata_df['date'].dt.year).size()
print("\nMatches by year:")
print(date_distribution)

# Analyze venue consistency
venue_corrections = {}
top_venues = match_metadata_df['venue'].value_counts().head(20)
print("\nTop 20 venues:")
print(top_venues)

# Check for similar venue names (potential inconsistencies)
def find_similar_venues(venues, threshold=90):
    similar_pairs = []
    venue_list = list(venues)
    
    # Process in smaller chunks to avoid memory issues
    chunk_size = 50
    for i in range(0, len(venue_list), chunk_size):
        chunk = venue_list[i:i+chunk_size]
        for venue1, venue2 in combinations(chunk, 2):
            similarity = fuzz.ratio(str(venue1).lower(), str(venue2).lower())
            if similarity >= threshold:
                similar_pairs.append((venue1, venue2, similarity))
        
        # Also compare with venues from other chunks
        if i > 0:
            for venue1 in chunk:
                for j in range(0, i, chunk_size):
                    other_chunk = venue_list[j:j+chunk_size]
                    for venue2 in other_chunk:
                        similarity = fuzz.ratio(str(venue1).lower(), str(venue2).lower())
                        if similarity >= threshold:
                            similar_pairs.append((venue1, venue2, similarity))
    
    return pd.DataFrame(similar_pairs, columns=['venue1', 'venue2', 'similarity']).sort_values('similarity', ascending=False)

# Get all unique venues
all_venues = match_metadata_df['venue'].dropna().unique()
print(f"Total unique venues: {len(all_venues)}")

# Find similar venues in chunks to manage memory
similar_venues = find_similar_venues(all_venues[:200])  # Start with top 200 venues
print("\nPotentially similar venue names:")
print(similar_venues.head(20))

# Save similar venues for later reference
similar_venues.to_csv('similar_venues.csv', index=False)

# Additional analysis: Player registry completeness
player_registry_counts = match_metadata_df['registry_count']
print("\nPlayer registry statistics:")
print(f"Average players per match: {player_registry_counts.mean():.2f}")
print(f"Minimum players: {player_registry_counts.min()}")
print(f"Maximum players: {player_registry_counts.max()}")

# Team name variations
team_variations = match_metadata_df['teams'].value_counts().head(20)
print("\nTop 20 team combinations:")
print(team_variations)

# Identify matches with potential data quality issues
quality_issues = match_metadata_df[
    (match_metadata_df['total_deliveries'] < 10) |  # Too few deliveries
    (match_metadata_df['team1_players'] < 11) |     # Too few players
    (match_metadata_df['team2_players'] < 11) |     # Too few players
    (match_metadata_df['registry_count'] < 22)      # Incomplete registry
]

print(f"\nMatches with potential quality issues: {len(quality_issues)}")
if len(quality_issues) > 0:
    print(quality_issues[['match_id', 'match_type', 'total_deliveries', 'team1_players', 'team2_players', 'registry_count']].head(10))

# Save results of quality issues for further investigation
quality_issues.to_csv('matches_with_quality_issues.csv', index=False)


# Function to check types in a sample JSON file
def check_data_types(file_path):
    """Examine data types in a sample JSON file without transforming anything"""
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    type_analysis = {
        'info_fields': {},
        'delivery_fields': {}
    }
    
    # Check info section data types
    info = data.get('info', {})
    for key, value in info.items():
        type_analysis['info_fields'][key] = type(value).__name__
    
    # Check delivery data types (from first over of first innings)
    if 'innings' in data and data['innings'] and 'overs' in data['innings'][0]:
        overs = data['innings'][0]['overs']
        if overs and 'deliveries' in overs[0]:
            delivery = overs[0]['deliveries'][0]
            for key, value in delivery.items():
                type_analysis['delivery_fields'][key] = type(value).__name__
                # Also check nested fields like 'runs'
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        type_analysis['delivery_fields'][f"{key}.{subkey}"] = type(subvalue).__name__
    
    return type_analysis

# Analyze a few sample files
sample_files = glob(data_path)[:5]  # Just analyze a few files

type_results = {}
for file in sample_files:
    type_results[file] = check_data_types(file)

# Summarize the results
print("Data type analysis (no transformations performed):")
for file, analysis in type_results.items():
    print(f"\nFile: {file}")
    print("Info field types:")
    for field, field_type in analysis['info_fields'].items():
        print(f"  {field}: {field_type}")
    
    print("\nDelivery field types:")
    for field, field_type in analysis['delivery_fields'].items():
        print(f"  {field}: {field_type}")


data_dir = data_path = "C:/Users/lohit/Desktop/Radboud University/Data Engineering (NWI-IMC073-2024)/Data & Codes/extracted_data_json"

# Stats collection
runs_per_delivery = []
extras_per_delivery = []
total_runs_per_delivery = []
invalid_runs = []

# Process each JSON file
file_count = 0
delivery_count = 0

# Process each file
for filename in os.listdir(data_dir):
    if filename.endswith('.json'):
        file_path = os.path.join(data_dir, filename)
        file_count += 1
        
        # Progress tracking (optional)
        if file_count % 1000 == 0:
            print(f"Processed {file_count} files...")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                match_data = json.load(f)
            
            # Process each innings
            if 'innings' in match_data:
                for innings in match_data['innings']:
                    if 'overs' in innings:
                        for over in innings['overs']:
                            if 'deliveries' in over:
                                for delivery in over['deliveries']:
                                    delivery_count += 1
                                    
                                    # Extract runs data
                                    if 'runs' in delivery:
                                        runs = delivery['runs']
                                        
                                        # Collect statistics
                                        batter_runs = runs.get('batter', 0)
                                        extras = runs.get('extras', 0)
                                        total = runs.get('total', 0)
                                        
                                        runs_per_delivery.append(batter_runs)
                                        extras_per_delivery.append(extras)
                                        total_runs_per_delivery.append(total)
                                        
                                        # Check for potential outliers or invalid data
                                        if total > 7:  # Allowing for 6 + no ball
                                            invalid_runs.append({
                                                'file': filename,
                                                'batter': delivery.get('batter', 'unknown'),
                                                'bowler': delivery.get('bowler', 'unknown'),
                                                'batter_runs': batter_runs,
                                                'extras': extras,
                                                'total': total
                                            })
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

# Convert to pandas DataFrames for analysis
runs_df = pd.DataFrame({
    'batter_runs': runs_per_delivery,
    'extras': extras_per_delivery,
    'total_runs': total_runs_per_delivery
})

# Generate summary statistics
summary_stats = runs_df.describe()
print("\nSummary Statistics:")
print(summary_stats)

print(f"\nTotal files processed: {file_count}")
print(f"Total deliveries analyzed: {delivery_count}")
print(f"Number of potentially invalid run values (>7): {len(invalid_runs)}")

# Show sample of invalid runs if any
if invalid_runs:
    print("\nSample of potentially invalid runs:")
    for i, item in enumerate(invalid_runs[:10]):
        print(item)
        if i >= 9:
            break

# Distribution analysis
print("\nRun Distribution (% of deliveries):")
for run_value in range(8):
    percentage = (runs_df['total_runs'] == run_value).mean() * 100
    print(f"{run_value} runs: {percentage:.2f}%")

# Stats collection
deliveries_per_over = defaultdict(list)
outlier_overs = []

# Process each JSON file
file_count = 0
over_count = 0

# Process each file
for filename in os.listdir(data_dir):
    if filename.endswith('.json'):
        file_path = os.path.join(data_dir, filename)
        file_count += 1
        
        # Progress tracking (optional)
        if file_count % 1000 == 0:
            print(f"Processed {file_count} files...")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                match_data = json.load(f)
            
            # Get match type (T20, ODI, Test, etc.)
            match_type = match_data.get('info', {}).get('match_type', 'unknown')
            balls_per_over = match_data.get('info', {}).get('balls_per_over', 6)
            
            # Process each innings
            if 'innings' in match_data:
                for innings in match_data['innings']:
                    if 'overs' in innings:
                        for over in innings['overs']:
                            over_count += 1
                            over_num = over.get('over', -1)
                            
                            # Count deliveries in this over
                            if 'deliveries' in over:
                                num_deliveries = len(over['deliveries'])
                                deliveries_per_over[match_type].append(num_deliveries)
                                
                                # Check for potential outliers
                                if num_deliveries > balls_per_over + 2:  # Allowing for extras
                                    outlier_overs.append({
                                        'file': filename,
                                        'match_type': match_type,
                                        'over_num': over_num,
                                        'deliveries': num_deliveries,
                                        'expected': balls_per_over
                                    })
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

# Generate summary statistics for each match type
stats_by_match_type = {}
for match_type, deliveries in deliveries_per_over.items():
    stats_by_match_type[match_type] = {
        'count': len(deliveries),
        'mean': np.mean(deliveries),
        'std': np.std(deliveries),
        'min': min(deliveries),
        'max': max(deliveries),
    }

print("\nSummary Statistics by Match Type:")
for match_type, stats in stats_by_match_type.items():
    print(f"\n{match_type} matches:")
    print(f"  Number of overs: {stats['count']}")
    print(f"  Average deliveries per over: {stats['mean']:.2f}")
    print(f"  Min deliveries: {stats['min']}")
    print(f"  Max deliveries: {stats['max']}")

print(f"\nTotal files processed: {file_count}")
print(f"Total overs analyzed: {over_count}")
print(f"Number of potentially outlier overs: {len(outlier_overs)}")

# Show sample of outlier overs if any
if outlier_overs:
    print("\nSample of potentially outlier overs:")
    for i, item in enumerate(outlier_overs[:10]):
        print(item)
        if i >= 9:
            break





