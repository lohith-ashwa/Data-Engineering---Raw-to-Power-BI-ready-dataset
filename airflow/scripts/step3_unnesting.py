import json
import pandas as pd
import os
import glob
import duckdb
from datetime import datetime
from collections import defaultdict, Counter

# Set up paths relative to Airflow directory
BASE_DIR = '/home/lohit/airflow'
DATA_DIR = os.path.join(BASE_DIR, 'data')
EXTRACTED_DIR = os.path.join(DATA_DIR, 'extracted_data_json')

# Global dictionaries for player tracking
player_name_to_id = {}
player_id_to_names = defaultdict(list)
next_synthetic_id = 1000000

def get_player_id(player_name, player_registry):
    """Get or create player ID with enhanced handling"""
    global next_synthetic_id
    
    if not player_name:
        return None
    
    # First check if player exists in registry
    player_id = player_registry.get(player_name, None)
    
    if player_id is not None:
        player_id_to_names[player_id].append(player_name)
        player_name_to_id[player_name] = player_id
        return player_id
    
    # If not in registry, check our global mapping
    if player_name in player_name_to_id:
        player_id = player_name_to_id[player_name]
        player_id_to_names[player_id].append(player_name)
        return player_id
    
    # Create a new synthetic ID
    player_id = f"SYNTH_{next_synthetic_id}"
    player_name_to_id[player_name] = player_id
    player_id_to_names[player_id].append(player_name)
    next_synthetic_id += 1
    
    return player_id

def extract_matches_table(data_list):
    """Extract matches table with enhanced player ID handling"""
    matches = []
    for data in data_list:
        match_info = data.get('info', {})
        match_id = data['match_id']
        
        # Extract event information 
        event_name = None
        event_id = None
        match_number = None
        
        if 'event' in match_info:
            if isinstance(match_info['event'], dict):
                event_name = match_info['event'].get('name', None)
                match_number = match_info['event'].get('match_number', None)
                event_id = match_info['event'].get('group', None)
            else:
                event_name = match_info['event']
        
        # Get player registry if available
        player_registry = {}
        if 'registry' in match_info and 'people' in match_info['registry']:
            player_registry = match_info['registry']['people']
        
        # Get player_of_match ID
        player_of_match = match_info.get('player_of_match', [None])[0]
        player_of_match_id = get_player_id(player_of_match, player_registry) if player_of_match else None
        
        # Format date
        date_str = match_info.get('dates', [None])[0]
        date = None
        if date_str:
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                date = date_str
                
        match_row = {
            'match_id': match_id,
            'date': date,
            'city': match_info.get('city', None),
            'venue': match_info.get('venue', None),
            'match_type': match_info.get('match_type', None),
            'gender': match_info.get('gender', None),
            'season': match_info.get('season', None),
            'match_event_name': event_name,
            'match_event_id': event_id,
            'match_number': match_number,
            'overs': match_info.get('overs', None),
            'team1': match_info.get('teams', [None, None])[0],
            'team2': match_info.get('teams', [None, None])[1] if len(match_info.get('teams', [])) > 1 else None,
            'toss_winner': match_info.get('toss', {}).get('winner', None),
            'toss_decision': match_info.get('toss', {}).get('decision', None),
            'outcome_winner': match_info.get('outcome', {}).get('winner', None),
            'outcome_by_runs': match_info.get('outcome', {}).get('by', {}).get('runs', None),
            'outcome_by_wickets': match_info.get('outcome', {}).get('by', {}).get('wickets', None),
            'outcome_method': match_info.get('outcome', {}).get('method', None),
            'player_of_match': player_of_match,
            'player_of_match_id': player_of_match_id
        }
        matches.append(match_row)
    
    return pd.DataFrame(matches)

def extract_innings_table(data_list):
    """Extract innings table"""
    innings_rows = []
    
    for data in data_list:
        match_id = data['match_id']
        
        if 'innings' in data:
            innings_data = data['innings']
            
            for i, inning in enumerate(innings_data):
                powerplay_start_over = None
                powerplay_end_over = None
                
                if 'powerplays' in inning:
                    for powerplay in inning['powerplays']:
                        if powerplay.get('type') == 'mandatory':
                            powerplay_start_over = powerplay.get('from')
                            powerplay_end_over = powerplay.get('to')
                
                innings_row = {
                    'innings_id': f"{match_id}_{i+1}",
                    'match_id': match_id,
                    'innings_number': i+1,
                    'batting_team': inning.get('team', None),
                    'powerplay_start_over': powerplay_start_over,
                    'powerplay_end_over': powerplay_end_over
                }
                
                # Find bowling team
                if 'info' in data and 'teams' in data['info']:
                    teams = data['info']['teams']
                    batting_team = inning.get('team', None)
                    if batting_team in teams:
                        bowling_team = [team for team in teams if team != batting_team][0] if len(teams) > 1 else None
                        innings_row['bowling_team'] = bowling_team
                    else:
                        innings_row['bowling_team'] = None
                else:
                    innings_row['bowling_team'] = None
                
                innings_rows.append(innings_row)
    
    return pd.DataFrame(innings_rows)

def extract_overs_table(data_list):
    """Extract overs table"""
    overs_rows = []
    
    for data in data_list:
        match_id = data['match_id']
        innings_data = data.get('innings', [])
        
        for i, inning in enumerate(innings_data):
            innings_id = f"{match_id}_{i+1}"
            
            if 'overs' not in inning:
                continue
                
            for over in inning['overs']:
                over_num = over['over']
                
                # Calculate statistics
                total_runs = sum(delivery['runs']['total'] for delivery in over.get('deliveries', []))
                wickets = sum(1 for delivery in over.get('deliveries', []) if 'wickets' in delivery)
                
                # Calculate extras
                total_extras = 0
                extras_wides = 0
                extras_noballs = 0
                extras_byes = 0
                extras_legbyes = 0
                
                for delivery in over.get('deliveries', []):
                    if 'extras' in delivery:
                        extras = delivery['extras']
                        total_extras += delivery['runs'].get('extras', 0)
                        
                        if 'wides' in extras:
                            extras_wides += extras['wides']
                        if 'noballs' in extras:
                            extras_noballs += extras['noballs']
                        if 'byes' in extras:
                            extras_byes += extras['byes']
                        if 'legbyes' in extras:
                            extras_legbyes += extras['legbyes']
                
                over_row = {
                    'over_id': f"{innings_id}_{over_num}",
                    'innings_id': innings_id,
                    'over_number': over_num,
                    'total_runs': total_runs,
                    'wickets': wickets,
                    'num_deliveries': len(over.get('deliveries', [])),
                    'total_extras': total_extras,
                    'extras_wides': extras_wides,
                    'extras_noballs': extras_noballs,
                    'extras_byes': extras_byes,
                    'extras_legbyes': extras_legbyes
                }
                overs_rows.append(over_row)
    
    return pd.DataFrame(overs_rows)

def extract_deliveries_table(data_list):
    """Extract deliveries table"""
    delivery_rows = []
    
    for data in data_list:
        match_id = data['match_id']
        innings_data = data.get('innings', [])
        
        # Get player registry
        player_registry = {}
        if 'info' in data and 'registry' in data['info'] and 'people' in data['info']['registry']:
            player_registry = data['info']['registry']['people']
        
        for i, inning in enumerate(innings_data):
            innings_id = f"{match_id}_{i+1}"
            
            if 'overs' not in inning:
                continue
                
            for over in inning['overs']:
                over_num = over['over']
                over_id = f"{innings_id}_{over_num}"
                
                for ball_idx, delivery in enumerate(over.get('deliveries', [])):
                    delivery_id = f"{over_id}_{ball_idx+1}"
                    batter = delivery.get('batter', None)
                    bowler = delivery.get('bowler', None)
                    non_striker = delivery.get('non_striker', None)
                    
                    # Get player IDs
                    batter_id = get_player_id(batter, player_registry)
                    bowler_id = get_player_id(bowler, player_registry)
                    non_striker_id = get_player_id(non_striker, player_registry)
                    
                    # Runs information
                    if 'runs' in delivery:
                        batter_runs = delivery['runs'].get('batter', 0)
                        extras = delivery['runs'].get('extras', 0)
                        total_runs = delivery['runs'].get('total', 0)
                    else:
                        batter_runs = extras = total_runs = 0
                    
                    # Extra details
                    extras_type = None
                    extras_value = 0
                    if 'extras' in delivery:
                        extras_type = list(delivery['extras'].keys())[0] if delivery['extras'] else None
                        extras_value = list(delivery['extras'].values())[0] if delivery['extras'] else 0
                    
                    # Wicket information
                    is_wicket = 1 if 'wickets' in delivery else 0
                    wicket_player_out = None
                    wicket_kind = None
                    wicket_fielder = None
                    wicket_player_out_id = None
                    wicket_fielder_id = None
                    
                    if is_wicket:
                        wicket = delivery['wickets'][0]
                        wicket_player_out = wicket.get('player_out', None)
                        wicket_player_out_id = get_player_id(wicket_player_out, player_registry)
                        wicket_kind = wicket.get('kind', None)
                        if 'fielders' in wicket and wicket['fielders']:
                            wicket_fielder = wicket['fielders'][0].get('name', None)
                            wicket_fielder_id = get_player_id(wicket_fielder, player_registry)
                    
                    delivery_row = {
                        'delivery_id': delivery_id,
                        'over_id': over_id,
                        'innings_id': innings_id,
                        'match_id': match_id,
                        'over_number': over_num,
                        'ball_number': ball_idx + 1,
                        'batter': batter,
                        'batter_id': batter_id,
                        'bowler': bowler,
                        'bowler_id': bowler_id,
                        'non_striker': non_striker,
                        'non_striker_id': non_striker_id,
                        'batter_runs': batter_runs,
                        'extras': extras,
                        'total_runs': total_runs,
                        'extras_type': extras_type,
                        'extras_value': extras_value,
                        'is_wicket': is_wicket,
                        'wicket_player_out': wicket_player_out,
                        'wicket_player_out_id': wicket_player_out_id,
                        'wicket_kind': wicket_kind,
                        'wicket_fielder': wicket_fielder,
                        'wicket_fielder_id': wicket_fielder_id
                    }
                    delivery_rows.append(delivery_row)
    
    return pd.DataFrame(delivery_rows)

def extract_players_table(data_list):
    """Extract players table with name variations"""
    # First, gather all player name variations from registry
    for data in data_list:
        if 'info' in data and 'registry' in data['info'] and 'people' in data['info']['registry']:
            registry = data['info']['registry']['people']
            for player_name, player_id in registry.items():
                player_name_to_id[player_name] = player_id
                player_id_to_names[player_id].append(player_name)
    
    # Create players table
    players_list = []
    for player_id, name_variations in player_id_to_names.items():
        name_counts = Counter(name_variations)
        primary_name = name_counts.most_common(1)[0][0]
        alt_names = [name for name, _ in name_counts.most_common() if name != primary_name]
        
        player_row = {
            'player_id': player_id,
            'player_name': primary_name,
            'name_variations': ';'.join(alt_names) if alt_names else None,
            'variant_count': len(alt_names) + 1
        }
        players_list.append(player_row)
    
    return pd.DataFrame(players_list)

def process_cricket_json_in_batches(directory_path, batch_size=1000):
    """Process JSON files in smaller batches to reduce memory usage"""
    json_files = glob.glob(os.path.join(directory_path, '*.json'))
    total_files = len(json_files)
    
    print(f"Processing {total_files} JSON files in batches of {batch_size}...")
    
    # Initialize combined DataFrames
    all_matches = []
    all_players = []
    all_innings = []
    all_overs = []
    all_deliveries = []
    
    # Process files in batches
    for batch_start in range(0, total_files, batch_size):
        batch_end = min(batch_start + batch_size, total_files)
        batch_files = json_files[batch_start:batch_end]
        
        print(f"Processing batch {batch_start//batch_size + 1}: files {batch_start+1}-{batch_end}")
        
        # Load batch data
        batch_data = []
        for json_file in batch_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    data['match_id'] = os.path.basename(json_file).split('.')[0]
                    batch_data.append(data)
            except Exception as e:
                print(f"Error processing {json_file}: {e}")
        
        if not batch_data:
            continue
        
        # Extract tables for this batch
        try:
            batch_matches = extract_matches_table(batch_data)
            batch_innings = extract_innings_table(batch_data)
            batch_overs = extract_overs_table(batch_data)
            batch_deliveries = extract_deliveries_table(batch_data)
            
            # Append to combined lists
            all_matches.append(batch_matches)
            all_innings.append(batch_innings)
            all_overs.append(batch_overs)
            all_deliveries.append(batch_deliveries)
            
            print(f"Batch {batch_start//batch_size + 1} completed: {len(batch_data)} files processed")
            
        except Exception as e:
            print(f"Error processing batch {batch_start//batch_size + 1}: {e}")
        
        # Clear batch data to free memory
        del batch_data
        
        # Memory management: collect garbage every few batches
        if (batch_start // batch_size + 1) % 5 == 0:
            import gc
            gc.collect()
    
    # Combine all batches
    print("Combining all batches...")
    
    tables = {}
    
    if all_matches:
        tables['matches'] = pd.concat(all_matches, ignore_index=True)
        print(f"Combined matches table: {len(tables['matches'])} rows")
    
    if all_innings:
        tables['innings'] = pd.concat(all_innings, ignore_index=True)
        print(f"Combined innings table: {len(tables['innings'])} rows")
    
    if all_overs:
        tables['overs'] = pd.concat(all_overs, ignore_index=True)
        print(f"Combined overs table: {len(tables['overs'])} rows")
    
    if all_deliveries:
        tables['deliveries'] = pd.concat(all_deliveries, ignore_index=True)
        print(f"Combined deliveries table: {len(tables['deliveries'])} rows")
    
    # Extract players table (only needs to be done once)
    # Use a sample of the data for player extraction to save memory
    sample_files = json_files[:min(5000, len(json_files))]  # Use first 5000 files for player registry
    sample_data = []
    for json_file in sample_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                data['match_id'] = os.path.basename(json_file).split('.')[0]
                sample_data.append(data)
        except:
            continue
    
    if sample_data:
        tables['players'] = extract_players_table(sample_data)
        print(f"Extracted players table: {len(tables['players'])} rows")
    
    return tables

def create_database_with_indexes(data_directory, db_name='cricket_analytics.db'):
    """Create database with indexes using batch processing"""
    cricket_data = process_cricket_json_in_batches(data_directory, batch_size=500)  # Smaller batches
    
    db_path = os.path.join(DATA_DIR, db_name)
    conn = duckdb.connect(db_path)
    
    # Create tables and indexes
    for table_name, df in cricket_data.items():
        if df is not None and not df.empty:
            df = df.drop_duplicates()
            
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            print(f"Created table: {table_name}")
            
            # Create indexes
            if table_name == 'matches':
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(match_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_type ON {table_name}(match_type)")
            
            elif table_name == 'players':
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(player_id)")
            
            elif table_name == 'innings':
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(innings_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_match ON {table_name}(match_id)")
            
            elif table_name == 'overs':
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(over_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_innings ON {table_name}(innings_id)")
            
            elif table_name == 'deliveries':
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(delivery_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_over ON {table_name}(over_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_innings ON {table_name}(innings_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_match ON {table_name}(match_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_batter ON {table_name}(batter_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_bowler ON {table_name}(bowler_id)")
    
    print(f"Successfully created database at {db_path}")
    conn.close()

def main():
    """Main function"""
    print("Starting data unnesting and database creation...")
    
    if not os.path.exists(EXTRACTED_DIR):
        raise FileNotFoundError(f"Extracted data directory not found: {EXTRACTED_DIR}")
    
    create_database_with_indexes(EXTRACTED_DIR)
    
    synthetic_players = sum(1 for pid in player_id_to_names.keys() if str(pid).startswith('SYNTH_'))
    print(f"Database creation completed. Created {synthetic_players} synthetic player IDs.")
    
    return "Database creation successful"

if __name__ == "__main__":
    main()