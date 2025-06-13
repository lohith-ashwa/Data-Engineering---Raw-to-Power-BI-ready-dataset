# even more imporved to choose the highest occuring name for same player id but different name variants. this includes the below addition
# of handling missing player ids i.e., synthetically generating stuff
import json
import pandas as pd
import os
import glob
import duckdb
from datetime import datetime
from collections import defaultdict, Counter

# Function to extract overs table with extras information
def extract_overs_table(data_list):
    # This function doesn't need player ID handling, so it remains unchanged
    # Code remains the same as original...
    overs_rows = []
    
    for data in data_list:
        match_id = data['match_id']
        innings_data = data['innings']
        
        for i, inning in enumerate(innings_data):
            innings_id = f"{match_id}_{i+1}"
            
            # Check if 'overs' exists in the inning data
            if 'overs' not in inning:
                # Skip this inning if no overs data
                print(f"Warning: No overs data found in inning {i+1} of match {match_id}")
                continue
                
            for over in inning['overs']:
                over_num = over['over']
                
                # Calculate runs in this over
                total_runs = sum(delivery['runs']['total'] for delivery in over['deliveries'])
                
                # Calculate wickets in this over
                wickets = sum(1 for delivery in over['deliveries'] if 'wickets' in delivery)
                
                # Calculate extras in this over
                total_extras = 0
                extras_wides = 0
                extras_noballs = 0
                extras_byes = 0
                extras_legbyes = 0
                
                for delivery in over['deliveries']:
                    if 'extras' in delivery:
                        extras = delivery['extras']
                        total_extras += delivery['runs'].get('extras', 0)
                        
                        # Count specific types of extras
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
                    'num_deliveries': len(over['deliveries']),
                    'total_extras': total_extras,
                    'extras_wides': extras_wides,
                    'extras_noballs': extras_noballs,
                    'extras_byes': extras_byes,
                    'extras_legbyes': extras_legbyes
                }
                overs_rows.append(over_row)
    
    # If no overs data was found, return an empty DataFrame with the right columns
    if not overs_rows:
        return pd.DataFrame(columns=['over_id', 'innings_id', 'over_number', 'total_runs', 'wickets', 
                                     'num_deliveries', 'total_extras', 'extras_wides', 'extras_noballs', 
                                     'extras_byes', 'extras_legbyes'])
    
    return pd.DataFrame(overs_rows)

# Global dictionaries for player tracking
player_name_to_id = {}  # Maps player names to IDs
player_id_to_names = defaultdict(list)  # Maps IDs to all name variations
next_synthetic_id = 1000000  # Start synthetic IDs from a high number to avoid conflicts

# Function to get or create player ID
def get_player_id(player_name, player_registry):
    global next_synthetic_id
    
    if not player_name:
        return None
    
    # First check if player exists in registry
    player_id = player_registry.get(player_name, None)
    
    # If in registry, record this name variation
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

# Function to extract deliveries table with enhanced player ID handling
def extract_deliveries_table(data_list):
    delivery_rows = []
    
    for data in data_list:
        match_id = data['match_id']
        innings_data = data['innings']
        
        # Get player registry if available
        player_registry = {}
        if 'info' in data and 'registry' in data['info'] and 'people' in data['info']['registry']:
            player_registry = data['info']['registry']['people']
        
        for i, inning in enumerate(innings_data):
            innings_id = f"{match_id}_{i+1}"
            
            # Check if 'overs' exists in the inning data
            if 'overs' not in inning:
                # Skip this inning if no overs data
                print(f"Warning: No overs data found in inning {i+1} of match {match_id}")
                continue
                
            for over in inning['overs']:
                over_num = over['over']
                over_id = f"{innings_id}_{over_num}"
                
                for ball_idx, delivery in enumerate(over['deliveries']):
                    # Basic information
                    delivery_id = f"{over_id}_{ball_idx+1}"
                    batter = delivery.get('batter', None)
                    bowler = delivery.get('bowler', None)
                    non_striker = delivery.get('non_striker', None)
                    
                    # Get player IDs with enhanced handling
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
                        wicket = delivery['wickets'][0]  # Taking first wicket info
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
    
    # If no deliveries data was found, return an empty DataFrame with the right columns
    if not delivery_rows:
        return pd.DataFrame(columns=['delivery_id', 'over_id', 'innings_id', 'match_id', 'over_number', 
                                     'ball_number', 'batter', 'batter_id', 'bowler', 'bowler_id', 
                                     'non_striker', 'non_striker_id', 'batter_runs', 'extras', 
                                     'total_runs', 'extras_type', 'extras_value', 'is_wicket', 
                                     'wicket_player_out', 'wicket_player_out_id', 'wicket_kind', 
                                     'wicket_fielder', 'wicket_fielder_id'])
    
    return pd.DataFrame(delivery_rows)

# Enhanced matches table extraction with player ID handling
def extract_matches_table(data_list):
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
        
        # Extract outcome method if available
        outcome_method = None
        if 'outcome' in match_info and 'method' in match_info['outcome']:
            outcome_method = match_info['outcome']['method']
        
        # Get player registry if available
        player_registry = {}
        if 'registry' in match_info and 'people' in match_info['registry']:
            player_registry = match_info['registry']['people']
        
        # Get player_of_match ID with enhanced handling
        player_of_match = match_info.get('player_of_match', [None])[0]
        player_of_match_id = get_player_id(player_of_match, player_registry) if player_of_match else None
        
        # Format date as proper DATE
        date_str = match_info.get('dates', [None])[0]
        date = None
        if date_str:
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                date = date_str  # Keep as string if parsing fails
                
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
            'outcome_method': outcome_method,
            'player_of_match': player_of_match,
            'player_of_match_id': player_of_match_id
        }
        matches.append(match_row)
    
    return pd.DataFrame(matches)

# Enhanced innings table with powerplay information (no player ID handling needed)
def extract_innings_table(data_list):
    # No changes needed here as this function doesn't deal with player IDs
    # Code remains the same as original...
    innings_rows = []
    
    for data in data_list:
        match_id = data['match_id']
        
        if 'innings' in data:
            innings_data = data['innings']
            
            for i, inning in enumerate(innings_data):
                innings_id = f"{match_id}_{i+1}"
                
                # Extract powerplay information if available
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
                
                # Find bowling team if possible
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

# Enhanced players table to handle multiple name variations
def extract_players_table(data_list):
    # First, gather all player name variations from registry
    for data in data_list:
        if 'info' in data and 'registry' in data['info'] and 'people' in data['info']['registry']:
            registry = data['info']['registry']['people']
            for player_name, player_id in registry.items():
                player_name_to_id[player_name] = player_id
                player_id_to_names[player_id].append(player_name)
    
    # Create players table with preferred name for each player ID
    players_list = []
    for player_id, name_variations in player_id_to_names.items():
        # Count occurrences of each name variation
        name_counts = Counter(name_variations)
        
        # Select the most common name as the primary name
        primary_name = name_counts.most_common(1)[0][0]
        
        # Get all alternative names (excluding the primary)
        alt_names = [name for name, _ in name_counts.most_common() if name != primary_name]
        
        # Add record with primary name and alternatives
        player_row = {
            'player_id': player_id,
            'player_name': primary_name,
            'name_variations': ';'.join(alt_names) if alt_names else None,
            'variant_count': len(alt_names) + 1  # Total number of name variations
        }
        players_list.append(player_row)
    
    # Create DataFrame from the list
    players_df = pd.DataFrame(players_list)
    
    # Count statistics about name variations
    if not players_df.empty:
        players_with_variations = players_df[players_df['variant_count'] > 1]
        print(f"Found {len(players_with_variations)} players with multiple name variations")
        if not players_with_variations.empty:
            max_variations = players_with_variations['variant_count'].max()
            print(f"Maximum number of variations for any player: {max_variations}")
    
    return players_df

# Main processing function
def process_all_cricket_json(directory_path):
    # Get all JSON files
    json_files = glob.glob(os.path.join(directory_path, '*.json'))
    
    all_data = []
    
    # Read and pre-process each file
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                data['match_id'] = os.path.basename(json_file).split('.')[0]
                all_data.append(data)
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
    
    print(f"Processed {len(all_data)} JSON files")
    
    # Extract all tables with basic error handling
    try:
        matches_df = extract_matches_table(all_data)
        print(f"Extracted matches table: {len(matches_df)} rows")
    except Exception as e:
        print(f"Error extracting matches table: {e}")
        matches_df = pd.DataFrame()
    
    try:
        innings_df = extract_innings_table(all_data)
        print(f"Extracted innings table: {len(innings_df)} rows")
    except Exception as e:
        print(f"Error extracting innings table: {e}")
        innings_df = pd.DataFrame()
    
    try:
        overs_df = extract_overs_table(all_data)
        print(f"Extracted overs table: {len(overs_df)} rows")
    except Exception as e:
        print(f"Error extracting overs table: {e}")
        overs_df = pd.DataFrame()
    
    try:
        deliveries_df = extract_deliveries_table(all_data)
        print(f"Extracted deliveries table: {len(deliveries_df)} rows")
    except Exception as e:
        print(f"Error extracting deliveries table: {e}")
        deliveries_df = pd.DataFrame()
    
    try:
        players_df = extract_players_table(all_data)
        print(f"Extracted players table: {len(players_df)} rows")
        print(f"Created {sum(1 for pid in players_df['player_id'] if str(pid).startswith('SYNTH_'))} synthetic player IDs")
    except Exception as e:
        print(f"Error extracting players table: {e}")
        players_df = pd.DataFrame()
    
    # Return dictionary of all dataframes
    return {
        'matches': matches_df,
        'players': players_df,
        'innings': innings_df,
        'overs': overs_df,
        'deliveries': deliveries_df
    }

# Process all files and create database with indexes
def create_database_with_indexes(data_directory, db_name='cricket_analytics.db'):
    # Process all files
    cricket_data = process_all_cricket_json(data_directory)
    
    # Create database connection
    conn = duckdb.connect(db_name)
    
    # Create tables and indexes
    for table_name, df in cricket_data.items():
        if not df.empty:
            # Remove potential duplicate rows
            df = df.drop_duplicates()
            
            # Create table and save data
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
            print(f"Created table: {table_name}")
            
            # Create indexes based on table name
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
    
    print(f"Successfully created database with indexes in {db_name}")
    conn.close()

# Run the database creation
if __name__ == "__main__":
    create_database_with_indexes("./extracted_data_json/")


