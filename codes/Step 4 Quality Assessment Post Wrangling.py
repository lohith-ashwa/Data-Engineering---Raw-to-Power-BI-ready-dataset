import duckdb
import pandas as pd

# Connect to your database
conn = duckdb.connect('cricket_analytics.db')

# Function to explore table structure and data
def explore_table(table_name):
    print(f"\n--- Table: {table_name} ---")
    
    # Get column names and data types
    schema_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
    print("\nSchema:")
    for column in schema_info:
        print(f"  {column[0]} - {column[1]}")
    
    # Get row count
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"\nRow count: {count}")
    
    # Get sample data
    print("\nSample data (top 20 rows):")
    sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 20").fetchdf()
    return sample

# List all tables
tables = conn.execute("SHOW TABLES").fetchall()
table_names = [table[0] for table in tables]
print(f"Tables in database: {table_names}")

# Explore each table
for table_name in table_names:
    sample_df = explore_table(table_name)
    display(sample_df)


from tabulate import tabulate

def analyze_column_ranges():
    """Analyze min and max values for columns to be converted"""
    
    # Define the columns to analyze (excluding 'season')
    columns_to_analyze = {
        'deliveries': [
            'over_number', 'ball_number', 'batter_runs', 
            'extras', 'total_runs', 'extras_value', 'is_wicket'
        ],
        'innings': ['innings_number'],
        'matches': [
            'date', 'overs', 'outcome_by_runs', 'outcome_by_wickets'
        ],
        'overs': [
            'over_number', 'total_runs', 'wickets', 'num_deliveries'
        ]
    }
    
    # Results container
    results = []
    
    # For each table and its columns
    for table, columns in columns_to_analyze.items():
        for col in columns:
            try:
                # Query min and max values
                query = f"""
                SELECT 
                    MIN({col}) as min_value,
                    MAX({col}) as max_value
                FROM {table}
                """
                
                result = conn.execute(query).fetchone()
                min_val, max_val = result
                
                results.append({
                    'Table': table,
                    'Column': col,
                    'Min Value': min_val,
                    'Max Value': max_val
                })
            except Exception as e:
                # Handle any errors (like invalid column names)
                results.append({
                    'Table': table,
                    'Column': col,
                    'Min Value': f"Error: {str(e)}",
                    'Max Value': f"Error: {str(e)}"
                })
    
    # Convert to DataFrame for display
    return pd.DataFrame(results)

# Get the min/max values
range_analysis = analyze_column_ranges()

# Print the results as a nice table
print("\n=== Column Range Analysis ===\n")
print(tabulate(range_analysis, headers='keys', tablefmt='grid'))


def get_current_schema(connection):
    """Get the current schema for all tables in the database"""
    tables = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
    tables = [table[0] for table in tables]
    
    schema_info = []
    
    for table_name in tables:
        columns = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        for col in columns:
            schema_info.append({
                'Table': table_name,
                'Column': col[1],
                'CurrentType': col[2]
            })
    
    return pd.DataFrame(schema_info)

def apply_type_conversions(connection):
    """Apply the recommended data type conversions based on actual value ranges"""
    
    # Store the current schema
    current_schema = get_current_schema(connection)
    
    # Define the type conversion mapping for each table and column
    # Updated based on the actual value ranges from your analysis
    type_conversions = {
        'deliveries': {
            'over_number': 'SMALLINT',    # Max 248, needs SMALLINT instead of TINYINT
            'ball_number': 'TINYINT',     # Max 19, TINYINT works
            'batter_runs': 'TINYINT',     # Max 8, TINYINT works
            'extras': 'TINYINT',          # Max 12, TINYINT works
            'total_runs': 'TINYINT',      # Max 13, TINYINT works
            'extras_value': 'TINYINT',    # Max 12, TINYINT works
            'is_wicket': 'BOOLEAN'        # Binary values, BOOLEAN works
        },
        'innings': {
            'innings_number': 'TINYINT'   # Max 6, TINYINT works
        },
        'matches': {
            'date': 'DATE',               # Date format, DATE type appropriate
            'overs': 'DECIMAL(4,1)',      # Values 20.0-50.0, DECIMAL works well
            'outcome_by_runs': 'SMALLINT', # Max 504, needs SMALLINT
            'outcome_by_wickets': 'TINYINT' # Max 10, TINYINT works
        },
        'overs': {
            'over_number': 'SMALLINT',    # Max 248, needs SMALLINT
            'total_runs': 'TINYINT',      # Max 43, TINYINT works
            'wickets': 'TINYINT',         # Max 5, TINYINT works
            'num_deliveries': 'TINYINT'   # Max 19, TINYINT works
        }
    }
    
    # Apply the conversions
    print("Converting data types...")
    
    # For each table
    for table, columns in type_conversions.items():
        # Get the current columns from the table
        table_columns = connection.execute(f"PRAGMA table_info('{table}')").fetchall()
        column_names = [col[1] for col in table_columns]
        
        # Create a temporary table with the new schema
        temp_table = f"{table}_temp"
        create_temp_sql = f"CREATE TABLE {temp_table} AS SELECT "
        
        # Build the column list with type casts
        column_clauses = []
        for col in column_names:
            if col in columns:
                # Convert this column's type
                column_clauses.append(f"CAST({col} AS {columns[col]}) AS {col}")
            else:
                # Keep this column's type
                column_clauses.append(col)
        
        create_temp_sql += ", ".join(column_clauses)
        create_temp_sql += f" FROM {table}"
        
        # Execute the conversion
        connection.execute(create_temp_sql)
        
        # Drop the original table and rename the temp table
        connection.execute(f"DROP TABLE {table}")
        connection.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")
        
        print(f"Converted table: {table}")
    
    # Get the new schema
    new_schema = get_current_schema(connection)
    
    # Merge the dataframes to show before and after
    schema_comparison = current_schema.merge(
        new_schema, 
        on=['Table', 'Column'], 
        suffixes=('_Before', '_After')
    )
    
    # Add a "Changed" column
    schema_comparison['Changed'] = schema_comparison['CurrentType_Before'] != schema_comparison['CurrentType_After']
    
    return schema_comparison

def print_type_conversion_report(schema_comparison, connection):
    """Print a nicely formatted report of the type conversions"""
    
    # Group by table
    for table_name in schema_comparison['Table'].unique():
        table_data = schema_comparison[schema_comparison['Table'] == table_name]
        
        print(f"\n=== Table: {table_name} ===\n")
        
        # Format the data for table display
        display_data = []
        for _, row in table_data.iterrows():
            # Get column min and max values if the column was changed
            min_val = max_val = "N/A"
            if row['Changed']:
                try:
                    min_max = connection.execute(f"SELECT MIN({row['Column']}), MAX({row['Column']}) FROM {table_name}").fetchone()
                    min_val = str(min_max[0]) if min_max[0] is not None else "NULL"
                    max_val = str(min_max[1]) if min_max[1] is not None else "NULL"
                except:
                    pass  # Skip if error occurs
            
            display_data.append([
                row['Column'],
                row['CurrentType_Before'],
                row['CurrentType_After'],
                min_val,
                max_val,
                "✓" if row['Changed'] else ""
            ])
        
        # Print the table
        print(tabulate(
            display_data,
            headers=["Column", "Before", "After", "Min Value", "Max Value", "Changed"],
            tablefmt="grid"
        ))
    
    # Summary
    changed_count = schema_comparison[schema_comparison['Changed']].shape[0]
    total_count = schema_comparison.shape[0]
    
    print(f"\n=== Summary ===\n")
    print(f"Total columns: {total_count}")
    print(f"Columns with type changes: {changed_count} ({changed_count/total_count*100:.1f}%)")
    
    # Group changes by type
    type_changes = schema_comparison[schema_comparison['Changed']].groupby(['CurrentType_Before', 'CurrentType_After']).size().reset_index(name='Count')
    
    print("\nType conversion breakdown:")
    for _, row in type_changes.iterrows():
        print(f"  {row['CurrentType_Before']} → {row['CurrentType_After']}: {row['Count']} columns")
    
    # Estimate storage savings
    before_sizes = {
        'VARCHAR': 16,  # Average estimate
        'BIGINT': 8,
        'DOUBLE': 8,
        'TINYINT': 1,
        'SMALLINT': 2, 
        'BOOLEAN': 1,
        'DATE': 4,
        'DECIMAL(4,1)': 4
    }
    
    after_sizes = {
        'VARCHAR': 16,  # Average estimate
        'BIGINT': 8,
        'DOUBLE': 8,
        'TINYINT': 1,
        'SMALLINT': 2, 
        'BOOLEAN': 1,
        'DATE': 4,
        'DECIMAL(4,1)': 4
    }
    
    # Calculate before and after sizes
    schema_comparison['SizeBefore'] = schema_comparison['CurrentType_Before'].map(
        lambda x: before_sizes.get(x, 8)  # Default to 8 bytes if unknown
    )
    
    schema_comparison['SizeAfter'] = schema_comparison['CurrentType_After'].map(
        lambda x: after_sizes.get(x, 8)  # Default to 8 bytes if unknown
    )
    
    # Get row counts for each table
    row_counts = {}
    for table in schema_comparison['Table'].unique():
        row_counts[table] = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    
    # Add table row counts to the dataframe
    schema_comparison['RowCount'] = schema_comparison['Table'].map(row_counts)
    
    # Calculate storage impact
    schema_comparison['StorageBefore'] = schema_comparison['SizeBefore'] * schema_comparison['RowCount']
    schema_comparison['StorageAfter'] = schema_comparison['SizeAfter'] * schema_comparison['RowCount']
    
    total_before = schema_comparison['StorageBefore'].sum()
    total_after = schema_comparison['StorageAfter'].sum()
    savings = total_before - total_after
    savings_pct = (savings / total_before) * 100
    
    print(f"\nEstimated storage impact:")
    print(f"  Before: {total_before/1024/1024:.2f} MB")
    print(f"  After: {total_after/1024/1024:.2f} MB")
    print(f"  Savings: {savings/1024/1024:.2f} MB ({savings_pct:.1f}%)")
    
    # Print per-table statistics
    print("\nPer-table storage impact:")
    table_stats = schema_comparison.groupby('Table').agg({
        'RowCount': 'first',
        'StorageBefore': 'sum',
        'StorageAfter': 'sum'
    }).reset_index()
    
    table_stats['Savings'] = table_stats['StorageBefore'] - table_stats['StorageAfter']
    table_stats['SavingsPct'] = (table_stats['Savings'] / table_stats['StorageBefore']) * 100
    
    table_display = []
    for _, row in table_stats.iterrows():
        table_display.append([
            row['Table'],
            f"{row['RowCount']:,}",
            f"{row['StorageBefore']/1024/1024:.2f} MB",
            f"{row['StorageAfter']/1024/1024:.2f} MB",
            f"{row['Savings']/1024/1024:.2f} MB",
            f"{row['SavingsPct']:.1f}%"
        ])
    
    print(tabulate(
        table_display,
        headers=["Table", "Rows", "Before", "After", "Savings", "Savings %"],
        tablefmt="grid"
    ))

# Run the conversion
schema_comparison = apply_type_conversions(conn)

# Print the report
print_type_conversion_report(schema_comparison, conn)


import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to your database
conn = duckdb.connect('cricket_analytics.db')

# Function to check for NULL values in each table
def analyze_null_values(connection):
    # Get list of tables
    tables = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
    tables = [table[0] for table in tables]
    
    null_stats = {}
    
    for table in tables:
        # Get all columns for the table
        columns = connection.execute(f"PRAGMA table_info('{table}')").fetchall()
        column_names = [col[1] for col in columns]
        
        table_stats = {}
        total_rows = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        for col in column_names:
            # Count NULL values
            null_count = connection.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL").fetchone()[0]
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            
            table_stats[col] = {
                'null_count': null_count,
                'null_percentage': null_percentage
            }
        
        null_stats[table] = {
            'total_rows': total_rows,
            'columns': table_stats
        }
    
    return null_stats

# Execute the analysis
null_analysis = analyze_null_values(conn)

# Print formatted results
for table, stats in null_analysis.items():
    print(f"\n=== NULL Analysis for {table} table ===")
    print(f"Total rows: {stats['total_rows']}")
    print("\nColumn-wise NULL statistics:")
    
    null_data = []
    for col, values in stats['columns'].items():
        null_data.append({
            'Column': col,
            'NULL Count': values['null_count'],
            'NULL %': f"{values['null_percentage']:.2f}%"
        })
    
    # Convert to DataFrame for better display
    null_df = pd.DataFrame(null_data)
    null_df = null_df.sort_values(by='NULL Count', ascending=False)
    print(null_df.to_string(index=False))
    
    # Plot the results for columns with missing values
    null_cols = null_df[null_df['NULL Count'] > 0]
    if not null_cols.empty:
        plt.figure(figsize=(12, 6))
        sns.barplot(x='Column', y='NULL Count', data=null_cols)
        plt.title(f'NULL Values in {table} Table')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f'{table}_null_analysis.png')
        plt.close()


def analyze_venue_consistency(connection):
    # Identify variations in venue names
    venue_query = """
    SELECT 
        venue, 
        COUNT(*) as match_count
    FROM 
        matches
    WHERE 
        venue IS NOT NULL
    GROUP BY 
        venue
    ORDER BY 
        match_count DESC
    """
    
    venues = connection.execute(venue_query).fetchdf()
    
    # Find similar venue names using fuzzy matching
    import difflib
    
    similar_venues = []
    venue_list = venues['venue'].tolist()
    
    for i, venue1 in enumerate(venue_list):
        for venue2 in venue_list[i+1:]:
            # Calculate similarity ratio
            similarity = difflib.SequenceMatcher(None, venue1, venue2).ratio()
            
            # If venues are similar but not identical
            if similarity > 0.8 and similarity < 1.0:
                similar_venues.append({
                    'venue1': venue1,
                    'venue2': venue2,
                    'similarity': similarity,
                    'count1': venues[venues['venue'] == venue1]['match_count'].values[0],
                    'count2': venues[venues['venue'] == venue2]['match_count'].values[0]
                })
    
    # Convert to DataFrame for better display
    similar_venues_df = pd.DataFrame(similar_venues)
    if not similar_venues_df.empty:
        similar_venues_df = similar_venues_df.sort_values(by='similarity', ascending=False)
    
    return {
        'venue_counts': venues,
        'similar_venues': similar_venues_df
    }

# Execute the venue analysis
venue_analysis = analyze_venue_consistency(conn)

# Print results
print("\n=== Venue Analysis ===")
print(f"Total unique venues: {len(venue_analysis['venue_counts'])}")
print("\nTop 20 venues by match count:")
print(venue_analysis['venue_counts'].head(20).to_string(index=False))

if not venue_analysis['similar_venues'].empty:
    print("\nPotentially similar venue names:")
    print(venue_analysis['similar_venues'].to_string(index=False))


def cricket_domain_aware_validation(connection):
    """
    Performs data validation with proper cricket domain knowledge
    """
    print("\n=== Cricket-Specific Data Validation ===")
    
    # 1. Check for truly invalid batter runs (above 7)
    print("\n1. Checking for invalid batter runs...")
    
    invalid_runs_query = """
    SELECT 
        delivery_id, 
        batter,
        bowler,
        match_id,
        batter_runs
    FROM 
        deliveries
    WHERE 
        batter_runs > 7  -- Allow up to 7 runs (3 runs + 4 overthrow boundary)
    ORDER BY
        batter_runs DESC
    LIMIT 10
    """
    
    invalid_runs = connection.execute(invalid_runs_query).fetchdf()
    
    if len(invalid_runs) > 0:
        print(f"Found {len(invalid_runs)} deliveries with truly invalid runs (>7):")
        print(invalid_runs.to_string(index=False))
    else:
        print("No truly invalid batter runs found. All values are within cricket rules (0-7).")
        
    # 2. Check for valid 7-run deliveries and confirm they're rare
    print("\n2. Analyzing frequency of 7-run deliveries...")
    
    seven_runs_query = """
    SELECT 
        COUNT(*) as count_7_runs,
        (SELECT COUNT(*) FROM deliveries) as total_deliveries,
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM deliveries) as percentage
    FROM 
        deliveries
    WHERE 
        batter_runs = 7
    """
    
    seven_runs_stats = connection.execute(seven_runs_query).fetchone()
    
    print(f"7-run deliveries: {seven_runs_stats[0]:,} out of {seven_runs_stats[1]:,} total deliveries ({seven_runs_stats[2]:.6f}%)")
    print("VALID: While rare, 7 runs can occur legitimately in cricket (3 runs + 4 overthrow boundary)")
    
    # 3. Analyze match overs with cricket-specific logic
    print("\n3. Analyzing match overs with cricket-specific knowledge...")
    
    match_overs_query = """
    WITH match_overs AS (
        SELECT 
            m.match_id,
            m.match_type,
            m.overs as declared_overs,
            MAX(o.over_number) + 1 as max_over_number,  -- Add 1 because over numbers start at 0
            COUNT(DISTINCT o.over_id) as actual_overs_count,
            m.outcome_winner
        FROM 
            matches m
        JOIN 
            innings i ON m.match_id = i.match_id
        JOIN 
            overs o ON i.innings_id = o.innings_id
        GROUP BY 
            m.match_id, m.match_type, m.overs, m.outcome_winner
    )
    SELECT 
        match_type,
        COUNT(*) as matches,
        AVG(declared_overs) as avg_declared_overs,
        AVG(max_over_number) as avg_max_over,
        AVG(actual_overs_count) as avg_actual_overs,
        COUNT(CASE WHEN outcome_winner IS NULL THEN 1 END) as no_result_matches,
        COUNT(CASE WHEN actual_overs_count < declared_overs * 0.7 AND outcome_winner IS NOT NULL THEN 1 END) as shortened_with_result
    FROM 
        match_overs
    WHERE
        declared_overs IS NOT NULL AND
        match_type IN ('ODI', 'T20', 'IT20', 'ODM', 'T20M')
    GROUP BY
        match_type
    ORDER BY
        matches DESC
    """
    
    match_overs_stats = connection.execute(match_overs_query).fetchdf()
    
    print("Match overs statistics by match type:")
    print(match_overs_stats.to_string(index=False))
    
    # 4. Find truly problematic matches (overs count exceeds declared limit)
    print("\n4. Checking for matches where actual overs exceed declared limit...")
    
    exceeded_overs_query = """
    WITH match_overs AS (
        SELECT 
            m.match_id,
            m.match_type,
            m.overs as declared_overs,
            MAX(o.over_number) + 1 as max_over_number,
            COUNT(DISTINCT o.over_id) as actual_overs_count,
            m.outcome_winner
        FROM 
            matches m
        JOIN 
            innings i ON m.match_id = i.match_id
        JOIN 
            overs o ON i.innings_id = o.innings_id
        GROUP BY 
            m.match_id, m.match_type, m.overs, m.outcome_winner
    )
    SELECT 
        match_id, 
        match_type,
        declared_overs,
        max_over_number,
        actual_overs_count,
        outcome_winner
    FROM 
        match_overs
    WHERE 
        declared_overs IS NOT NULL AND
        match_type IN ('ODI', 'T20', 'IT20', 'ODM', 'T20M') AND
        max_over_number > declared_overs * 1.1  -- 10% buffer for rounding
    ORDER BY
        (max_over_number - declared_overs) DESC
    LIMIT 10
    """
    
    exceeded_overs = connection.execute(exceeded_overs_query).fetchdf()
    
    if len(exceeded_overs) > 0:
        print(f"Found {len(exceeded_overs)} matches where actual overs exceed declared limit:")
        print(exceeded_overs.to_string(index=False))
    else:
        print("No matches found where actual overs exceed the declared limit.")
        print("VALID: The '50.0 declared vs 50 actual' is a data type difference (float vs int), not a data issue.")
    
    # 5. Check for unusual over lengths
    print("\n5. Checking for unusual over lengths...")
    
    unusual_overs_query = """
    SELECT 
        num_deliveries,
        COUNT(*) as over_count,
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM overs) as percentage
    FROM 
        overs
    GROUP BY 
        num_deliveries
    ORDER BY 
        num_deliveries
    """
    
    unusual_overs = connection.execute(unusual_overs_query).fetchdf()
    
    print("Over lengths distribution:")
    print(unusual_overs.to_string(index=False))
    print("VALID: Overs with >6 deliveries occur due to extras (wides, no-balls), which is normal in cricket.")
    
    return {
        "invalid_runs": invalid_runs,
        "seven_runs_stats": seven_runs_stats,
        "match_overs_stats": match_overs_stats,
        "exceeded_overs": exceeded_overs,
        "unusual_overs": unusual_overs
    }

# Run
validation_results = cricket_domain_aware_validation(conn)



def analyze_runs_distribution(connection):
    # Get overall runs distribution
    runs_query = """
    SELECT 
        batter_runs,
        COUNT(*) as frequency
    FROM 
        deliveries
    GROUP BY 
        batter_runs
    ORDER BY 
        batter_runs
    """
    
    runs_dist = connection.execute(runs_query).fetchdf()
    
    # Check for outliers (runs that should not exist in cricket)
    outlier_runs_query = """
    SELECT 
        delivery_id,
        match_id,
        batter,
        bowler,
        batter_runs,
        extras,
        total_runs
    FROM 
        deliveries
    WHERE 
        batter_runs > 6  -- In cricket, maximum legitimate batter runs is 6
    ORDER BY 
        batter_runs DESC
    LIMIT 100
    """
    
    outlier_runs = connection.execute(outlier_runs_query).fetchdf()
    
    # Analyze extras distribution
    extras_query = """
    SELECT 
        extras_type,
        COUNT(*) as frequency,
        AVG(extras_value) as avg_value,
        MAX(extras_value) as max_value
    FROM 
        deliveries
    WHERE 
        extras > 0
    GROUP BY 
        extras_type
    ORDER BY 
        frequency DESC
    """
    
    extras_dist = connection.execute(extras_query).fetchdf()
    
    # Check for unusual total runs (very high totals might be errors)
    high_runs_query = """
    SELECT 
        delivery_id,
        match_id,
        batter,
        bowler,
        batter_runs,
        extras,
        total_runs
    FROM 
        deliveries
    WHERE 
        total_runs > 7  -- Legitimate totals rarely exceed 7 (6 runs + 1 extra)
    ORDER BY 
        total_runs DESC
    LIMIT 100
    """
    
    high_total_runs = connection.execute(high_runs_query).fetchdf()
    
    return {
        'runs_distribution': runs_dist,
        'outlier_runs': outlier_runs,
        'extras_distribution': extras_dist,
        'high_total_runs': high_total_runs
    }

# Execute runs analysis
runs_analysis = analyze_runs_distribution(conn)

# Print results
print("\n=== Runs Distribution Analysis ===")
print("\nBatter Runs Distribution:")
print(runs_analysis['runs_distribution'].to_string(index=False))

print("\nOutlier Runs (batter_runs > 6):")
if runs_analysis['outlier_runs'].empty:
    print("No outliers found.")
else:
    print(f"Found {len(runs_analysis['outlier_runs'])} outliers")
    print(runs_analysis['outlier_runs'].head(10).to_string(index=False))

print("\nExtras Distribution:")
print(runs_analysis['extras_distribution'].to_string(index=False))

print("\nUnusually High Total Runs (total_runs > 7):")
if runs_analysis['high_total_runs'].empty:
    print("No unusually high totals found.")
else:
    print(f"Found {len(runs_analysis['high_total_runs'])} unusually high totals")
    print(runs_analysis['high_total_runs'].head(10).to_string(index=False))

# Plot runs distribution
plt.figure(figsize=(10, 6))
sns.barplot(x='batter_runs', y='frequency', data=runs_analysis['runs_distribution'])
plt.title('Batter Runs Distribution')
plt.ylabel('Frequency (log scale)')
plt.yscale('log')
plt.savefig('runs_distribution.png')
plt.close()

# Plot extras distribution if available
if not runs_analysis['extras_distribution'].empty:
    plt.figure(figsize=(10, 6))
    sns.barplot(x='extras_type', y='frequency', data=runs_analysis['extras_distribution'])
    plt.title('Extras Type Distribution')
    plt.ylabel('Frequency (log scale)')
    plt.yscale('log')
    plt.savefig('extras_distribution.png')
    plt.close()


def analyze_over_lengths(connection):
    # Check over lengths by match type
    over_length_query = """
    SELECT 
        m.match_type,
        AVG(o.num_deliveries) as avg_deliveries_per_over,
        MIN(o.num_deliveries) as min_deliveries,
        MAX(o.num_deliveries) as max_deliveries,
        COUNT(*) as over_count
    FROM 
        overs o
    JOIN 
        innings i ON o.innings_id = i.innings_id
    JOIN 
        matches m ON i.match_id = m.match_id
    GROUP BY 
        m.match_type
    ORDER BY 
        avg_deliveries_per_over DESC
    """
    
    over_lengths = connection.execute(over_length_query).fetchdf()
    
    # Find overs with unusually high number of deliveries
    long_overs_query = """
    SELECT 
        o.over_id,
        m.match_id,
        m.match_type,
        i.batting_team,
        i.bowling_team,
        o.over_number,
        o.num_deliveries,
        o.total_runs,
        o.wickets
    FROM 
        overs o
    JOIN 
        innings i ON o.innings_id = i.innings_id
    JOIN 
        matches m ON i.match_id = m.match_id
    WHERE 
        o.num_deliveries > 8  -- Most overs should have 6 deliveries (or slightly more with extras)
    ORDER BY 
        o.num_deliveries DESC
    LIMIT 100
    """
    
    long_overs = connection.execute(long_overs_query).fetchdf()
    
    # Analyze what makes overs longer (extras, wickets)
    over_factors_query = """
    WITH over_details AS (
        SELECT 
            o.over_id,
            o.num_deliveries,
            COUNT(CASE WHEN d.extras > 0 THEN 1 END) as extras_count,
            COUNT(CASE WHEN d.is_wicket = 1 THEN 1 END) as wicket_count,
            SUM(d.total_runs) as total_runs
        FROM 
            overs o
        JOIN 
            deliveries d ON o.over_id = d.over_id
        GROUP BY 
            o.over_id, o.num_deliveries
    )
    SELECT 
        num_deliveries,
        COUNT(*) as over_count,
        AVG(extras_count) as avg_extras,
        AVG(wicket_count) as avg_wickets,
        AVG(total_runs) as avg_runs
    FROM 
        over_details
    GROUP BY 
        num_deliveries
    ORDER BY 
        num_deliveries
    """
    
    over_factors = connection.execute(over_factors_query).fetchdf()
    
    return {
        'over_lengths_by_type': over_lengths,
        'unusually_long_overs': long_overs,
        'over_length_factors': over_factors
    }

# Execute over length analysis
over_analysis = analyze_over_lengths(conn)

# Print results
print("\n=== Over Length Analysis ===")
print("\nOver Lengths by Match Type:")
print(over_analysis['over_lengths_by_type'].to_string(index=False))

print("\nUnusually Long Overs (>8 deliveries):")
if over_analysis['unusually_long_overs'].empty:
    print("No unusually long overs found.")
else:
    print(f"Found {len(over_analysis['unusually_long_overs'])} unusually long overs")
    print(over_analysis['unusually_long_overs'].head(10).to_string(index=False))

print("\nFactors Affecting Over Length:")
print(over_analysis['over_length_factors'].to_string(index=False))

# Plot average deliveries per over by match type
plt.figure(figsize=(10, 6))
sns.barplot(x='match_type', y='avg_deliveries_per_over', data=over_analysis['over_lengths_by_type'])
plt.title('Average Deliveries per Over by Match Type')
plt.ylim(5.8, 7.0)  # Expected range for cricket
plt.savefig('over_lengths_by_type.png')
plt.close()

# Plot relationship between extras and over length
plt.figure(figsize=(10, 6))
sns.scatterplot(x='num_deliveries', y='avg_extras', 
                size='over_count', data=over_analysis['over_length_factors'])
plt.title('Relationship Between Over Length and Extras')
plt.xlabel('Number of Deliveries in Over')
plt.ylabel('Average Extras per Over')
plt.savefig('over_length_vs_extras.png')
plt.close()



def check_player_id_consistency(connection):
    """
    Comprehensive check for player ID and name consistency issues
    Returns detailed information about inconsistencies
    """
    # Check players with missing IDs or multiple IDs
    player_id_check = """
    WITH player_names AS (
        SELECT DISTINCT 
            batter as player_name 
        FROM 
            deliveries
        UNION
        SELECT DISTINCT 
            bowler
        FROM 
            deliveries
        UNION
        SELECT DISTINCT 
            non_striker
        FROM 
            deliveries
        UNION
        SELECT DISTINCT 
            wicket_player_out
        FROM 
            deliveries
        WHERE 
            wicket_player_out IS NOT NULL
        UNION
        SELECT DISTINCT 
            wicket_fielder
        FROM 
            deliveries
        WHERE 
            wicket_fielder IS NOT NULL
    )
    SELECT 
        p.player_name,
        COUNT(DISTINCT pl.player_id) as id_count
    FROM 
        player_names p
    LEFT JOIN 
        players pl ON p.player_name = pl.player_name
    GROUP BY 
        p.player_name
    HAVING 
        COUNT(DISTINCT pl.player_id) > 1 OR
        COUNT(DISTINCT pl.player_id) = 0
    ORDER BY 
        id_count DESC, p.player_name
    """
    
    player_id_issues = connection.execute(player_id_check).fetchdf()
    
    # Get the actual IDs for players with multiple IDs
    multiple_ids_detail = []
    missing_ids_detail = []
    
    for _, row in player_id_issues.iterrows():
        player_name = row['player_name']
        id_count = row['id_count']
        
        if id_count > 1:
            # Get all IDs for this player
            ids_query = f"""
            SELECT player_id
            FROM players
            WHERE player_name = '{player_name}'
            ORDER BY player_id
            """
            try:
                ids = connection.execute(ids_query).fetchall()
                ids = [id[0] for id in ids]
                multiple_ids_detail.append({
                    'player_name': player_name,
                    'id_count': id_count,
                    'player_ids': ', '.join(ids)
                })
            except:
                # Handle potential SQL injection issues with quotes in names
                multiple_ids_detail.append({
                    'player_name': player_name,
                    'id_count': id_count,
                    'player_ids': 'Error retrieving IDs'
                })
        else:
            # This is a missing ID case
            missing_ids_detail.append({
                'player_name': player_name,
                'matches_played': 0  # We'll update this below
            })
    
    # Get match count for players with missing IDs
    if missing_ids_detail:
        for i, player_detail in enumerate(missing_ids_detail):
            player_name = player_detail['player_name']
            # Count matches as batter
            batter_query = f"""
            SELECT COUNT(DISTINCT match_id) 
            FROM deliveries 
            WHERE batter = '{player_name}'
            """
            try:
                matches_as_batter = connection.execute(batter_query).fetchone()[0]
                
                # Count matches as bowler
                bowler_query = f"""
                SELECT COUNT(DISTINCT match_id) 
                FROM deliveries 
                WHERE bowler = '{player_name}'
                """
                matches_as_bowler = connection.execute(bowler_query).fetchone()[0]
                
                # Update the detail
                missing_ids_detail[i]['matches_played'] = max(matches_as_batter, matches_as_bowler)
            except:
                # Handle potential SQL injection issues
                missing_ids_detail[i]['matches_played'] = 'Error counting'
    
    # Check if any player IDs are associated with multiple names
    name_check = """
    SELECT 
        player_id,
        COUNT(DISTINCT player_name) as name_count,
        STRING_AGG(DISTINCT player_name, ', ') as names
    FROM 
        players
    GROUP BY 
        player_id
    HAVING 
        COUNT(DISTINCT player_name) > 1
    ORDER BY 
        name_count DESC
    """
    
    name_issues = connection.execute(name_check).fetchdf()
    
    # Get detailed information about frequency of each name variant
    name_variants_detail = []
    
    for _, row in name_issues.iterrows():
        player_id = row['player_id']
        # Get occurrence count for each name variant
        variants_query = f"""
        SELECT 
            player_name,
            COUNT(*) as occurrence_count
        FROM 
            players
        WHERE 
            player_id = '{player_id}'
        GROUP BY 
            player_name
        ORDER BY 
            occurrence_count DESC
        """
        
        try:
            variants = connection.execute(variants_query).fetchdf()
            
            # Format the variants with counts
            variants_with_counts = []
            for _, v_row in variants.iterrows():
                variants_with_counts.append(f"{v_row['player_name']} ({v_row['occurrence_count']})")
            
            name_variants_detail.append({
                'player_id': player_id,
                'name_count': row['name_count'],
                'name_variants': variants_with_counts
            })
        except:
            # Handle potential issues
            name_variants_detail.append({
                'player_id': player_id,
                'name_count': row['name_count'],
                'name_variants': ['Error retrieving variants']
            })
    
    # Get overall statistics
    total_unique_players_query = """
    SELECT COUNT(DISTINCT player_name) FROM (
        SELECT DISTINCT batter as player_name FROM deliveries
        UNION
        SELECT DISTINCT bowler FROM deliveries
        UNION
        SELECT DISTINCT non_striker FROM deliveries
        UNION
        SELECT DISTINCT wicket_player_out FROM deliveries WHERE wicket_player_out IS NOT NULL
        UNION
        SELECT DISTINCT wicket_fielder FROM deliveries WHERE wicket_fielder IS NOT NULL
    )
    """
    
    total_unique_players = connection.execute(total_unique_players_query).fetchone()[0]
    
    total_players_with_ids_query = "SELECT COUNT(DISTINCT player_name) FROM players"
    total_players_with_ids = connection.execute(total_players_with_ids_query).fetchone()[0]
    
    # Calculate percentages
    missing_ids_pct = (len(missing_ids_detail) / total_unique_players) * 100 if total_unique_players > 0 else 0
    multiple_ids_pct = (len(multiple_ids_detail) / total_unique_players) * 100 if total_unique_players > 0 else 0
    
    return {
        'player_id_issues': player_id_issues,
        'player_name_issues': name_issues,
        'multiple_ids_detail': multiple_ids_detail,
        'missing_ids_detail': missing_ids_detail,
        'name_variants_detail': name_variants_detail,
        'stats': {
            'total_unique_players': total_unique_players,
            'total_players_with_ids': total_players_with_ids,
            'players_missing_ids': len(missing_ids_detail),
            'players_with_multiple_ids': len(multiple_ids_detail),
            'ids_with_multiple_names': len(name_issues),
            'missing_ids_pct': missing_ids_pct,
            'multiple_ids_pct': multiple_ids_pct
        }
    }

def print_player_consistency_report(player_consistency, max_rows=20):
    """Print a comprehensive report of player ID and name consistency issues"""
    from tabulate import tabulate
    
    stats = player_consistency['stats']
    
    print("\n=========== PLAYER ID CONSISTENCY REPORT ===========\n")
    
    print("Overall Statistics:")
    print(f"Total unique players in the database: {stats['total_unique_players']:,}")
    print(f"Players with IDs in the players table: {stats['total_players_with_ids']:,}")
    print(f"Players missing IDs: {stats['players_missing_ids']:,} ({stats['missing_ids_pct']:.2f}%)")
    print(f"Players with multiple IDs: {stats['players_with_multiple_ids']} ({stats['multiple_ids_pct']:.2f}%)")
    print(f"IDs associated with multiple names: {stats['ids_with_multiple_names']}")
    
    # Print players with multiple IDs
    print("\n=== Players with Multiple IDs ===\n")
    if player_consistency['multiple_ids_detail']:
        multiple_ids_data = []
        for item in player_consistency['multiple_ids_detail'][:max_rows]:
            multiple_ids_data.append([
                item['player_name'],
                item['id_count'],
                item['player_ids']
            ])
        
        print(tabulate(
            multiple_ids_data,
            headers=["Player Name", "ID Count", "Player IDs"],
            tablefmt="grid"
        ))
        
        if len(player_consistency['multiple_ids_detail']) > max_rows:
            print(f"\n... and {len(player_consistency['multiple_ids_detail']) - max_rows} more players with multiple IDs")
    else:
        print("No players with multiple IDs found.")
    
    # Print players with missing IDs
    print("\n=== Players with Missing IDs ===\n")
    if player_consistency['missing_ids_detail']:
        # Sort by matches played (descending) to show most significant missing IDs first
        missing_ids_sorted = sorted(
            player_consistency['missing_ids_detail'], 
            key=lambda x: x['matches_played'] if isinstance(x['matches_played'], int) else 0,
            reverse=True
        )
        
        missing_ids_data = []
        for item in missing_ids_sorted[:max_rows]:
            missing_ids_data.append([
                item['player_name'],
                item['matches_played']
            ])
        
        print(tabulate(
            missing_ids_data,
            headers=["Player Name", "Matches Played"],
            tablefmt="grid"
        ))
        
        if len(missing_ids_sorted) > max_rows:
            print(f"\n... and {len(missing_ids_sorted) - max_rows} more players with missing IDs")
    else:
        print("No players with missing IDs found.")
    
    # Print IDs with multiple name variants
    print("\n=== Player IDs Associated with Multiple Names ===\n")
    if player_consistency['name_variants_detail']:
        name_variants_data = []
        for item in player_consistency['name_variants_detail'][:max_rows]:
            name_variants_data.append([
                item['player_id'],
                item['name_count'],
                "\n".join(item['name_variants'][:5]) + 
                (f"\n... and {len(item['name_variants']) - 5} more variants" 
                 if len(item['name_variants']) > 5 else "")
            ])
        
        print(tabulate(
            name_variants_data,
            headers=["Player ID", "Name Variant Count", "Name Variants (occurrence count)"],
            tablefmt="grid"
        ))
        
        if len(player_consistency['name_variants_detail']) > max_rows:
            print(f"\n... and {len(player_consistency['name_variants_detail']) - max_rows} more IDs with multiple names")
    else:
        print("No player IDs with multiple names found.")
    
# Execute the enhanced player ID consistency check
player_consistency = check_player_id_consistency(conn)
print_player_consistency_report(player_consistency)


import chardet
from collections import defaultdict

def check_encoding_of_varchar_columns(connection, exclude_tables=None):
    """
    Check the encoding of all varchar columns across tables in a DuckDB database
    
    Args:
        connection: A DuckDB connection object
        exclude_tables: Optional list of tables to exclude
    
    Returns:
        A dictionary with encoding statistics for each table and column
    """
    if exclude_tables is None:
        exclude_tables = []
    
    # Get all tables in the database
    tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = [table[0] for table in tables if table[0] not in exclude_tables]
    
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    sample_sizes = {}
    
    for table in table_names:
        # Get column information for the table
        columns_info = connection.execute(f"PRAGMA table_info({table})").fetchall()
        varchar_columns = [col[1] for col in columns_info if 'varchar' in col[2].lower()]
        
        if not varchar_columns:
            continue
            
        print(f"Analyzing encoding for {len(varchar_columns)} varchar columns in table: {table}")
        
        # For each varchar column, check the encoding of values
        for column in varchar_columns:
            # Get a sample of non-null values from the column
            sample = connection.execute(
                f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT 1000"
            ).fetchall()
            sample = [row[0] for row in sample if row[0] is not None]
            
            if not sample:
                continue
                
            sample_sizes[(table, column)] = len(sample)
            
            # Check encoding for each value
            for value in sample:
                if not isinstance(value, str):
                    continue
                    
                # Use chardet to detect encoding
                result = chardet.detect(value.encode('utf-8', errors='ignore'))
                encoding = result['encoding']
                confidence = result['confidence']
                
                # Store results
                results[table][column][encoding] += 1
    
    # Format results
    formatted_results = {}
    for table in results:
        formatted_results[table] = {}
        for column in results[table]:
            total_samples = sample_sizes.get((table, column), 0)
            if total_samples > 0:
                encodings = {}
                for encoding, count in results[table][column].items():
                    encodings[encoding] = {
                        'count': count,
                        'percentage': round((count / total_samples) * 100, 2)
                    }
                formatted_results[table][column] = {
                    'sample_size': total_samples,
                    'encodings': encodings
                }
    
    return formatted_results

def print_encoding_report(results):
    """Print a formatted report of encoding results"""
    for table, columns in results.items():
        print(f"\n=== Table: {table} ===")
        for column, data in columns.items():
            print(f"\nColumn: {column} (Sample size: {data['sample_size']})")
            print("Encodings detected:")
            
            for encoding, stats in data['encodings'].items():
                print(f"  {encoding}: {stats['count']} values ({stats['percentage']}%)")
            
            # Highlight potential issues
            encodings = list(data['encodings'].keys())
            if len(encodings) > 1:
                print(⚠️ WARNING: Multiple encodings detected in this column")

def main():
    # Connect to your DuckDB database
    conn = duckdb.connect('cricket_analytics.db')  # Replace with your database file
    
    try:
        # Check encodings
        results = check_encoding_of_varchar_columns(conn)
        
        # Print report
        print("\n===== ENCODING ANALYSIS REPORT =====")
        print_encoding_report(results)
        
        # Summary
        tables_with_issues = sum(1 for table in results.values() 
                               for column in table.values() 
                               if len(column['encodings']) > 1)
        
        print("\n===== SUMMARY =====")
        print(f"Total tables analyzed: {len(results)}")
        print(f"Tables with potential encoding issues: {tables_with_issues}")
        
        if tables_with_issues > 0:
            print("\nRecommendations:")
            print("- Consider standardizing to UTF-8 for all text data")
            print("- Check for special characters in player names and venues")
            print("- Verify that data import processes handle encodings correctly")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
