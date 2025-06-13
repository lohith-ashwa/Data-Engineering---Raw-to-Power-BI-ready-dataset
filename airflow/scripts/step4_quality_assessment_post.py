import duckdb
import pandas as pd
import os

# Set up paths relative to Airflow directory
BASE_DIR = '/home/lohit/airflow'
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'cricket_analytics.db')

def explore_table_structure(conn, table_name):
    """Explore table structure and basic stats"""
    try:
        # Get schema info
        schema_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
        
        # Get row count
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        return {
            'table_name': table_name,
            'columns': len(schema_info),
            'rows': count,
            'schema': schema_info
        }
    except Exception as e:
        print(f"Error exploring table {table_name}: {e}")
        return None

def analyze_column_ranges(conn):
    """Analyze min and max values for numeric columns"""
    columns_to_analyze = {
        'deliveries': ['over_number', 'ball_number', 'batter_runs', 'extras', 'total_runs'],
        'innings': ['innings_number'],
        'matches': ['overs', 'outcome_by_runs', 'outcome_by_wickets'],
        'overs': ['over_number', 'total_runs', 'wickets', 'num_deliveries']
    }
    
    results = []
    
    for table, columns in columns_to_analyze.items():
        for col in columns:
            try:
                query = f"SELECT MIN({col}) as min_value, MAX({col}) as max_value FROM {table}"
                result = conn.execute(query).fetchone()
                min_val, max_val = result
                
                results.append({
                    'Table': table,
                    'Column': col,
                    'Min Value': min_val,
                    'Max Value': max_val
                })
            except Exception as e:
                results.append({
                    'Table': table,
                    'Column': col,
                    'Min Value': f"Error: {str(e)}",
                    'Max Value': f"Error: {str(e)}"
                })
    
    return pd.DataFrame(results)

def apply_type_conversions(conn):
    """Apply data type conversions for optimization"""
    type_conversions = {
        'deliveries': {
            'over_number': 'SMALLINT',
            'ball_number': 'TINYINT',
            'batter_runs': 'TINYINT',
            'extras': 'TINYINT',
            'total_runs': 'TINYINT',
            'extras_value': 'TINYINT',
            'is_wicket': 'BOOLEAN'
        },
        'innings': {
            'innings_number': 'TINYINT'
        },
        'matches': {
            'overs': 'DECIMAL(4,1)',
            'outcome_by_runs': 'SMALLINT',
            'outcome_by_wickets': 'TINYINT'
        },
        'overs': {
            'over_number': 'SMALLINT',
            'total_runs': 'TINYINT',
            'wickets': 'TINYINT',
            'num_deliveries': 'TINYINT'
        }
    }
    
    conversions_applied = []
    
    for table, columns in type_conversions.items():
        try:
            # Get current columns
            table_columns = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
            column_names = [col[1] for col in table_columns]
            
            # Create temporary table with new schema
            temp_table = f"{table}_temp"
            create_temp_sql = f"CREATE TABLE {temp_table} AS SELECT "
            
            column_clauses = []
            for col in column_names:
                if col in columns:
                    column_clauses.append(f"CAST({col} AS {columns[col]}) AS {col}")
                    conversions_applied.append(f"{table}.{col} -> {columns[col]}")
                else:
                    column_clauses.append(col)
            
            create_temp_sql += ", ".join(column_clauses)
            create_temp_sql += f" FROM {table}"
            
            # Execute conversion
            conn.execute(create_temp_sql)
            conn.execute(f"DROP TABLE {table}")
            conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")
            
            print(f"Converted table: {table}")
            
        except Exception as e:
            print(f"Error converting table {table}: {e}")
    
    return conversions_applied

def analyze_null_values(conn):
    """Check for NULL values in each table"""
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
    tables = [table[0] for table in tables]
    
    null_stats = {}
    
    for table in tables:
        columns = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        column_names = [col[1] for col in columns]
        
        table_stats = {}
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        for col in column_names:
            try:
                null_count = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL").fetchone()[0]
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
                
                table_stats[col] = {
                    'null_count': null_count,
                    'null_percentage': null_percentage
                }
            except Exception as e:
                print(f"Error checking nulls in {table}.{col}: {e}")
        
        null_stats[table] = {
            'total_rows': total_rows,
            'columns': table_stats
        }
    
    return null_stats

def cricket_domain_validation(conn):
    """Perform cricket-specific data validation"""
    validation_results = {}
    
    # Check for invalid batter runs (>7)
    try:
        invalid_runs_query = """
        SELECT COUNT(*) as count_invalid
        FROM deliveries
        WHERE batter_runs > 7
        """
        invalid_runs = conn.execute(invalid_runs_query).fetchone()[0]
        validation_results['invalid_batter_runs'] = invalid_runs
    except Exception as e:
        validation_results['invalid_batter_runs'] = f"Error: {e}"
    
    # Check 7-run deliveries (should be rare but valid)
    try:
        seven_runs_query = """
        SELECT COUNT(*) as count_7_runs,
               (SELECT COUNT(*) FROM deliveries) as total_deliveries
        FROM deliveries
        WHERE batter_runs = 7
        """
        result = conn.execute(seven_runs_query).fetchone()
        seven_runs, total_deliveries = result
        seven_runs_pct = (seven_runs / total_deliveries * 100) if total_deliveries > 0 else 0
        validation_results['seven_run_deliveries'] = {
            'count': seven_runs,
            'percentage': seven_runs_pct
        }
    except Exception as e:
        validation_results['seven_run_deliveries'] = f"Error: {e}"
    
    # Check for matches where actual overs exceed declared limit
    try:
        exceeded_overs_query = """
        WITH match_overs AS (
            SELECT 
                m.match_id,
                m.match_type,
                m.overs as declared_overs,
                MAX(o.over_number) + 1 as max_over_number
            FROM 
                matches m
            JOIN 
                innings i ON m.match_id = i.match_id
            JOIN 
                overs o ON i.innings_id = o.innings_id
            WHERE
                m.overs IS NOT NULL AND
                m.match_type IN ('ODI', 'T20', 'IT20', 'ODM', 'T20M')
            GROUP BY 
                m.match_id, m.match_type, m.overs
        )
        SELECT COUNT(*)
        FROM match_overs
        WHERE max_over_number > declared_overs * 1.1
        """
        exceeded_count = conn.execute(exceeded_overs_query).fetchone()[0]
        validation_results['matches_exceeded_overs'] = exceeded_count
    except Exception as e:
        validation_results['matches_exceeded_overs'] = f"Error: {e}"
    
    # Check over length distribution
    try:
        over_length_query = """
        SELECT 
            num_deliveries,
            COUNT(*) as over_count
        FROM overs
        GROUP BY num_deliveries
        ORDER BY num_deliveries
        """
        over_lengths = conn.execute(over_length_query).fetchall()
        validation_results['over_length_distribution'] = dict(over_lengths)
    except Exception as e:
        validation_results['over_length_distribution'] = f"Error: {e}"
    
    return validation_results

def check_player_consistency(conn):
    """Check player ID and name consistency"""
    try:
        # Check for players with missing IDs
        missing_ids_query = """
        WITH all_player_names AS (
            SELECT DISTINCT batter as player_name FROM deliveries WHERE batter IS NOT NULL
            UNION
            SELECT DISTINCT bowler FROM deliveries WHERE bowler IS NOT NULL
            UNION
            SELECT DISTINCT non_striker FROM deliveries WHERE non_striker IS NOT NULL
            UNION
            SELECT DISTINCT wicket_player_out FROM deliveries WHERE wicket_player_out IS NOT NULL
        )
        SELECT COUNT(*)
        FROM all_player_names apn
        LEFT JOIN players p ON apn.player_name = p.player_name
        WHERE p.player_id IS NULL
        """
        missing_ids = conn.execute(missing_ids_query).fetchone()[0]
        
        # Check for players with multiple IDs
        multiple_ids_query = """
        SELECT COUNT(DISTINCT player_id)
        FROM players
        GROUP BY player_name
        HAVING COUNT(DISTINCT player_id) > 1
        """
        multiple_ids_result = conn.execute(multiple_ids_query).fetchall()
        multiple_ids = len(multiple_ids_result)
        
        # Count synthetic IDs
        synthetic_ids_query = """
        SELECT COUNT(*)
        FROM players
        WHERE player_id LIKE 'SYNTH_%'
        """
        synthetic_ids = conn.execute(synthetic_ids_query).fetchone()[0]
        
        return {
            'players_missing_ids': missing_ids,
            'players_with_multiple_ids': multiple_ids,
            'synthetic_player_ids': synthetic_ids
        }
        
    except Exception as e:
        return {'error': str(e)}

def generate_summary_report(conn):
    """Generate overall summary report"""
    summary = {}
    
    # Get table counts
    tables = ['matches', 'players', 'innings', 'overs', 'deliveries']
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            summary[f"{table}_count"] = count
        except:
            summary[f"{table}_count"] = 0
    
    # Get match type distribution
    try:
        match_types_query = """
        SELECT match_type, COUNT(*) as count
        FROM matches
        GROUP BY match_type
        ORDER BY count DESC
        """
        match_types = conn.execute(match_types_query).fetchall()
        summary['match_types'] = dict(match_types)
    except:
        summary['match_types'] = {}
    
    # Get date range
    try:
        date_range_query = """
        SELECT MIN(date) as earliest, MAX(date) as latest
        FROM matches
        WHERE date IS NOT NULL
        """
        date_range = conn.execute(date_range_query).fetchone()
        summary['date_range'] = {
            'earliest': str(date_range[0]) if date_range[0] else None,
            'latest': str(date_range[1]) if date_range[1] else None
        }
    except:
        summary['date_range'] = {'earliest': None, 'latest': None}
    
    return summary

def main():
    """Main function for post-wrangling quality assessment"""
    print("Starting post-wrangling quality assessment...")
    
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Get list of tables
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        print(f"Found tables: {table_names}")
        
        # Explore table structures
        table_info = {}
        for table_name in table_names:
            info = explore_table_structure(conn, table_name)
            if info:
                table_info[table_name] = info
                print(f"Table {table_name}: {info['rows']} rows, {info['columns']} columns")
        
        # Analyze column ranges
        print("Analyzing column ranges...")
        range_analysis = analyze_column_ranges(conn)
        print(f"Analyzed {len(range_analysis)} columns")
        
        # Apply type conversions
        print("Applying type conversions...")
        conversions = apply_type_conversions(conn)
        print(f"Applied {len(conversions)} type conversions")
        
        # Analyze NULL values
        print("Analyzing NULL values...")
        null_analysis = analyze_null_values(conn)
        
        # Print NULL summary
        for table, stats in null_analysis.items():
            columns_with_nulls = sum(1 for col_stats in stats['columns'].values() 
                                   if col_stats['null_count'] > 0)
            if columns_with_nulls > 0:
                print(f"Table {table}: {columns_with_nulls} columns have NULL values")
        
        # Cricket domain validation
        print("Performing cricket-specific validation...")
        validation_results = cricket_domain_validation(conn)
        
        print("Validation results:")
        for key, value in validation_results.items():
            print(f"- {key}: {value}")
        
        # Player consistency check
        print("Checking player consistency...")
        player_consistency = check_player_consistency(conn)
        
        print("Player consistency results:")
        for key, value in player_consistency.items():
            print(f"- {key}: {value}")
        
        # Generate summary report
        summary = generate_summary_report(conn)
        
        print("\nSummary Report:")
        print(f"- Total matches: {summary.get('matches_count', 0)}")
        print(f"- Total players: {summary.get('players_count', 0)}")
        print(f"- Total deliveries: {summary.get('deliveries_count', 0)}")
        print(f"- Date range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")
        print(f"- Match types: {len(summary.get('match_types', {}))}")
        
        # Save summary to file
        summary_path = os.path.join(DATA_DIR, 'quality_assessment_summary.txt')
        with open(summary_path, 'w') as f:
            f.write("Post-Wrangling Quality Assessment Summary\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("Table Information:\n")
            for table, info in table_info.items():
                f.write(f"- {table}: {info['rows']} rows, {info['columns']} columns\n")
            
            f.write(f"\nType Conversions Applied: {len(conversions)}\n")
            
            f.write("\nValidation Results:\n")
            for key, value in validation_results.items():
                f.write(f"- {key}: {value}\n")
            
            f.write("\nPlayer Consistency:\n")
            for key, value in player_consistency.items():
                f.write(f"- {key}: {value}\n")
            
            f.write(f"\nSummary:\n")
            for key, value in summary.items():
                f.write(f"- {key}: {value}\n")
        
        print(f"Summary saved to {summary_path}")
        
        return {
            'status': 'success',
            'tables_processed': len(table_names),
            'conversions_applied': len(conversions),
            'summary': summary
        }
        
    except Exception as e:
        print(f"Error during quality assessment: {e}")
        return {'status': 'error', 'message': str(e)}
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()