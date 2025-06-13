import duckdb
import pandas as pd
import os

# Set up paths relative to Airflow directory
BASE_DIR = '/home/lohit/airflow'
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'cricket_analytics.db')

def add_deliveries_features(conn):
    """Add calculated features to deliveries table"""
    deliveries_updates = [
        # Add new columns
        "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS boundary_type VARCHAR;",
        "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS is_dot_ball BOOLEAN;",
        "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS cumulative_runs_in_innings INTEGER;",
        "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS ball_in_over INTEGER;",
        
        # Update boundary_type and is_dot_ball
        """
        UPDATE deliveries SET
          boundary_type = CASE
            WHEN batter_runs = 4 THEN 'four'
            WHEN batter_runs = 6 THEN 'six'
            ELSE NULL
          END,
          is_dot_ball = CASE 
            WHEN total_runs = 0 THEN TRUE 
            ELSE FALSE 
          END;
        """,
        
        # Update cumulative_runs_in_innings
        """
        UPDATE deliveries SET
          cumulative_runs_in_innings = t.cum_runs
        FROM (
          SELECT 
            delivery_id, 
            SUM(total_runs) OVER (
              PARTITION BY innings_id 
              ORDER BY over_number, ball_number
            ) AS cum_runs
          FROM deliveries
        ) t
        WHERE deliveries.delivery_id = t.delivery_id;
        """,
        
        # Update ball_in_over
        """
        UPDATE deliveries SET
          ball_in_over = t.ball_num
        FROM (
          SELECT 
            delivery_id, 
            ROW_NUMBER() OVER (
              PARTITION BY over_id 
              ORDER BY ball_number
            ) AS ball_num
          FROM deliveries
        ) t
        WHERE deliveries.delivery_id = t.delivery_id;
        """
    ]
    
    for sql in deliveries_updates:
        conn.execute(sql)
    
    print("Added features to deliveries table")

def add_innings_features(conn):
    """Add calculated features to innings table"""
    innings_updates = [
        # Add new columns
        "ALTER TABLE innings ADD COLUMN IF NOT EXISTS total_runs INTEGER;",
        "ALTER TABLE innings ADD COLUMN IF NOT EXISTS total_wickets INTEGER;",
        "ALTER TABLE innings ADD COLUMN IF NOT EXISTS run_rate DOUBLE;",
        "ALTER TABLE innings ADD COLUMN IF NOT EXISTS boundary_count INTEGER;",
        "ALTER TABLE innings ADD COLUMN IF NOT EXISTS dot_ball_percentage DOUBLE;",
        "ALTER TABLE innings ADD COLUMN IF NOT EXISTS powerplay_runs INTEGER;",
        
        # Update total_runs
        """
        UPDATE innings SET
        total_runs = (SELECT SUM(total_runs) 
                     FROM deliveries 
                     WHERE deliveries.innings_id = innings.innings_id);
        """,
        
        # Update total_wickets
        """
        UPDATE innings SET
        total_wickets = (SELECT SUM(CASE WHEN is_wicket = 1 THEN 1 ELSE 0 END) 
                         FROM deliveries 
                         WHERE deliveries.innings_id = innings.innings_id);
        """,
        
        # Update run_rate
        """
        UPDATE innings SET
        run_rate = (SELECT SUM(total_runs) FROM deliveries WHERE deliveries.innings_id = innings.innings_id) /
                  NULLIF((SELECT MAX(over_number) + (MAX(ball_number)*1.0/6) 
                          FROM deliveries 
                          WHERE deliveries.innings_id = innings.innings_id), 0);
        """,
        
        # Update boundary_count
        """
        UPDATE innings SET
        boundary_count = (SELECT COUNT(*) 
                         FROM deliveries 
                         WHERE deliveries.innings_id = innings.innings_id 
                         AND (batter_runs = 4 OR batter_runs = 6));
        """,
        
        # Update dot_ball_percentage
        """
        UPDATE innings SET
        dot_ball_percentage = (SELECT COUNT(*) * 100.0 / NULLIF(COUNT(*), 0)
                              FROM deliveries 
                              WHERE deliveries.innings_id = innings.innings_id 
                              AND total_runs = 0);
        """,
        
        # Update powerplay_runs
        """
        UPDATE innings SET
        powerplay_runs = (SELECT SUM(d.total_runs)
                          FROM deliveries d
                          JOIN overs o ON d.over_id = o.over_id
                          WHERE d.innings_id = innings.innings_id
                          AND innings.powerplay_start_over IS NOT NULL 
                          AND innings.powerplay_end_over IS NOT NULL
                          AND o.over_number >= innings.powerplay_start_over
                          AND o.over_number <= innings.powerplay_end_over);
        """
    ]
    
    for sql in innings_updates:
        conn.execute(sql)
    
    print("Added features to innings table")

def add_matches_features(conn):
    """Add calculated features to matches table"""
    matches_updates = [
        # Add new columns
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS match_result VARCHAR;",
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS margin_description VARCHAR;",
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS chasing_team VARCHAR;",
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS setting_team VARCHAR;",
        
        # Update match_result
        """
        UPDATE matches SET
        match_result = CASE
            WHEN outcome_winner IS NULL THEN 'No Result'
            WHEN outcome_winner = team1 THEN team1 || ' won'
            WHEN outcome_winner = team2 THEN team2 || ' won'
            ELSE 'Tie'
        END;
        """,
        
        # Update margin_description
        """
        UPDATE matches SET
        margin_description = CASE
            WHEN outcome_by_runs > 0 THEN outcome_by_runs || ' runs'
            WHEN outcome_by_wickets > 0 THEN outcome_by_wickets || ' wickets'
            WHEN outcome_method IS NOT NULL THEN outcome_method
            ELSE NULL
        END;
        """,
        
        # Update chasing_team (team that bats second)
        """
        UPDATE matches SET
        chasing_team = (SELECT bowling_team
                       FROM innings
                       WHERE innings.match_id = matches.match_id
                       AND innings_number = 1
                       LIMIT 1);
        """,
        
        # Update setting_team (team that bats first)
        """
        UPDATE matches SET
        setting_team = (SELECT batting_team
                       FROM innings
                       WHERE innings.match_id = matches.match_id
                       AND innings_number = 1
                       LIMIT 1);
        """
    ]
    
    for sql in matches_updates:
        conn.execute(sql)
    
    print("Added features to matches table")

def add_overs_features(conn):
    """Add calculated features to overs table"""
    overs_updates = [
        # Add new columns
        "ALTER TABLE overs ADD COLUMN IF NOT EXISTS run_rate DOUBLE;",
        "ALTER TABLE overs ADD COLUMN IF NOT EXISTS is_powerplay BOOLEAN;",
        "ALTER TABLE overs ADD COLUMN IF NOT EXISTS boundaries_in_over INTEGER;",
        "ALTER TABLE overs ADD COLUMN IF NOT EXISTS dot_balls_in_over INTEGER;",
        "ALTER TABLE overs ADD COLUMN IF NOT EXISTS cumulative_runs_in_innings INTEGER;",
        "ALTER TABLE overs ADD COLUMN IF NOT EXISTS cumulative_wickets_in_innings INTEGER;",
        
        # Update run_rate
        """
        UPDATE overs SET
        run_rate = total_runs * 1.0 / CASE WHEN num_deliveries > 0 THEN num_deliveries / 6.0 ELSE 1 END;
        """,
        
        # Update is_powerplay
        """
        UPDATE overs SET
        is_powerplay = CASE
            WHEN over_number >= (SELECT powerplay_start_over FROM innings WHERE innings.innings_id = overs.innings_id 
                              AND powerplay_start_over IS NOT NULL)
            AND over_number <= (SELECT powerplay_end_over FROM innings WHERE innings.innings_id = overs.innings_id
                             AND powerplay_end_over IS NOT NULL)
            THEN TRUE
            ELSE FALSE
        END;
        """,
        
        # Update boundaries_in_over
        """
        UPDATE overs SET
        boundaries_in_over = (SELECT COUNT(*)
                              FROM deliveries
                              WHERE deliveries.over_id = overs.over_id
                              AND (batter_runs = 4 OR batter_runs = 6));
        """,
        
        # Update dot_balls_in_over
        """
        UPDATE overs SET
        dot_balls_in_over = (SELECT COUNT(*)
                             FROM deliveries
                             WHERE deliveries.over_id = overs.over_id
                             AND total_runs = 0);
        """
    ]
    
    # Execute basic updates first
    for sql in overs_updates:
        conn.execute(sql)
    
    # Handle cumulative values with temporary table
    cumulative_updates = [
        """
        CREATE TEMPORARY TABLE temp_cumulative AS
        SELECT 
            over_id,
            SUM(total_runs) OVER (PARTITION BY innings_id ORDER BY over_number) AS cum_runs,
            SUM(wickets) OVER (PARTITION BY innings_id ORDER BY over_number) AS cum_wickets
        FROM overs;
        """,
        
        """
        UPDATE overs SET
        cumulative_runs_in_innings = temp_cumulative.cum_runs,
        cumulative_wickets_in_innings = temp_cumulative.cum_wickets
        FROM temp_cumulative
        WHERE overs.over_id = temp_cumulative.over_id;
        """,
        
        "DROP TABLE IF EXISTS temp_cumulative;"
    ]
    
    for sql in cumulative_updates:
        conn.execute(sql)
    
    print("Added features to overs table")

def add_players_features(conn):
    """Add calculated features to players table"""
    # Group 1: Add all columns first
    players_columns = [
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS total_matches_played INTEGER;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS total_runs_scored INTEGER;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS batting_strike_rate DOUBLE;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS total_wickets_taken INTEGER;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS bowling_economy_rate DOUBLE;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS batting_average DOUBLE;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS bowling_average DOUBLE;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS highest_score INTEGER;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS half_centuries INTEGER;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS centuries INTEGER;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS test_matches INTEGER;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS odi_matches INTEGER;",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS t20_matches INTEGER;"
    ]
    
    for sql in players_columns:
        conn.execute(sql)
    
    # Group 2: Basic statistics updates (limit to essential ones for Airflow performance)
    players_basic_updates = [
        # Total matches played
        """
        UPDATE players SET
        total_matches_played = (
            SELECT COUNT(DISTINCT match_id) 
            FROM deliveries 
            WHERE batter_id = players.player_id 
            OR bowler_id = players.player_id 
            OR non_striker_id = players.player_id
        );
        """,
        
        # Total runs scored
        """
        UPDATE players SET
        total_runs_scored = (
            SELECT SUM(batter_runs) 
            FROM deliveries 
            WHERE batter_id = players.player_id
        );
        """,
        
        # Batting strike rate
        """
        UPDATE players SET
        batting_strike_rate = (
            SELECT SUM(batter_runs) * 100.0 / NULLIF(COUNT(*), 0)
            FROM deliveries 
            WHERE batter_id = players.player_id
        );
        """,
        
        # Total wickets taken
        """
        UPDATE players SET
        total_wickets_taken = (
            SELECT COUNT(*) 
            FROM deliveries 
            WHERE bowler_id = players.player_id 
            AND is_wicket = 1 
            AND wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
        );
        """,
        
        # Bowling economy rate
        """
        UPDATE players SET
        bowling_economy_rate = (
            SELECT SUM(total_runs) * 6.0 / NULLIF(COUNT(*), 0)
            FROM deliveries 
            WHERE bowler_id = players.player_id
        );
        """
    ]
    
    for sql in players_basic_updates:
        conn.execute(sql)
    
    # Group 3: More complex calculations (essential ones only)
    players_complex_updates = [
        # Highest score
        """
        UPDATE players SET
        highest_score = (
            WITH innings_runs AS (
                SELECT 
                    batter_id,
                    SUM(batter_runs) AS total_runs
                FROM deliveries
                GROUP BY match_id, innings_id, batter_id
            )
            SELECT MAX(total_runs)
            FROM innings_runs
            WHERE batter_id = players.player_id
        );
        """,
        
        # Half centuries
        """
        UPDATE players SET
        half_centuries = (
            WITH innings_runs AS (
                SELECT 
                    batter_id,
                    SUM(batter_runs) AS total_runs
                FROM deliveries
                GROUP BY match_id, innings_id, batter_id
            )
            SELECT COUNT(*)
            FROM innings_runs
            WHERE batter_id = players.player_id
            AND total_runs >= 50 
            AND total_runs < 100
        );
        """,
        
        # Centuries
        """
        UPDATE players SET
        centuries = (
            WITH innings_runs AS (
                SELECT 
                    batter_id,
                    SUM(batter_runs) AS total_runs
                FROM deliveries
                GROUP BY match_id, innings_id, batter_id
            )
            SELECT COUNT(*)
            FROM innings_runs
            WHERE batter_id = players.player_id
            AND total_runs >= 100
        );
        """
    ]
    
    for sql in players_complex_updates:
        conn.execute(sql)
    
    print("Added features to players table")

def create_player_match_stats_table(conn):
    """Create comprehensive player match statistics table"""
    
    # Drop table if exists
    conn.execute("DROP TABLE IF EXISTS player_match_stats")
    
    player_match_stats_sql = """
    CREATE TABLE player_match_stats AS
    SELECT 
        d.match_id,
        m.date,
        m.venue,
        m.match_type,
        m.team1,
        m.team2,
        p.player_id,
        p.player_name,
        CASE 
            WHEN EXISTS (SELECT 1 FROM innings i WHERE i.match_id = d.match_id AND 
                        i.batting_team = m.team1 AND 
                        p.player_id IN (SELECT batter_id FROM deliveries WHERE innings_id = i.innings_id))
            THEN m.team1
            ELSE m.team2
        END AS player_team,
        
        -- Batting stats
        COUNT(DISTINCT CASE WHEN d.batter_id = p.player_id THEN d.innings_id END) AS innings_batted,
        SUM(CASE WHEN d.batter_id = p.player_id THEN 1 ELSE 0 END) AS balls_faced,
        SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) AS runs_scored,
        SUM(CASE WHEN d.batter_id = p.player_id AND d.batter_runs = 4 THEN 1 ELSE 0 END) AS fours,
        SUM(CASE WHEN d.batter_id = p.player_id AND d.batter_runs = 6 THEN 1 ELSE 0 END) AS sixes,
        
        -- Bowling stats
        SUM(CASE WHEN d.bowler_id = p.player_id THEN 1 ELSE 0 END) AS balls_bowled,
        SUM(CASE WHEN d.bowler_id = p.player_id THEN d.total_runs ELSE 0 END) AS runs_conceded,
        SUM(CASE WHEN d.bowler_id = p.player_id AND d.is_wicket = 1 
            THEN 1 ELSE 0 END) AS wickets_taken,
        
        -- Performance flags
        CASE WHEN SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) >= 50 
             AND SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) < 100 
             THEN TRUE ELSE FALSE END AS is_half_century,
        
        CASE WHEN SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) >= 100 
             THEN TRUE ELSE FALSE END AS is_century,
        
        CASE WHEN SUM(CASE WHEN d.bowler_id = p.player_id AND d.is_wicket = 1 
                            THEN 1 ELSE 0 END) >= 5 
             THEN TRUE ELSE FALSE END AS is_five_wicket_haul,
        
        -- Match result for player
        CASE WHEN m.player_of_match_id = p.player_id THEN TRUE ELSE FALSE END AS is_player_of_match
        
    FROM 
        deliveries d
    JOIN 
        matches m ON d.match_id = m.match_id
    JOIN 
        players p ON p.player_id IN (d.batter_id, d.bowler_id, d.non_striker_id)
    GROUP BY 
        d.match_id, m.date, m.venue, m.match_type, m.team1, m.team2, 
        p.player_id, p.player_name, m.player_of_match_id
    """
    
    print("Creating player_match_stats table - this may take some time...")
    conn.execute(player_match_stats_sql)
    
    # Add indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pms_match_id ON player_match_stats(match_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pms_player_id ON player_match_stats(player_id)")
    
    print("Successfully created player_match_stats table")

def verify_features(conn):
    """Verify that features were added successfully"""
    verification_queries = {
        'deliveries_features': "SELECT COUNT(*) FROM deliveries WHERE boundary_type IS NOT NULL",
        'innings_features': "SELECT COUNT(*) FROM innings WHERE total_runs IS NOT NULL",
        'matches_features': "SELECT COUNT(*) FROM matches WHERE match_result IS NOT NULL",
        'overs_features': "SELECT COUNT(*) FROM overs WHERE run_rate IS NOT NULL",
        'players_features': "SELECT COUNT(*) FROM players WHERE total_matches_played IS NOT NULL",
        'player_match_stats': "SELECT COUNT(*) FROM player_match_stats"
    }
    
    verification_results = {}
    
    for feature_type, query in verification_queries.items():
        try:
            result = conn.execute(query).fetchone()[0]
            verification_results[feature_type] = result
        except Exception as e:
            verification_results[feature_type] = f"Error: {e}"
    
    return verification_results

def main():
    """Main function for feature engineering"""
    print("Starting feature engineering...")
    
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Add features to each table
        print("Adding features to deliveries table...")
        add_deliveries_features(conn)
        
        print("Adding features to innings table...")
        add_innings_features(conn)
        
        print("Adding features to matches table...")
        add_matches_features(conn)
        
        print("Adding features to overs table...")
        add_overs_features(conn)
        
        print("Adding features to players table...")
        add_players_features(conn)
        
        print("Creating player match statistics table...")
        create_player_match_stats_table(conn)
        
        # Verify features
        print("Verifying feature engineering...")
        verification_results = verify_features(conn)
        
        print("Feature engineering verification results:")
        for feature_type, result in verification_results.items():
            print(f"- {feature_type}: {result}")
        
        # Save verification results
        verification_path = os.path.join(DATA_DIR, 'feature_engineering_verification.txt')
        with open(verification_path, 'w') as f:
            f.write("Feature Engineering Verification Results\n")
            f.write("=" * 50 + "\n\n")
            
            for feature_type, result in verification_results.items():
                f.write(f"{feature_type}: {result}\n")
        
        print(f"Verification results saved to {verification_path}")
        
        return {
            'status': 'success',
            'verification_results': verification_results
        }
        
    except Exception as e:
        print(f"Error during feature engineering: {e}")
        return {'status': 'error', 'message': str(e)}
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()