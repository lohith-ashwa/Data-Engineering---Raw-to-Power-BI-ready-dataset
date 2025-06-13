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


# Breaking down the ALTER TABLE statements
deliveries_update_sql = [
    # 1. Add boundary_type column
    """
    ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS boundary_type VARCHAR;
    """,
    
    # 2. Add is_dot_ball column
    """
    ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS is_dot_ball BOOLEAN;
    """,
    
    # 3. Add cumulative_runs_in_innings column
    """
    ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS cumulative_runs_in_innings INTEGER;
    """,
    
    # 4. Add ball_in_over column
    """
    ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS ball_in_over INTEGER;
    """,
    
    # 5. Update boundary_type and is_dot_ball
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
    
    # 6. Update cumulative_runs_in_innings
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
    
    # 7. Update ball_in_over
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

# Execute each SQL statement separately
for sql in deliveries_update_sql:
    conn.execute(sql)
    
print("Successfully added new columns to the deliveries table")


# Breaking down the ALTER TABLE and UPDATE statements for innings table
innings_update_sql = [
    # 1. Add total_runs column
    """
    ALTER TABLE innings ADD COLUMN IF NOT EXISTS total_runs INTEGER;
    """,
    
    # 2. Add total_wickets column
    """
    ALTER TABLE innings ADD COLUMN IF NOT EXISTS total_wickets INTEGER;
    """,
    
    # 3. Add run_rate column
    """
    ALTER TABLE innings ADD COLUMN IF NOT EXISTS run_rate DOUBLE;
    """,
    
    # 4. Add boundary_count column
    """
    ALTER TABLE innings ADD COLUMN IF NOT EXISTS boundary_count INTEGER;
    """,
    
    # 5. Add dot_ball_percentage column
    """
    ALTER TABLE innings ADD COLUMN IF NOT EXISTS dot_ball_percentage DOUBLE;
    """,
    
    # 6. Add powerplay_runs column
    """
    ALTER TABLE innings ADD COLUMN IF NOT EXISTS powerplay_runs INTEGER;
    """,
    
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

# Execute each SQL statement separately
for sql in innings_update_sql:
    conn.execute(sql)
    
print("Successfully added new columns to the innings table")

# Verify the changes
verify_innings_sql = """
SELECT 
    innings_id, 
    batting_team, 
    total_runs, 
    total_wickets, 
    run_rate, 
    boundary_count, 
    dot_ball_percentage,
    powerplay_runs
FROM 
    innings
LIMIT 10;
"""
conn.execute(verify_innings_sql)


# Breaking down the ALTER TABLE and UPDATE statements for matches table
matches_update_sql = [
    # 1. Add match_result column
    """
    ALTER TABLE matches ADD COLUMN IF NOT EXISTS match_result VARCHAR;
    """,
    
    # 2. Add margin_description column
    """
    ALTER TABLE matches ADD COLUMN IF NOT EXISTS margin_description VARCHAR;
    """,
    
    # 3. Add chasing_team column
    """
    ALTER TABLE matches ADD COLUMN IF NOT EXISTS chasing_team VARCHAR;
    """,
    
    # 4. Add setting_team column
    """
    ALTER TABLE matches ADD COLUMN IF NOT EXISTS setting_team VARCHAR;
    """,
    
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
    
    # Update chasing_team
    """
    UPDATE matches SET
    chasing_team = (SELECT bowling_team
                   FROM innings
                   WHERE innings.match_id = matches.match_id
                   AND innings_number = 1
                   LIMIT 1);
    """,
    
    # Update setting_team
    """
    UPDATE matches SET
    setting_team = (SELECT batting_team
                   FROM innings
                   WHERE innings.match_id = matches.match_id
                   AND innings_number = 1
                   LIMIT 1);
    """
]

# Execute each SQL statement separately
for sql in matches_update_sql:
    conn.execute(sql)
    
print("Successfully added new columns to the matches table")

# Verify the changes
verify_matches_sql = """
SELECT 
    match_id, 
    team1, 
    team2, 
    outcome_winner, 
    match_result, 
    margin_description, 
    setting_team, 
    chasing_team
FROM 
    matches
LIMIT 10;
"""
conn.execute(verify_matches_sql)


# Breaking down the ALTER TABLE and UPDATE statements for overs table
overs_update_sql = [
    # 1. Add run_rate column
    """
    ALTER TABLE overs ADD COLUMN IF NOT EXISTS run_rate DOUBLE;
    """,
    
    # 2. Add is_powerplay column
    """
    ALTER TABLE overs ADD COLUMN IF NOT EXISTS is_powerplay BOOLEAN;
    """,
    
    # 3. Add boundaries_in_over column
    """
    ALTER TABLE overs ADD COLUMN IF NOT EXISTS boundaries_in_over INTEGER;
    """,
    
    # 4. Add dot_balls_in_over column
    """
    ALTER TABLE overs ADD COLUMN IF NOT EXISTS dot_balls_in_over INTEGER;
    """,
    
    # 5. Add cumulative_runs_in_innings column
    """
    ALTER TABLE overs ADD COLUMN IF NOT EXISTS cumulative_runs_in_innings INTEGER;
    """,
    
    # 6. Add cumulative_wickets_in_innings column
    """
    ALTER TABLE overs ADD COLUMN IF NOT EXISTS cumulative_wickets_in_innings INTEGER;
    """,
    
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
    """,
    
    # Update cumulative values - need to use a separate temporary table for window functions
    """
    CREATE TEMPORARY TABLE temp_cumulative AS
    SELECT 
        over_id,
        SUM(total_runs) OVER (PARTITION BY innings_id ORDER BY over_number) AS cum_runs,
        SUM(wickets) OVER (PARTITION BY innings_id ORDER BY over_number) AS cum_wickets
    FROM overs;
    """,
    
    # Apply cumulative values from temp table
    """
    UPDATE overs SET
    cumulative_runs_in_innings = temp_cumulative.cum_runs,
    cumulative_wickets_in_innings = temp_cumulative.cum_wickets
    FROM temp_cumulative
    WHERE overs.over_id = temp_cumulative.over_id;
    """,
    
    # Clean up temporary table
    """
    DROP TABLE IF EXISTS temp_cumulative;
    """
]

# Execute each SQL statement separately
for sql in overs_update_sql:
    conn.execute(sql)
    
print("Successfully added new columns to the overs table")

# Verify the changes
verify_overs_sql = """
SELECT 
    over_id, 
    innings_id, 
    over_number, 
    total_runs, 
    run_rate, 
    is_powerplay, 
    boundaries_in_over, 
    dot_balls_in_over, 
    cumulative_runs_in_innings, 
    cumulative_wickets_in_innings
FROM 
    overs
LIMIT 10;
"""
conn.execute(verify_overs_sql)


# Breaking down the ALTER TABLE and UPDATE statements for players table
# We'll group the updates by category for better organization

# Group 1: Basic ALTER TABLE statements to add all columns
players_alter_columns = [
    # Basic statistics
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS total_matches_played INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS total_runs_scored INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS batting_strike_rate DOUBLE;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS total_wickets_taken INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS bowling_economy_rate DOUBLE;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS batting_average DOUBLE;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS bowling_average DOUBLE;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS boundary_percentage DOUBLE;",
    
    # Milestone statistics
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS half_centuries INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS centuries INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS double_centuries INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS triple_centuries INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS five_wicket_hauls INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS ten_wicket_hauls INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS four_wicket_hauls INTEGER;",
    
    # Performance highlights
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS highest_score INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS best_bowling_wickets INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS ducks INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS not_outs INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS hat_tricks INTEGER;",
    
    # Format-specific match counts
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS test_matches INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS odi_matches INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS t20_matches INTEGER;",
    
    # Format-specific batting averages
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS test_batting_average DOUBLE;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS odi_batting_average DOUBLE;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS t20_batting_average DOUBLE;",
    
    # Format-specific bowling averages
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS test_bowling_average DOUBLE;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS odi_bowling_average DOUBLE;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS t20_bowling_average DOUBLE;",
    
    # Format-specific run totals
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS test_runs INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS odi_runs INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS t20_runs INTEGER;",
    
    # Format-specific wicket totals
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS test_wickets INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS odi_wickets INTEGER;",
    "ALTER TABLE players ADD COLUMN IF NOT EXISTS t20_wickets INTEGER;"
]

# Group 2: Basic statistics updates
players_basic_stats_updates = [
    # 1. Total matches played
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
    
    # 2. Total runs scored
    """
    UPDATE players SET
    total_runs_scored = (
        SELECT SUM(batter_runs) 
        FROM deliveries 
        WHERE batter_id = players.player_id
    );
    """,
    
    # 3. Batting strike rate
    """
    UPDATE players SET
    batting_strike_rate = (
        SELECT SUM(batter_runs) * 100.0 / NULLIF(COUNT(*), 0)
        FROM deliveries 
        WHERE batter_id = players.player_id
    );
    """,
    
    # 4. Total wickets taken
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
    
    # 5. Bowling economy rate
    """
    UPDATE players SET
    bowling_economy_rate = (
        SELECT SUM(total_runs) * 6.0 / NULLIF(COUNT(*), 0)
        FROM deliveries 
        WHERE bowler_id = players.player_id
    );
    """,
    
    # 6. Batting average
    """
    UPDATE players SET
    batting_average = (
        SELECT SUM(batter_runs) * 1.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN is_wicket = 1 AND wicket_player_out_id = players.player_id 
                            THEN match_id || '-' || innings_id END), 0) 
        FROM deliveries 
        WHERE batter_id = players.player_id
    );
    """,
    
    # 7. Bowling average
    """
    UPDATE players SET
    bowling_average = (
        SELECT SUM(CASE WHEN bowler_id = players.player_id THEN total_runs ELSE 0 END) * 1.0 / 
        NULLIF(SUM(CASE WHEN bowler_id = players.player_id AND is_wicket = 1 AND 
                     wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket') 
                THEN 1 ELSE 0 END), 0) 
        FROM deliveries
    );
    """,
    
    # 8. Boundary percentage
    """
    UPDATE players SET
    boundary_percentage = (
        SELECT COUNT(CASE WHEN batter_runs IN (4, 6) THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0) 
        FROM deliveries 
        WHERE batter_id = players.player_id
    );
    """
]

# Group 3: Milestone statistics (requires WITH clauses)
players_milestone_updates = [
    # 9. Half centuries
    """
    UPDATE players SET
    half_centuries = (
        WITH innings_runs AS (
            SELECT 
                match_id,
                innings_id,
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
    
    # 10. Centuries
    """
    UPDATE players SET
    centuries = (
        WITH innings_runs AS (
            SELECT 
                match_id,
                innings_id,
                batter_id,
                SUM(batter_runs) AS total_runs
            FROM deliveries
            GROUP BY match_id, innings_id, batter_id
        )
        SELECT COUNT(*)
        FROM innings_runs
        WHERE batter_id = players.player_id
        AND total_runs >= 100
        AND total_runs < 200
    );
    """,
    
    # 11. Double centuries
    """
    UPDATE players SET
    double_centuries = (
        WITH innings_runs AS (
            SELECT 
                match_id,
                innings_id,
                batter_id,
                SUM(batter_runs) AS total_runs
            FROM deliveries
            GROUP BY match_id, innings_id, batter_id
        )
        SELECT COUNT(*)
        FROM innings_runs
        WHERE batter_id = players.player_id
        AND total_runs >= 200
        AND total_runs < 300
    );
    """,
    
    # 12. Triple centuries
    """
    UPDATE players SET
    triple_centuries = (
        WITH innings_runs AS (
            SELECT 
                match_id,
                innings_id,
                batter_id,
                SUM(batter_runs) AS total_runs
            FROM deliveries
            GROUP BY match_id, innings_id, batter_id
        )
        SELECT COUNT(*)
        FROM innings_runs
        WHERE batter_id = players.player_id
        AND total_runs >= 300
    );
    """,
    
    # 13. Five wicket hauls
    """
    UPDATE players SET
    five_wicket_hauls = (
        WITH innings_wickets AS (
            SELECT 
                match_id,
                innings_id,
                bowler_id,
                SUM(CASE WHEN is_wicket = 1 AND
                        wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    THEN 1 ELSE 0 END) AS wickets
            FROM deliveries
            GROUP BY match_id, innings_id, bowler_id
        )
        SELECT COUNT(*)
        FROM innings_wickets
        WHERE bowler_id = players.player_id
        AND wickets >= 5
    );
    """,
    
    # 14. Ten wicket hauls
    """
    UPDATE players SET
    ten_wicket_hauls = (
        WITH match_wickets AS (
            SELECT 
                match_id,
                bowler_id,
                SUM(CASE WHEN is_wicket = 1 AND
                        wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    THEN 1 ELSE 0 END) AS wickets
            FROM deliveries
            GROUP BY match_id, bowler_id
        )
        SELECT COUNT(*)
        FROM match_wickets
        WHERE bowler_id = players.player_id
        AND wickets >= 10
    );
    """,
    
    # 20. Four wicket hauls
    """
    UPDATE players SET
    four_wicket_hauls = (
        WITH innings_wickets AS (
            SELECT 
                match_id,
                innings_id,
                bowler_id,
                SUM(CASE WHEN is_wicket = 1 AND
                        wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    THEN 1 ELSE 0 END) AS wickets
            FROM deliveries
            GROUP BY match_id, innings_id, bowler_id
        )
        SELECT COUNT(*)
        FROM innings_wickets
        WHERE bowler_id = players.player_id
        AND wickets = 4
    );
    """
]

# Group 4: Performance highlights
players_performance_updates = [
    # 15. Highest score
    """
    UPDATE players SET
    highest_score = (
        WITH innings_runs AS (
            SELECT 
                match_id,
                innings_id,
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
    
    # 16. Best bowling wickets
    """
    UPDATE players SET
    best_bowling_wickets = (
        WITH innings_bowling AS (
            SELECT 
                match_id,
                innings_id,
                bowler_id,
                SUM(CASE WHEN is_wicket = 1 AND
                        wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    THEN 1 ELSE 0 END) AS wickets
            FROM deliveries
            GROUP BY match_id, innings_id, bowler_id
        )
        SELECT MAX(wickets)
        FROM innings_bowling
        WHERE bowler_id = players.player_id
    );
    """,
    
    # 17. Ducks
    """
    UPDATE players SET
    ducks = (
        WITH innings_runs AS (
            SELECT 
                match_id,
                innings_id,
                batter_id,
                SUM(batter_runs) AS total_runs,
                MAX(CASE WHEN is_wicket = 1 AND wicket_player_out_id = batter_id THEN 1 ELSE 0 END) AS got_out
            FROM deliveries
            GROUP BY match_id, innings_id, batter_id
        )
        SELECT COUNT(*)
        FROM innings_runs
        WHERE batter_id = players.player_id
        AND total_runs = 0
        AND got_out = 1
    );
    """,
    
    # 18. Not outs
    """
    UPDATE players SET
    not_outs = (
        WITH innings_status AS (
            SELECT 
                match_id,
                innings_id,
                batter_id,
                MAX(CASE WHEN is_wicket = 1 AND wicket_player_out_id = batter_id THEN 1 ELSE 0 END) AS got_out
            FROM deliveries
            GROUP BY match_id, innings_id, batter_id
        )
        SELECT COUNT(*)
        FROM innings_status
        WHERE batter_id = players.player_id
        AND got_out = 0
    );
    """,
    
    # 19. Hat tricks
    """
    UPDATE players SET
    hat_tricks = (
        WITH consecutive_wickets AS (
            SELECT 
                match_id,
                innings_id,
                bowler_id,
                delivery_id,
                is_wicket,
                LAG(is_wicket, 1) OVER (PARTITION BY match_id, innings_id, bowler_id ORDER BY over_number, ball_number) as prev_wicket1,
                LAG(is_wicket, 2) OVER (PARTITION BY match_id, innings_id, bowler_id ORDER BY over_number, ball_number) as prev_wicket2
            FROM deliveries
            WHERE is_wicket = 1
            AND wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
        )
        SELECT COUNT(*)
        FROM (
            SELECT match_id, innings_id, MIN(delivery_id) as first_ball
            FROM consecutive_wickets
            WHERE bowler_id = players.player_id
            AND is_wicket = 1 
            AND prev_wicket1 = 1 
            AND prev_wicket2 = 1
            GROUP BY match_id, innings_id
        ) hat_tricks
    );
    """
]

# Group 5: Format-specific match counts
players_format_matches_updates = [
    # 21. Test matches
    """
    UPDATE players SET
    test_matches = (
        SELECT COUNT(DISTINCT m.match_id)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE (d.batter_id = players.player_id
            OR d.bowler_id = players.player_id
            OR d.non_striker_id = players.player_id)
        AND m.match_type = 'Test'
    );
    """,
    
    # 22. ODI matches
    """
    UPDATE players SET
    odi_matches = (
        SELECT COUNT(DISTINCT m.match_id)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE (d.batter_id = players.player_id
            OR d.bowler_id = players.player_id
            OR d.non_striker_id = players.player_id)
        AND m.match_type = 'ODI'
    );
    """,
    
    # 23. T20 matches
    """
    UPDATE players SET
    t20_matches = (
        SELECT COUNT(DISTINCT m.match_id)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE (d.batter_id = players.player_id
            OR d.bowler_id = players.player_id
            OR d.non_striker_id = players.player_id)
        AND m.match_type IN ('T20', 'IT20')
    );
    """
]

# Group 6: Format-specific batting averages
players_format_batting_updates = [
    # 24. Test batting average
    """
    UPDATE players SET
    test_batting_average = (
        SELECT SUM(d.batter_runs) * 1.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN d.is_wicket = 1
                                AND d.wicket_player_out_id = players.player_id
                                AND m.match_type = 'Test'
                               THEN d.match_id || '-' || d.innings_id END), 0)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.batter_id = players.player_id
        AND m.match_type = 'Test'
    );
    """,
    
    # 25. ODI batting average
    """
    UPDATE players SET
    odi_batting_average = (
        SELECT SUM(d.batter_runs) * 1.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN d.is_wicket = 1
                                AND d.wicket_player_out_id = players.player_id
                                AND m.match_type = 'ODI'
                               THEN d.match_id || '-' || d.innings_id END), 0)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.batter_id = players.player_id
        AND m.match_type = 'ODI'
    );
    """,
    
    # 26. T20 batting average
    """
    UPDATE players SET
    t20_batting_average = (
        SELECT SUM(d.batter_runs) * 1.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN d.is_wicket = 1
                                AND d.wicket_player_out_id = players.player_id
                                AND m.match_type IN ('T20', 'IT20')
                               THEN d.match_id || '-' || d.innings_id END), 0)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.batter_id = players.player_id
        AND m.match_type IN ('T20', 'IT20')
    );
    """
]

# Group 7: Format-specific bowling averages
players_format_bowling_updates = [
    # 27. Test bowling average
    """
    UPDATE players SET
    test_bowling_average = (
        SELECT SUM(CASE WHEN d.bowler_id = players.player_id THEN d.total_runs ELSE 0 END) * 1.0 / 
        NULLIF(SUM(CASE WHEN d.bowler_id = players.player_id
                    AND d.is_wicket = 1
                    AND d.wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    AND m.match_type = 'Test'
                   THEN 1 ELSE 0 END), 0)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE m.match_type = 'Test'
    );
    """,
    
    # 28. ODI bowling average
    """
    UPDATE players SET
    odi_bowling_average = (
        SELECT SUM(CASE WHEN d.bowler_id = players.player_id THEN d.total_runs ELSE 0 END) * 1.0 / 
        NULLIF(SUM(CASE WHEN d.bowler_id = players.player_id
                    AND d.is_wicket = 1
                    AND d.wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    AND m.match_type = 'ODI'
                   THEN 1 ELSE 0 END), 0)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE m.match_type = 'ODI'
    );
    """,
    
    # 29. T20 bowling average
    """
    UPDATE players SET
    t20_bowling_average = (
        SELECT SUM(CASE WHEN d.bowler_id = players.player_id THEN d.total_runs ELSE 0 END) * 1.0 / 
        NULLIF(SUM(CASE WHEN d.bowler_id = players.player_id
                    AND d.is_wicket = 1
                    AND d.wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    AND m.match_type IN ('T20', 'IT20')
                   THEN 1 ELSE 0 END), 0)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE m.match_type IN ('T20', 'IT20')
    );
    """
]

# Group 8: Format-specific run totals
players_format_runs_updates = [
    # 30. Test runs
    """
    UPDATE players SET
    test_runs = (
        SELECT SUM(d.batter_runs)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.batter_id = players.player_id
        AND m.match_type = 'Test'
    );
    """,
    
    # 31. ODI runs
    """
    UPDATE players SET
    odi_runs = (
        SELECT SUM(d.batter_runs)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.batter_id = players.player_id
        AND m.match_type = 'ODI'
    );
    """,
    
    # 32. T20 runs
    """
    UPDATE players SET
    t20_runs = (
        SELECT SUM(d.batter_runs)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.batter_id = players.player_id
        AND m.match_type IN ('T20', 'IT20')
    );
    """
]

# Group 9: Format-specific wicket totals
players_format_wickets_updates = [
    # 33. Test wickets
    """
    UPDATE players SET
    test_wickets = (
        SELECT SUM(CASE WHEN d.is_wicket = 1
                     AND d.wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    THEN 1 ELSE 0 END)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.bowler_id = players.player_id
        AND m.match_type = 'Test'
    );
    """,
    
    # 34. ODI wickets
    """
    UPDATE players SET
    odi_wickets = (
        SELECT SUM(CASE WHEN d.is_wicket = 1
                     AND d.wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    THEN 1 ELSE 0 END)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.bowler_id = players.player_id
        AND m.match_type = 'ODI'
    );
    """,
    
    # 35. T20 wickets
    """
    UPDATE players SET
    t20_wickets = (
        SELECT SUM(CASE WHEN d.is_wicket = 1
                     AND d.wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                    THEN 1 ELSE 0 END)
        FROM deliveries d
        JOIN matches m ON d.match_id = m.match_id
        WHERE d.bowler_id = players.player_id
        AND m.match_type IN ('T20', 'IT20')
    );
    """
]

# Combine all update groups
players_update_sql = (
    players_alter_columns +
    players_basic_stats_updates +
    players_milestone_updates +
    players_performance_updates +
    players_format_matches_updates +
    players_format_batting_updates +
    players_format_bowling_updates +
    players_format_runs_updates +
    players_format_wickets_updates
)

# Execute each SQL statement separately and print progress
for i, sql in enumerate(players_update_sql):
    try:
        conn.execute(sql)
        if i % 5 == 0:  # Print progress every 5 statements
            print(f"Executed {i+1}/{len(players_update_sql)} statements")
    except Exception as e:
        print(f"Error executing statement {i+1}: {e}")
        print(f"Statement: {sql}")

print("Successfully added new columns to the players table")

# Verify the changes - show a sample of the data
verify_players_sql = """
SELECT 
    player_id, 
    player_name, 
    total_matches_played, 
    total_runs_scored, 
    total_wickets_taken,
    batting_average,
    bowling_average,
    highest_score,
    test_matches,
    odi_matches,
    t20_matches
FROM 
    players
ORDER BY total_runs_scored DESC NULLS LAST
LIMIT 10;
"""
conn.execute(verify_players_sql)


# Create the player_match_stats table with corrected GROUP BY
player_match_stats_sql = """
-- Create the player_match_stats table
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
                    (i.batting_team = m.team1 OR i.bowling_team = m.team1) AND 
                    p.player_id IN (SELECT batter_id FROM deliveries WHERE innings_id = i.innings_id
                                   UNION SELECT bowler_id FROM deliveries WHERE innings_id = i.innings_id
                                   UNION SELECT non_striker_id FROM deliveries WHERE innings_id = i.innings_id))
        THEN m.team1
        ELSE m.team2
    END AS player_team,
    
    -- Batting stats
    COUNT(DISTINCT CASE WHEN d.batter_id = p.player_id THEN d.innings_id END) AS innings_batted,
    SUM(CASE WHEN d.batter_id = p.player_id THEN 1 ELSE 0 END) AS balls_faced,
    SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) AS runs_scored,
    SUM(CASE WHEN d.batter_id = p.player_id AND d.batter_runs = 4 THEN 1 ELSE 0 END) AS fours,
    SUM(CASE WHEN d.batter_id = p.player_id AND d.batter_runs = 6 THEN 1 ELSE 0 END) AS sixes,
    CASE 
        WHEN SUM(CASE WHEN d.batter_id = p.player_id THEN 1 ELSE 0 END) > 0
        THEN SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) * 100.0 / 
             SUM(CASE WHEN d.batter_id = p.player_id THEN 1 ELSE 0 END)
        ELSE NULL
    END AS batting_strike_rate,
    
    -- Did player get out?
    MAX(CASE WHEN d.wicket_player_out_id = p.player_id THEN 1 ELSE 0 END) AS was_dismissed,
    MAX(CASE WHEN d.wicket_player_out_id = p.player_id THEN d.wicket_kind ELSE NULL END) AS dismissal_type,
    
    -- Highest continuous score calculation
    SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) AS total_runs,
    
    -- Bowling stats
    COUNT(DISTINCT CASE WHEN d.bowler_id = p.player_id THEN CONCAT(d.innings_id, '_', d.over_number) END) AS overs_bowled,
    SUM(CASE WHEN d.bowler_id = p.player_id THEN 1 ELSE 0 END) AS balls_bowled,
    SUM(CASE WHEN d.bowler_id = p.player_id THEN d.total_runs ELSE 0 END) AS runs_conceded,
    SUM(CASE WHEN d.bowler_id = p.player_id AND d.is_wicket = 1 AND d.wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
        THEN 1 ELSE 0 END) AS wickets_taken,
    SUM(CASE WHEN d.bowler_id = p.player_id AND d.extras_type = 'wides' THEN 1 ELSE 0 END) AS wides_bowled,
    SUM(CASE WHEN d.bowler_id = p.player_id AND d.extras_type = 'noballs' THEN 1 ELSE 0 END) AS noballs_bowled,
    SUM(CASE WHEN d.bowler_id = p.player_id AND d.total_runs = 0 THEN 1 ELSE 0 END) AS dot_balls_bowled,
    
    -- Economy rate
    CASE 
        WHEN SUM(CASE WHEN d.bowler_id = p.player_id THEN 1 ELSE 0 END) >= 6
        THEN SUM(CASE WHEN d.bowler_id = p.player_id THEN d.total_runs ELSE 0 END) * 6.0 / 
             SUM(CASE WHEN d.bowler_id = p.player_id THEN 1 ELSE 0 END)
        ELSE NULL
    END AS bowling_economy_rate,
    
    -- Fielding stats
    SUM(CASE WHEN d.wicket_fielder_id = p.player_id THEN 1 ELSE 0 END) AS catches_taken,
    
    -- Match result for player's team
    CASE 
        WHEN m.outcome_winner = 
             CASE 
                 WHEN EXISTS (SELECT 1 FROM innings i WHERE i.match_id = d.match_id AND 
                             (i.batting_team = m.team1 OR i.bowling_team = m.team1) AND 
                             p.player_id IN (SELECT batter_id FROM deliveries WHERE innings_id = i.innings_id
                                           UNION SELECT bowler_id FROM deliveries WHERE innings_id = i.innings_id
                                           UNION SELECT non_striker_id FROM deliveries WHERE innings_id = i.innings_id))
                 THEN m.team1
                 ELSE m.team2
             END
        THEN 'won'
        WHEN m.outcome_winner IS NULL THEN 'no result'
        ELSE 'lost'
    END AS match_result,
    
    -- Player of match flag
    CASE WHEN m.player_of_match_id = p.player_id THEN TRUE ELSE FALSE END AS is_player_of_match,
    
    -- Additional metrics included directly in the table creation
    -- 7. Flag for scoring a half-century
    CASE WHEN SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) >= 50 
         AND SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) < 100 
         THEN TRUE ELSE FALSE END AS is_half_century,
    
    -- 8. Flag for scoring a century
    CASE WHEN SUM(CASE WHEN d.batter_id = p.player_id THEN d.batter_runs ELSE 0 END) >= 100 
         THEN TRUE ELSE FALSE END AS is_century,
    
    -- 9. Flag for taking five or more wickets
    CASE WHEN SUM(CASE WHEN d.bowler_id = p.player_id AND d.is_wicket = 1 
                        AND d.wicket_kind IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
                        THEN 1 ELSE 0 END) >= 5 
         THEN TRUE ELSE FALSE END AS is_five_wicket_haul
    
FROM 
    deliveries d
JOIN 
    matches m ON d.match_id = m.match_id
JOIN 
    players p ON p.player_id IN (d.batter_id, d.bowler_id, d.non_striker_id, d.wicket_player_out_id, d.wicket_fielder_id)
GROUP BY 
    d.match_id, m.date, m.venue, m.match_type, m.team1, m.team2, 
    p.player_id, p.player_name, m.player_of_match_id, m.outcome_winner;  -- Added m.outcome_winner here

-- Add indexes to improve query performance
CREATE INDEX idx_pms_match_id ON player_match_stats(match_id);
CREATE INDEX idx_pms_player_id ON player_match_stats(player_id);
CREATE INDEX idx_pms_player_team ON player_match_stats(player_team);
"""

# Execute the table creation
print("Creating player_match_stats table - this may take some time...")
conn.execute(player_match_stats_sql)
print("Successfully created player_match_stats table with base metrics")

# Now add the additional metrics that require separate updates
additional_columns_sql = [
    # Add boundary_percentage column
    """
    ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS boundary_percentage DOUBLE;
    """,
    
    # Update boundary_percentage values
    """
    UPDATE player_match_stats SET
    boundary_percentage = CASE 
                             WHEN balls_faced > 0 
                             THEN (fours + sixes) * 100.0 / balls_faced 
                             ELSE NULL 
                          END;
    """,
    
    # Add dot_ball_percentage_bowled column
    """
    ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS dot_ball_percentage_bowled DOUBLE;
    """,
    
    # Update dot_ball_percentage_bowled values
    """
    UPDATE player_match_stats SET
    dot_ball_percentage_bowled = CASE 
                                    WHEN balls_bowled > 0 
                                    THEN dot_balls_bowled * 100.0 / balls_bowled 
                                    ELSE NULL 
                                 END;
    """,
    
    # Add bowling_strike_rate column
    """
    ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS bowling_strike_rate DOUBLE;
    """,
    
    # Update bowling_strike_rate values
    """
    UPDATE player_match_stats SET
    bowling_strike_rate = CASE 
                             WHEN wickets_taken > 0 
                             THEN balls_bowled * 1.0 / wickets_taken 
                             ELSE NULL 
                          END;
    """,
    
    # Add match_runs_contribution_percentage column
    """
    ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS match_runs_contribution_percentage DOUBLE;
    """,
    
    # Add match_wickets_contribution_percentage column
    """
    ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS match_wickets_contribution_percentage DOUBLE;
    """,
    
    # Add batting_position column
    """
    ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS batting_position INTEGER;
    """
]

# Execute the ALTER TABLE statements
for sql in additional_columns_sql:
    conn.execute(sql)
print("Added additional columns to player_match_stats table")

# Now perform the more complex updates that require aggregation across the table
complex_updates_sql = [
    # Update match_runs_contribution_percentage
    """
    UPDATE player_match_stats AS pms
    SET match_runs_contribution_percentage = (
        pms.runs_scored * 100.0 / NULLIF((
            SELECT SUM(runs_scored)
            FROM player_match_stats
            WHERE match_id = pms.match_id
            AND player_team = pms.player_team
        ), 0)
    )
    WHERE pms.runs_scored > 0;
    """,
    
    # Update match_wickets_contribution_percentage
    """
    UPDATE player_match_stats AS pms
    SET match_wickets_contribution_percentage = (
        pms.wickets_taken * 100.0 / NULLIF((
            SELECT SUM(wickets_taken)
            FROM player_match_stats
            WHERE match_id = pms.match_id
            AND player_team = pms.player_team
        ), 0)
    )
    WHERE pms.wickets_taken > 0;
    """
]

# Execute the complex updates
print("Updating contribution percentages - this may take some time...")
for sql in complex_updates_sql:
    conn.execute(sql)
print("Updated contribution percentages in player_match_stats table")

# Finally, update the batting position using a more complex approach with CTE
batting_position_sql = """
-- Create a temporary table to calculate batting positions
CREATE TEMPORARY TABLE temp_batting_positions AS
WITH batting_order AS (
    SELECT 
        d.match_id,
        d.innings_id,
        d.batter_id,
        MIN(d.over_number * 6 + d.ball_number) AS first_ball,
        ROW_NUMBER() OVER (
            PARTITION BY d.match_id, d.innings_id 
            ORDER BY MIN(d.over_number * 6 + d.ball_number)
        ) AS position
    FROM 
        deliveries d
    GROUP BY 
        d.match_id, d.innings_id, d.batter_id
)
SELECT 
    match_id,
    batter_id,
    MIN(position) AS batting_position
FROM 
    batting_order
GROUP BY 
    match_id, batter_id;

-- Update the player_match_stats table with the batting positions
UPDATE player_match_stats pms
SET batting_position = bp.batting_position
FROM temp_batting_positions bp
WHERE pms.match_id = bp.match_id
AND pms.player_id = bp.batter_id;

-- Drop the temporary table
DROP TABLE temp_batting_positions;
"""

print("Updating batting positions - this may take some time...")
conn.execute(batting_position_sql)
print("Successfully updated batting positions")

# Verify the creation of the table and all added columns
verify_table_sql = """
SELECT 
    column_name
FROM 
    information_schema.columns
WHERE 
    table_name = 'player_match_stats'
ORDER BY 
    ordinal_position;
"""
column_results = conn.execute(verify_table_sql).fetchall()
print(f"player_match_stats table created with {len(column_results)} columns:")
for col in column_results:
    print(f"- {col[0]}")

# Display sample data
verify_data_sql = """
SELECT 
    match_id, 
    player_name, 
    player_team, 
    runs_scored, 
    wickets_taken, 
    batting_strike_rate, 
    bowling_economy_rate,
    is_half_century,
    is_century,
    is_five_wicket_haul,
    boundary_percentage,
    match_runs_contribution_percentage,
    match_wickets_contribution_percentage,
    batting_position,
    is_player_of_match,
    match_result
FROM 
    player_match_stats
WHERE 
    runs_scored > 50 OR wickets_taken > 2
ORDER BY 
    date DESC, runs_scored DESC
LIMIT 5;
"""
conn.execute(verify_data_sql)




