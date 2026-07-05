WITH team_scores AS (
    SELECT 
        fixture_id,
        team_name,
        side,
        COALESCE(SUM(goals), 0) AS goals_scored
    FROM match_players
    GROUP BY fixture_id, team_name, side
)
SELECT 
    s.fixture_id,
    s.team_name,
    s.expected_goals,
    t.goals_scored,        
    CAST(s.expected_goals - t.goals_scored AS DECIMAL(5,2)) AS xg_overperformance  
FROM match_statistics s
JOIN team_scores t
    ON s.fixture_id = t.fixture_id 
    AND s.team_name = t.team_name
ORDER BY xg_overperformance ASC
LIMIT 10;
