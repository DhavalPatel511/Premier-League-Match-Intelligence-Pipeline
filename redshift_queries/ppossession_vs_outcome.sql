WITH team_scores AS (
    SELECT 
        fixture_id,
        team_name,
        side,
        COALESCE(SUM(goals), 0) AS goals_scored
    FROM match_players
    GROUP BY fixture_id, team_name, side
),
match_result AS (
    SELECT 
        h.fixture_id,
        h.team_name AS home_team,
        a.team_name AS away_team,
        h.goals_scored AS home_goals,
        a.goals_scored AS away_goals,
        CASE 
            WHEN h.goals_scored > a.goals_scored THEN 'home_win'
            WHEN h.goals_scored < a.goals_scored THEN 'away_win'
            ELSE 'draw'
        END AS result
    FROM team_scores h
    JOIN team_scores a 
        ON h.fixture_id = a.fixture_id 
        AND h.side = 'home' 
        AND a.side = 'away'
)
SELECT 
    r.fixture_id,
    r.home_team,
    r.away_team,
    s_home.possession AS home_possession,
    s_away.possession AS away_possession,
    r.home_goals,
    r.away_goals,
    r.result
FROM match_result r
JOIN match_statistics s_home 
    ON r.fixture_id = s_home.fixture_id AND s_home.side = 'home'
JOIN match_statistics s_away 
    ON r.fixture_id = s_away.fixture_id AND s_away.side = 'away'
ORDER BY r.fixture_id;
