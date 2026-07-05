SELECT 
    formation,
    COUNT(*) AS teams_using,
    SUM(CASE WHEN side = 'home' THEN 1 ELSE 0 END) AS home_teams,
    SUM(CASE WHEN side = 'away' THEN 1 ELSE 0 END) AS away_teams
FROM (
    SELECT DISTINCT fixture_id, team_name, side, formation 
    FROM match_lineups
) t
GROUP BY formation
ORDER BY teams_using DESC;
