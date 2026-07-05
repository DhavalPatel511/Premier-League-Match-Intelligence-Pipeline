SELECT 
    player_name,
    team_name,
    COALESCE(goals, 0) + COALESCE(assists, 0) AS goal_involvement,
    COALESCE(goals, 0) AS goals,
    COALESCE(assists, 0) AS assists,
    rating
FROM match_players
WHERE COALESCE(goals, 0) + COALESCE(assists, 0) > 0
ORDER BY goal_involvement DESC, rating DESC
LIMIT 10;
