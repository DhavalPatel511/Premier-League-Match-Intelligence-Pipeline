SELECT 
    team_name,
    COUNT(*) AS goals
FROM "premier_league_db"."events"
WHERE event_type = 'Goal'
GROUP BY team_name
ORDER BY goals DESC;
