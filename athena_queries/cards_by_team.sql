SELECT 
    team_name,
    detail,
    COUNT(*) AS cards
FROM "premier_league_db"."events"
WHERE event_type = 'Card'
GROUP BY team_name, detail
ORDER BY cards DESC;
