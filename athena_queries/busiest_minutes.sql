SELECT 
    minute,
    event_type,
    COUNT(*) AS events
FROM "premier_league_db"."events"
GROUP BY minute, event_type
ORDER BY minute, events DESC;
