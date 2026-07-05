SELECT 
    CASE 
        WHEN minute < 16 THEN '0-15 min'
        WHEN minute BETWEEN 16 AND 30 THEN '16-30 min'
        WHEN minute BETWEEN 31 AND 45 THEN '31-45 min'
        WHEN minute BETWEEN 46 AND 60 THEN '46-60 min'
        WHEN minute BETWEEN 61 AND 75 THEN '61-75 min'
        WHEN minute > 75 THEN '76+ min'
    END AS time_period,
    COUNT(*) AS goals
FROM "premier_league_db"."events"
WHERE event_type = 'Goal'
GROUP BY 
    CASE 
        WHEN minute < 16 THEN '0-15 min'
        WHEN minute BETWEEN 16 AND 30 THEN '16-30 min'
        WHEN minute BETWEEN 31 AND 45 THEN '31-45 min'
        WHEN minute BETWEEN 46 AND 60 THEN '46-60 min'
        WHEN minute BETWEEN 61 AND 75 THEN '61-75 min'
        WHEN minute > 75 THEN '76+ min'
    END
ORDER BY goals DESC;
