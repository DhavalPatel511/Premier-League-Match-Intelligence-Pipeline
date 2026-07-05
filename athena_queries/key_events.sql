SELECT 
    fixture_id,
    team_name,
    player_name,
    event_type,
    detail,
    minute
FROM "premier_league_db"."events"
WHERE is_key_event = true
ORDER BY minute, fixture_id;
