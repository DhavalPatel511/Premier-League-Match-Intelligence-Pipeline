SELECT player_name, team_name, rating, goals, assists, minutes_played
FROM match_players
WHERE minutes_played >= 45
ORDER BY rating DESC
LIMIT 5;
