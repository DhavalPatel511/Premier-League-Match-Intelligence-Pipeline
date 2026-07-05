import boto3
import json
import csv
import io
import time


s3 = boto3.client('s3')
redshift = boto3.client('redshift-data')

STAGING_BUCKET = 'premier-league-processed'
STAGING_PREFIX = 'redshift-staging/premier-league/'
WORKGROUP = 'premier-league-workgroup'
DATABASE = 'dev'


# Home and away team lookup per fixture
fixture_home_teams = {
    1208393: 'Bournemouth',
    1208394: 'Fulham',
    1208395: 'Ipswich',
    1208396: 'Liverpool',
    1208397: 'Manchester United',
    1208398: 'Newcastle United',
    1208399: 'Nottingham Forest',
    1208400: 'Southampton',
    1208401: 'Tottenham',
    1208402: 'Wolves'
}

fixture_away_teams = {
    1208393: 'Leicester',
    1208394: 'Manchester City',
    1208395: 'West Ham',
    1208396: 'Crystal Palace',
    1208397: 'Aston Villa',
    1208398: 'Everton',
    1208399: 'Chelsea',
    1208400: 'Arsenal',
    1208401: 'Brighton',
    1208402: 'Brentford'
}



def flatten_statistics(raw, fixture_id):
    """Flatten one weather API response into a flat dict"""
    stats_lookup = {item['type']: item['value'] for item in raw['statistics']}
    home_team = fixture_home_teams.get(int(fixture_id),'')
    is_home = (raw['team']['name'] == home_team)
    side = 'home' if is_home else 'away'


    return {
        'fixture_id': fixture_id,
        'team_name': raw['team']['name'],
        'side': side,
        'possession': float(str(stats_lookup.get('Ball Possession','0')).replace('%','') or 0),
        'total_shots': stats_lookup.get('Total Shots'),
        'shots_on_target': stats_lookup.get('Shots on Goal'),
        'blocked_shots': stats_lookup.get('Blocked Shots'),
        'shots_off_target': stats_lookup.get('Shots off Goal'),
        'corner_kicks': stats_lookup.get('Corner Kicks'),
        'offsides': stats_lookup.get('Offsides'),
        'fouls': stats_lookup.get('Fouls'),
        'yellow_cards': stats_lookup.get('Yellow Cards'),
        'red_cards': stats_lookup.get('Red Cards'),
        'goalkeeper_saves': stats_lookup.get('Goalkeeper Saves'),
        'total_passes': stats_lookup.get('Total passes'),
        'passes_accurate': stats_lookup.get('Passes accurate'),
        'pass_accuracy': float(str(stats_lookup.get('Passes %', '0')).replace('%','') or 0),
        'expected_goals': float(stats_lookup.get('expected_goals') or 0),
        'goals_prevented': float(stats_lookup.get('goals_prevented') or 0)
    }



def flatten_lineups(raw, fixture_id):
    home_team = fixture_home_teams.get(int(fixture_id),'')
    is_home = (raw['team']['name'] == home_team)
    side = 'home' if is_home else 'away'

    rows=[]
    for player_entry in raw['startXI']:
        p = player_entry['player']
        rows.append({
            'fixture_id': fixture_id,
            'team_name': raw['team']['name'],
            'side': side,
            'formation': raw['formation'],
            'coach_name': raw['coach']['name'],
            'player_id': p['id'],
            'player_name': p['name'],
            'jersey_number': p['number'],
            'position': p['pos']
        })
    return rows


def flatten_players(raw, fixture_id):
    home_team = fixture_home_teams.get(int(fixture_id),'')
    is_home = (raw['team']['name'] == home_team)
    side = 'home' if is_home else 'away'

    rows=[]
    for player_entry in raw['players']:
        s = player_entry['statistics'][0]


        # Skip players who never came on
        if not s['games']['minutes']:
            continue
        
        
        rows.append({
            'fixture_id': fixture_id,
            'team_name': raw['team']['name'],
            'side': side,
            'player_id': player_entry['player']['id'],
            'player_name': player_entry['player']['name'],
            'position': s['games']['position'],
            'minutes_played':       s['games']['minutes'],
            'rating':               float(s['games']['rating']) if s['games']['rating'] else None,
            'goals':                s['goals']['total'],
            'assists':              s['goals']['assists'],
            'passes_total':         s['passes']['total'],
            'pass_accuracy':        float(s['passes']['accuracy']) if s['passes']['accuracy'] else None,
            'shots_total':          s['shots']['total'],
            'shots_on_target':      s['shots']['on'],
            'duels_total':          s['duels']['total'],
            'duels_won':            s['duels']['won'],
            'dribbles_attempted':   s['dribbles']['attempts'],
            'dribbles_succeeded':   s['dribbles']['success'],
            'yellow_cards':         s['cards']['yellow'],
            'red_cards':            s['cards']['red']
        })
    return rows

def write_csv_to_s3(rows, key):
    """Write list of dicts to S3 as CSV"""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
    writer.writerows(rows)
    s3.put_object(
        Bucket=STAGING_BUCKET,
        Key=key,
        Body=buffer.getvalue()
    )
    return f's3://{STAGING_BUCKET}/{key}'

def copy_to_redshift(s3_path, table_name, columns):
    cols = ', '.join(columns)
    copy_sql = f"""
        COPY {table_name} ({cols})
        FROM '{s3_path}'
        IAM_ROLE default
        FORMAT AS CSV
        EMPTYASNULL
        BLANKSASNULL;
    """
    response = redshift.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=copy_sql
    )
    return response['Id']


stats_columns = [
    'fixture_id', 'team_name', 'side', 'possession', 'total_shots',
    'shots_on_target', 'blocked_shots', 'shots_off_target', 'corner_kicks',
    'offsides', 'fouls', 'yellow_cards', 'red_cards', 'goalkeeper_saves',
    'total_passes', 'passes_accurate', 'pass_accuracy', 'expected_goals',
    'goals_prevented'
]

lineup_columns = [
    'fixture_id', 'team_name', 'side', 'formation', 'coach_name',
    'player_id', 'player_name', 'jersey_number', 'position'
]

player_columns = [
    'fixture_id', 'team_name', 'side', 'player_id', 'player_name',
    'position', 'minutes_played', 'rating', 'goals', 'assists',
    'passes_total', 'pass_accuracy', 'shots_total', 'shots_on_target',
    'duels_total', 'duels_won', 'dribbles_attempted', 'dribbles_succeeded',
    'yellow_cards', 'red_cards'
]

def wait_for_copy(query_id):
    """Poll until COPY completes or fails"""
    while True:
        response = redshift.describe_statement(Id=query_id)
        status = response['Status']
        
        if status == 'FINISHED':
            print(f"COPY completed successfully — query {query_id}")
            return True
        elif status in ['FAILED', 'ABORTED']:
            print(f"COPY failed — {response.get('Error', 'unknown error')}")
            return False
        
        print(f"COPY status: {status} — waiting...")
        time.sleep(3)

def lambda_handler(event, context):
    fixtures = [1208393, 1208394, 1208395, 1208396, 1208397, 1208398, 1208399, 1208400, 1208401, 1208402]
    
    all_stats = []
    all_lineups = []
    all_players = []

    for fixture_id in fixtures:
        try:
            # Statistics - 2 rows per fixture
            stats_obj = s3.get_object(
                Bucket='premier-league-raw',
                Key=f'round=38/fixture={fixture_id}/stats.json'
            )
            stats_raw =json.loads(stats_obj['Body'].read().decode())
            for team_data in stats_raw['response']:
                all_stats.append(flatten_statistics(team_data,fixture_id))

            # Lineups - 22(11*2) rows per fixture
            lineup_obj = s3.get_object(
                Bucket='premier-league-raw',
                Key=f'round=38/fixture={fixture_id}/lineups.json'
            )
            stats_raw =json.loads(lineup_obj['Body'].read().decode())
            for team_data in stats_raw['response']:
                all_lineups.extend(flatten_lineups(team_data,fixture_id))


            # Players - all players who played in that fixture
            players_obj = s3.get_object(
                Bucket='premier-league-raw',
                Key=f'round=38/fixture={fixture_id}/players.json'
            )
            stats_raw =json.loads(players_obj['Body'].read().decode())
            for team_data in stats_raw['response']:
                all_players.extend(flatten_players(team_data,fixture_id))


        except Exception as e:
            print(f"Failed to process fixture {fixture_id}: {e}")
            continue

    
    results ={}

    if all_stats:
        s3_path = write_csv_to_s3(all_stats, f'{STAGING_PREFIX}statistics.csv')
        query_id = copy_to_redshift(s3_path, 'match_statistics',stats_columns)
        wait_for_copy(query_id)

    if all_lineups:
        s3_path = write_csv_to_s3(all_lineups, f'{STAGING_PREFIX}lineups.csv')
        query_id = copy_to_redshift(s3_path, 'match_lineups',lineup_columns)
        wait_for_copy(query_id)

    if all_players:
        s3_path = write_csv_to_s3(all_players, f'{STAGING_PREFIX}players.csv')
        query_id = copy_to_redshift(s3_path, 'match_players',player_columns)
        wait_for_copy(query_id)

    print(f"COPY jobs triggered: {results}")
    return {
        'statusCode': 200,
        'body': f'Stats: {len(all_stats)} rows, Lineups: {len(all_lineups)} rows, Players: {len(all_players)} rows'
    }
