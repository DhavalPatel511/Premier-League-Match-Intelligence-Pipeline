import boto3
import base64
import json
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
firehose = boto3.client('firehose')

TABLE_NAME = 'premier-league-live'
DELIVERY_STREAM = 'premier-league-events-firehose'

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

def decode_kinesis_record(record):
    """Base64 decode and JSON parse a single Kinesis record"""
    raw = base64.b64decode(record['kinesis']['data']).decode('utf-8')
    return json.loads(raw)

def enrich_record(data):
    """Add derived fields not present in the original event"""
    data['processed_at'] = datetime.now(timezone.utc).isoformat()
    
    # Derive alert flag based on business logic
    data['is_key_event'] = (
        data['event_type'] == 'Goal' or
        data['event_type'] == 'Red Card'
    )
    return data

def write_to_dynamodb(table, data):
    """Write current state to DynamoDB for live lookups"""
    fixture_id = str(data['fixture_id'])
    team_name = data['team_name']
    event_type = data['event_type']
    detail = data['detail']

    ## Determine home or away
    is_home = (team_name == fixture_home_teams.get(int(fixture_id), ''))
    side = 'home' if is_home else 'away'

    ## Base update - always aaplied regardless of event type
    update_expr = """
        SET last_event_type = :et,
            last_event_minute = :min,
            last_event_player = :player,
            last_updated = :updated,
            home_team = if_not_exists(home_team, :home),
            away_team = if_not_exists(away_team, :away),
            #ttl = :ttl
    """
    expr_values = {
        ':et': event_type,
        ':min': int(data['minute']),
        ':player': data['player_name'],
        ':updated': data['processed_at'],
        ':home': fixture_home_teams.get(int(fixture_id),'Unknown'),
        ':away': fixture_away_teams.get(int(fixture_id),'Unknown'),
        ':ttl': int(datetime.now(timezone.utc).timestamp()) + (7*24*60*60),
        ':one': 1
    }

    if event_type == 'Goal':
        update_expr += f" ADD {side}_score :one"
    if detail == 'Yellow Card':
        update_expr += f" ADD {side}_yellow_cards :one"
    if detail == 'Red Card':
        update_expr += f" ADD {side}_red_cards :one"
    if event_type == 'Substitution':
        update_expr += f" ADD {side}_substitutions :one"

    table.update_item(
        Key={'fixture_id': fixture_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames={'#ttl': 'ttl'} 
    )

def write_to_firehose(stream_name, data):
    """Forward enriched record to Firehose for S3 delivery"""
    firehose.put_record(
        DeliveryStreamName=stream_name,
        Record={'Data': json.dumps(data) + '\n'}  # newline required by Firehose
    )

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    
    success_count = 0
    failure_count = 0

    for record in event['Records']:
        try:
            # Step 1 — decode
            data = decode_kinesis_record(record)
            
            # Step 2 — enrich
            data = enrich_record(data)
            
            # Step 3 — write to DynamoDB (live state)
            write_to_dynamodb(table, data)
            
            # Step 4 — forward to Firehose (analytics layer)
            write_to_firehose(DELIVERY_STREAM, data)
            
            success_count += 1

        except Exception as e:
            print(f"Failed to process record: {e}")
            print(f"Raw record: {record}")
            failure_count += 1
            continue

    print(f"Processed {success_count} records, {failure_count} failures")
    return {'statusCode': 200, 'body': f'{success_count} records processed'}


