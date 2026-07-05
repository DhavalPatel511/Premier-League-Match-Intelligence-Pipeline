import boto3
import json
import time
from datetime import datetime, timezone

kinesis = boto3.client('kinesis', region_name='us-east-1')
s3 = boto3.client('s3')

STREAM_NAME = 'premier-league-events'
BUCKET = 'premier-league-raw'
REPLAY_SPEED = 0.5


def load_events_from_s3(fixture_id):
    """Load raw events JSON from S3 for a given fixture"""
    key = f"round=38/fixture={fixture_id}/events.json"
    response = s3.get_object(Bucket=BUCKET, Key=key)
    raw = json.loads(response['Body'].read().decode())
    return raw['response']  # the actual events array

def send_to_kinesis(record, partition_key):
    """Send a single shaped record to Kinesis"""
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(record),
        PartitionKey=str(partition_key)
    )
    print(f"Sent: {partition_key} - {record['event_type']} at minute {record['minute']}")



def shape_record(event, fixture_id, replay_timestamp):
    """Shape a raw API event into a flat Kinesis record"""
    event_type_map = {
        'Goal': 'Goal',
        'Card': 'Card',
        'subst': 'Substitution',
        'Var': 'VAR'
    }
    return {
        'fixture_id': fixture_id,
        'minute': event['time']['elapsed'],
        'extra_time_minute': event['time']['extra'],
        'team_name': event['team']['name'],
        'team_id': event['team']['id'],
        'player_name': event['player']['name'],
        'player_id': event['player']['id'],
        'secondary_player_name': event['assist']['name'],
        'event_type': event_type_map.get(event['type'], event['type']),
        'detail': event['detail'],
        'comments': event['comments'],
        'replay_timestamp': replay_timestamp,
        'ingestion_timestamp': datetime.now(timezone.utc).isoformat()
    }


def replay_events():
    fixtures = [1208393, 1208394, 1208395, 1208396, 1208397, 1208398, 1208399, 1208400, 1208401, 1208402]
    #fixtures = [1208393]  # testing
    # Step 1 — load all events from S3
    all_events = []
    for fixture_id in fixtures:
        try:
            events = load_events_from_s3(fixture_id)
            for event in events:
                event['_fixture_id'] = fixture_id  # tag with source
            all_events.extend(events)
        except Exception as e:
            print(f"Failed to load {fixture_id}: {e}")
            continue

    # Step 2 — sort all events chronologically across all tickers
    all_events.sort(key=lambda x: x['time']['elapsed'])

    # Step 3 — replay in order with simulated timestamps
    base_time = datetime(2024, 5, 19, 15, 0, 0, tzinfo=timezone.utc)
    
    for event in all_events:
        fixture_id = event.pop('_fixture_id')
        elapsed_minutes = event['time']['elapsed']
        replay_timestamp = base_time.replace(
            minute=elapsed_minutes % 60,
            hour=15 + (elapsed_minutes // 60)
        ).isoformat()

        record = shape_record(event, fixture_id, replay_timestamp)
        
        try:
            send_to_kinesis(record, partition_key=fixture_id)
        except Exception as e:
            print(f"Failed to send {fixture_id} event: {e}")
            continue

        time.sleep(REPLAY_SPEED)


def lambda_handler(event, context):
    replay_events()
    return {
        'statusCode': 200,
        'body': json.dumps('Replay complete')
    }
