import boto3
import json
import time
import urllib.request
import urllib.error

s3 = boto3.client('s3')
BUCKET = 'premier-league-raw'

headers = {
    'x-apisports-key': 'your_access_key'
}

fixtures = [1208393, 1208394, 1208395, 1208396, 1208397, 1208398, 1208399, 1208400, 1208401, 1208402]

def save_to_s3(data, prefix, filename):
    key = f"{prefix}/{filename}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"Saved: s3://{BUCKET}/{key}")


def collect_match_data():
    for fixture_id in fixtures:
        try:
            
            ## Events
            event_url = f"https://v3.football.api-sports.io/fixtures/events?fixture={fixture_id}"
            event_response = urllib.request.Request(event_url,headers=headers)
            with urllib.request.urlopen(event_response) as response:
                event_data = json.loads(response.read().decode())
            
            save_to_s3(
                data=event_data,
                prefix=f"round=38/fixture={fixture_id}",
                filename="events"
            )

        except urllib.error.HTTPError as e:
            print(f"HTTP error - events {fixture_id}: {e.code} {e.reason}")
        except urllib.error.URLError as e:
            print(f"URL error - events {fixture_id}: {e.reason}")

        except Exception as e:
            print(f"Unexpected error for - events {fixture_id}: {e}")
        
        time.sleep(2)

        try:
            ## Players
            players_url = f"https://v3.football.api-sports.io/fixtures/players?fixture={fixture_id}"
            players_response = urllib.request.Request(players_url,headers=headers)
            with urllib.request.urlopen(players_response) as response:
                players_data = json.loads(response.read().decode())

            save_to_s3(
                data=players_data,
                prefix=f"round=38/fixture={fixture_id}",
                filename="players"
            )

        except urllib.error.HTTPError as e:
            print(f"HTTP error - players {fixture_id}: {e.code} {e.reason}")
        except urllib.error.URLError as e:
            print(f"URL error - players {fixture_id}: {e.reason}")

        except Exception as e:
            print(f"Unexpected error for - players {fixture_id}: {e}")

        time.sleep(2)

        try:
            ## Lineup
            lineup_url = f"https://v3.football.api-sports.io/fixtures/lineups?fixture={fixture_id}"
            lineup_response = urllib.request.Request(lineup_url,headers=headers)
            with urllib.request.urlopen(lineup_response) as response:
                lineup_data = json.loads(response.read().decode())

            save_to_s3(
                data=lineup_data,
                prefix=f"round=38/fixture={fixture_id}",
                filename="lineups"
            )

        except urllib.error.HTTPError as e:
            print(f"HTTP error - lineups {fixture_id}: {e.code} {e.reason}")
        except urllib.error.URLError as e:
            print(f"URL error - lineups {fixture_id}: {e.reason}")

        except Exception as e:
            print(f"Unexpected error for - lineups {fixture_id}: {e}")

        time.sleep(2)

        try:
            ## Stats
            stats_url = f"https://v3.football.api-sports.io/fixtures/statistics?fixture={fixture_id}"
            stats_response = urllib.request.Request(stats_url,headers=headers)
            with urllib.request.urlopen(stats_response) as response:
                stats_data = json.loads(response.read().decode())

            save_to_s3(
                data=stats_data,
                prefix=f"round=38/fixture={fixture_id}",
                filename="stats"
            )


        except urllib.error.HTTPError as e:
            print(f"HTTP error - stats {fixture_id}: {e.code} {e.reason}")
        except urllib.error.URLError as e:
            print(f"URL error - stats {fixture_id}: {e.reason}")

        
        except Exception as e:
            print(f"Unexpected error for - stats {fixture_id}: {e}")

        time.sleep(3)

def lambda_handler(event, context):
    collect_match_data()
    return {
        'statusCode': 200,
        'body': json.dumps('Collection complete — Round 38 data saved to S3')
    }
