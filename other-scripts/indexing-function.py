import boto3
import json
import urllib.request
import urllib.parse
import base64
from decimal import Decimal
from dotenv import load_dotenv
import os

load_dotenv()

OPENSEARCH_ENDPOINT = os.getenv('OPENSEARCH_ENDPOINT')
OPENSEARCH_USER = os.getenv('OPENSEARCH_USER')
OPENSEARCH_PASS = os.getenv('OPENSEARCH_PASS')
DYNAMODB_TABLE = os.getenv('DYNAMODB_TABLE')
AWS_REGION = os.getenv('AWS_REGION')

dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)


def opensearch_request(method, path, body=None):
    url = f"{OPENSEARCH_ENDPOINT}{path}"
    credentials = f"{OPENSEARCH_USER}:{OPENSEARCH_PASS}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    data = json.dumps(body).encode('utf-8') if body else None

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Basic {encoded_credentials}'
        },
        method=method
    )

    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))


def create_index():
    mapping = {
        "mappings": {
            "properties": {
                "RestaurantID": {"type": "keyword"},
                "Cuisine":      {"type": "keyword"}
            }
        }
    }
    try:
        result = opensearch_request('PUT', '/restaurants', mapping)
        print(f"Index created: {result}")
    except urllib.error.HTTPError as e:
        if e.code == 400:
            print("Index already exists, skipping creation.")
        else:
            raise


def get_all_restaurants_from_dynamodb():
    print("Fetching restaurants from DynamoDB")
    items = []
    response = table.scan()
    items.extend(response['Items'])

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    print(f"Found {len(items)} restaurants in DynamoDB.")
    return items


def index_restaurants(items):
    success = 0
    failed  = 0

    for item in items:
        business_id = item.get('business_id')
        cuisine     = item.get('cuisine', '').lower()

        if not business_id or not cuisine:
            print(f"Skipping item missing business_id or cuisine: {item}")
            failed += 1
            continue

        doc = {
            "RestaurantID": business_id,
            "Cuisine":      cuisine
        }

        try:
            opensearch_request('PUT', f'/restaurants/_doc/{business_id}', doc)
            success += 1
            if success % 10 == 0:
                print(f"Indexed {success} restaurants")
        except Exception as e:
            print(f"Failed to index {business_id}: {e}")
            failed += 1

    return success, failed


def verify_index():
    result = opensearch_request('GET', '/restaurants/_count')
    print(f"Total documents in OpenSearch: {result.get('count', 'unknown')}")

def delete_index():
    try:
        opensearch_request('DELETE', '/restaurants')
        print("Old index deleted.")
    except:
        print("No existing index to delete.")

def main():
    delete_index()
    create_index()
    items = get_all_restaurants_from_dynamodb()
    success, failed = index_restaurants(items)
    print(f"\nDone! Indexed: {success}, Failed: {failed}")
    verify_index()

def lambda_handler(event, context):
    main()

if __name__ == '__main__':
    main()