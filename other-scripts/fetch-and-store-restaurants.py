import boto3
import requests
import time
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

load_dotenv()

YELP_API_KEY = os.getenv('YELP_API_KEY')
AWS_REGION = os.getenv('AWS_REGION')

YELP_HEADERS = {'Authorization': f'Bearer {YELP_API_KEY}'}
YELP_URL = 'https://api.yelp.com/v3/businesses/search'

CUISINES = ['Chinese', 'Italian', 'Mexican', 'Indian', 'Japanese', 'Thai', 'American']
LOCATION = 'New York'
TARGET = 1        
BATCH_SIZE = 50 
MAX_OFFSET = 1000
TABLE_NAME = 'yelp-restaurants'

dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)


def to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_decimal(i) for i in obj]
    return obj


def get_restaurants(cuisine: str, total: int = TARGET) -> list:
    restaurants = []
    offset = 0

    while len(restaurants) < total:
        limit = min(BATCH_SIZE, total - len(restaurants), MAX_OFFSET - offset)
        if limit <= 0:
            print(f"Hit offset limit!")
            break

        params = {
            'term': f'{cuisine} restaurants',
            'location': LOCATION,
            'limit': limit,
            'offset': offset,
        }

        try:
            response = requests.get(YELP_URL, headers=YELP_HEADERS, params=params, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"HTTP error fetching {cuisine} at offset {offset}: {e}")
            break

        businesses = response.json().get('businesses', [])
        if not businesses:
            print(f"No more results for {cuisine} at offset {offset}.")
            break

        restaurants.extend(businesses)
        offset += len(businesses)
        print(f"Fetched {len(restaurants)}/{total} {cuisine} restaurants")
        time.sleep(0.3) 

    return restaurants[:total]


def store_restaurant(restaurant: dict, cuisine: str, seen_ids: set) -> bool:
    business_id = restaurant.get('id', '')
    if not business_id or business_id in seen_ids:
        return False

    location = restaurant.get('location',{})
    address = ' '.join(location.get('display_address', []))
    zip_code = location.get('zip_code', '')
    coordinates = restaurant.get('coordinates', {})

    item = {
        'business_id': business_id,
        'name': restaurant.get('name', ''),
        'address': address,
        'zip_code': zip_code,
        'coordinates': coordinates,
        'review_count': restaurant.get('review_count', 0),
        'rating': str(restaurant.get('rating', '')),
        'cuisine': cuisine,
        'insertedAtTimestamp': datetime.utcnow().isoformat(),
    }

    item = to_decimal(item)

    try:
        table.put_item(Item=item)
        seen_ids.add(business_id)
        return True
    except ClientError as e:
        print(f"DynamoDB error storing '{restaurant.get('name')}': {e}")
        return False


def main():
    seen_ids = set()
    total_stored = 0

    for cuisine in CUISINES:
        print(f"Searching for {cuisine} restaurants")
        restaurants  = get_restaurants(cuisine, total=TARGET)
        stored_count = 0

        for restaurant in restaurants:
            success = store_restaurant(restaurant, cuisine, seen_ids)
            if success:
                stored_count += 1

        print(f"Stored {stored_count} new {cuisine} restaurants.")
        total_stored += stored_count

    print(f"Total restaurants stored: {total_stored}")


if __name__ == '__main__':
    main()