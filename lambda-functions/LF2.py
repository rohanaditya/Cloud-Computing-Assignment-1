import json
import os
import boto3
import random
import urllib.request
import urllib.parse
import base64
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

OPENSEARCH_ENDPOINT = os.getenv('OPENSEARCH_ENDPOINT')
OPENSEARCH_USER = os.getenv('OPENSEARCH_USER')
OPENSEARCH_PASS = os.getenv('OPENSEARCH_PASS')

SQS_QUEUE_URL = os.getenv('QUEUE_URL')
DYNAMODB_TABLE = os.getenv('DYNAMODB_TABLE')
SES_SENDER = os.getenv('SES_SENDER')
AWS_REGION = os.getenv('AWS_REGION')

sqs = boto3.client('sqs',region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb',region_name=AWS_REGION)
ses = boto3.client('ses',region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)

def query_opensearch(cuisine, count=3):
    url = f"{OPENSEARCH_ENDPOINT}/restaurants/_search"
    query = json.dumps({
        "query": {
            "term": { "Cuisine": cuisine.lower() }
        },
        "size": 50
    }).encode('utf-8')

    credentials = f"{OPENSEARCH_USER}:{OPENSEARCH_PASS}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    req = urllib.request.Request(
        url,
        data=query,
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Basic {encoded_credentials}'
        },
        method='GET'
    )

    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode('utf-8'))

    hits = data.get('hits', {}).get('hits', [])
    if not hits:
        print(f"No restaurants found in OpenSearch for cuisine: {cuisine}")
        return []

    chosen = random.sample(hits, min(count, len(hits)))
    return [h['_source']['RestaurantID'] for h in chosen]


def get_restaurant_from_dynamodb(business_id):
    response = table.get_item(Key={'business_id': business_id})
    return response.get('Item')


def send_recommendation_email(to_address, restaurants, cuisine):
    restaurant_details = ""
    for i, restaurant in enumerate(restaurants, 1):
        name = restaurant.get('name', 'N/A')
        address = restaurant.get('address', 'N/A')
        rating = restaurant.get('rating', 'N/A')
        reviews = restaurant.get('review_count', 'N/A')
        restaurant_details += f"""
Recommendation {i}:
  Name: {name}
  Address: {address}
  Rating: {rating} stars ({reviews} reviews)
"""

    subject = f"Your {cuisine} Restaurant Recommendations!"
    body = f"""Hello!

Based on the information your provided, here are 3 {cuisine} restaurant recommendations in New York:
{restaurant_details}
Enjoy your meal!

- Dining Concierge Bot
"""

    ses.send_email(
        Source=SES_SENDER,
        Destination={'ToAddresses': [to_address]},
        Message={
            'Subject': {'Data': subject},
            'Body': {'Text': {'Data': body}}
        }
    )
    print(f"Email sent to {to_address}")


def lambda_handler(event, context):
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
    )

    messages = response.get('Messages', [])
    if not messages:
        print("No messages in queue.")
        return {'statusCode': 200, 'body': 'No messages to process'}

    message = messages[0]
    receipt_handle = message['ReceiptHandle']
    body = json.loads(message['Body'])

    cuisine = body.get('cuisine', '').strip()
    email = body.get('email', '').strip()

    print(f"Received request — cuisine: {cuisine}, email: {email}")

    if not cuisine or not email:
        print("Missing cuisine or email in message.")
        return {'statusCode': 400, 'body': 'Invalid message format'}

    business_ids = query_opensearch(cuisine, count=3)
    if not business_ids:
        return {'statusCode': 404, 'body': f'No restaurants found for {cuisine}'}

    print(f"Selected restaurant IDs from OpenSearch: {business_ids}")

    restaurants = []
    for business_id in business_ids:
        restaurant = get_restaurant_from_dynamodb(business_id)
        if restaurant:
            restaurants.append(restaurant)
            print(f"Found in DynamoDB: {restaurant.get('name')}")
        else:
            print(f"Restaurant {business_id} not found in DynamoDB, skipping.")

    if not restaurants:
        return {'statusCode': 404, 'body': 'No restaurant details found in DynamoDB'}

    send_recommendation_email(email, restaurants, cuisine)

    sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
    print("Message deleted from SQS.")

    return {
        'statusCode': 200,
        'body': f"3 {cuisine} recommendations sent to {email}"
    }