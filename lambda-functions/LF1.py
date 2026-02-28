import json
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

sqs = boto3.client('sqs')
QUEUE_URL = os.getenv('QUEUE_URL')

def lambda_handler(event, context):
    intent_name = event['sessionState']['intent']['name']
    
    if intent_name == 'GreetingIntent':
        return handle_greeting(event)
    elif intent_name == 'ThankYouIntent':
        return handle_thankyou(event)
    elif intent_name == 'DiningSuggestionsIntent':
        return handle_dining_suggestions(event)
    else:
        return close(event, "Sorry, I didn't understand that.")

def handle_greeting(event):
    return close(event, "Hi there! I'm your dining concierge. How can I help you today?")

def handle_thankyou(event):
    return close(event, "You're welcome! Enjoy your meal!")

def handle_dining_suggestions(event):
    slots = event['sessionState']['intent']['slots']
    location = get_slot(slots, 'Location')
    cuisine = get_slot(slots, 'Cuisine')
    date = get_slot(slots, 'Date')
    dining_time = get_slot(slots, 'DiningTime')
    num_people = get_slot(slots, 'NumberOfPeople')
    email = get_slot(slots, 'Email')

    if location and location.lower() not in ['new york', 'ny', 'new york city', 'nyc']:
        return elicit_slot(event, 'Location', 'Sorry, we only serve New York at the moment. Please enter New York as your location.')

    if date:
        from datetime import datetime
        date_obj = datetime.strptime(date, '%Y-%m-%d').date()
        today = datetime.now().date()
        if date_obj <= today:
            slots['Date'] = None
            return {
                'sessionState': {
                    'dialogAction': {
                        'type': 'ElicitSlot',
                        'slotToElicit': 'Date'
                    },
                    'intent': {
                        'name': event['sessionState']['intent']['name'],
                        'slots': slots,
                        'state': 'InProgress'
                    }
                },
                'messages': [{
                    'contentType': 'PlainText',
                    'content': 'Please enter a present or future date, not a date in the past.'
                }]
            }

    if not all([location, cuisine, date, dining_time, num_people, email]):
        return delegate(event)

    message = {
        'location': location,
        'cuisine': cuisine,
        'diningTime': dining_time,
        'date': date,
        'numberOfPeople': num_people,
        'email': email
    }
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message)
    )
    return close(event,
        f"Got it! I will find {cuisine} restaurants in {location} for {num_people} people at {dining_time}. "
        f"You are all set. I will send the suggestions to {email}. Thank you!")

def get_slot(slots, slot_name):
    if slots.get(slot_name) and slots[slot_name].get('value'):
        return slots[slot_name]['value']['interpretedValue']
    return None

def delegate(event):
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": event['sessionState']['intent']
        }
    }

def close(event, message):
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {
                "name": event['sessionState']['intent']['name'],
                "state": "Fulfilled"
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def elicit_slot(event, slot_to_elicit, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit
            },
            'intent': {
                'name': event['sessionState']['intent']['name'],
                'slots': event['sessionState']['intent']['slots'],
                'state': 'InProgress'
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }