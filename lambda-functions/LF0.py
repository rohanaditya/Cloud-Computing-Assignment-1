import json
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

AWS_REGION = os.getenv('AWS_REGION')
lex_client = boto3.client('lexv2-runtime', region_name=AWS_REGION)
BOT_ID = os.getenv('BOT_ID')
BOT_ALIAS_ID = os.getenv('BOT_ALIAS_ID')
LOCALE_ID = os.getenv('LOCALE_ID')

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))
    
    body = json.loads(event['body'])
    message = body['messages'][0]['unstructured']['text']
    
    response = lex_client.recognize_text(
        botId=BOT_ID,
        botAliasId=BOT_ALIAS_ID,
        localeId=LOCALE_ID,
        sessionId='user-session-1',
        text=message
    )
    
    print("Lex response:", json.dumps(response, default=str))
    
    lex_messages = response.get('messages', [])
    
    if lex_messages:
        reply_text = lex_messages[0]['content']
    else:
        reply_text = "I'm sorry, I didn't understand that."

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps({
            'messages': [{
                'type': 'unstructured',
                'unstructured': {
                    'text': reply_text
                }
            }]
        })
    }