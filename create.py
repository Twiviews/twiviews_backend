from __future__ import print_function # Python 2/3 compatibility
import json
import boto3
import uuid

def handler(event, context):
    print('received create event{}'.format(event))
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('reviews_tbl')
    id = str(uuid.uuid1())

    body = json.loads(event['body'])
    text = body['text']
    sentiment = body['sentiment']
    hashtag = body['hashtag']
    signal = body['signal']

    put_response = table.put_item(
        Item = {
            'review_id': id,
            'text': text,
            'sentiment': sentiment,
            'hashtag': hashtag,
            'signal': signal
            }
        )

    print('put response{}'.format(put_response))

    response = {
        "statusCode": 200,
        "body": json.dumps({"review_id": id})
    }

    return response
