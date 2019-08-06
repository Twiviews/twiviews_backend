from __future__ import print_function # Python 2/3 compatibility
import json
import boto3

def handler(event, context):
    print('received read event{}'.format(event))
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('reviews_tbl')

    pathParameters = event['pathParameters']
    hashtag = pathParameters['review_hash']

    #getting db here. but can we return directly from Kinesis stream.

    get_response = table.get_item(
        Key = {
            'hashtag': hashtag
            }
        )

    print('get response{}'.format(get_response))

    response = {
        "statusCode": 200,
        "body": json.dumps(get_response['Item'])
    }

    return response
