from __future__ import print_function # Python 2/3 compatibility
import json
import boto3
import uuid
from pgdbinit import pgdbinit
from response import response
import psycopg2

#nvdb
ACCESS_KEY = 'AKIAWRTZ4VAOWKYFZ2WI'
SECRET_KEY = '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'

DeliveryStreamName = 'review-stream'

def handler(event, context):
    print('received create event{}'.format(event))

    query_param_list = None

    client = boto3.client('firehose',
                          region_name='us-east-1',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY
                          )

    body = json.loads(event['body'])

    twiview = body['twiview']

    try:
        client.put_record(DeliveryStreamName=DeliveryStreamName, Record={'Data': twiview})
    except Exception as e:
        print(e)

    return  response({'message': 'Successfully put record into delivery stream'}, 200)



