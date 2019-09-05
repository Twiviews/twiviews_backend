from __future__ import print_function

import base64
import json
import boto3
import uuid
from decimal import Decimal



def handler(event, context):
    output = []
    hashtag=''

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('reviews_tbl')



    for record in event['records']:

        dict_data = base64.b64decode(record['data']).decode('utf-8').strip()
        print(dict_data)

        for tag in dict_data.split():
            if tag.startswith("#"):
                hashtag = tag.strip("#")

        comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
        sentiment_all = comprehend.detect_sentiment(Text=dict_data, LanguageCode='en')
        sentiment = sentiment_all['Sentiment']
        print(sentiment)
        positive = sentiment_all['SentimentScore']['Positive']
        negative = sentiment_all['SentimentScore']['Negative']
        signal = positive - negative
        print(signal)

        id = str(uuid.uuid1())

        put_response = table.put_item(
            Item ={
                'hashtag': hashtag,
                'review_id': id,
                'text': dict_data,
                'sentiment': sentiment,
                'signal': Decimal(signal)              
                }
            )

        print('put response{}'.format(put_response))

        data_record = {
            'id': record['recordId'],
            'hashtag': hashtag,
            'message': dict_data,
            'sentiment': sentiment,
            'signal': signal
        }



        print(data_record)

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(data_record).encode('utf-8')).decode('utf-8')
        }
        print(output_record)

        output.append(output_record)

    print(output)
    return {'records': output}
