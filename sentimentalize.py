from __future__ import print_function

import base64
import json
import boto3
import uuid
from decimal import Decimal

import psycopg2

from pgdbinit import pgdbinit


def handler(event, context):
    output = []
    hashtag=''

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

        query_params_list = (id, hashtag, dict_data, sentiment, Decimal(signal))

        print(json.dumps(query_params_list))

        sql = "INSERT INTO twiviews(id, hashtag, twiview, sentiment, signal) VALUES (%s)"
        conn = None
        try:
            conn = pgdbinit.get_conn_rds()
            cur = conn.cursor()

            cur.executemany(sql, query_params_list)

            conn.commit()

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

        data_record = {
            'id': record['recordId'],
            'hashtag': hashtag,
            'message': dict_data,
            'sentiment': sentiment,
            'signal': signal
        }

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(data_record).encode('utf-8')).decode('utf-8')
        }

        output.append(output_record)

    print(output)
    return {'records': output}
