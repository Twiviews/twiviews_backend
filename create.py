from __future__ import print_function # Python 2/3 compatibility
import json
import boto3
import uuid
from pgdbinit import pgdbinit
import psycopg2

def handler(event, context):
    print('received create event{}'.format(event))

    query_param_list = None

    id = str(uuid.uuid4())

    body = json.loads(event['body'])

    hashtag = body['hashtag']
    twiview = body['twiview']
    sentiment = body['sentiment']
    signal = body['signal']

    sql = '''
    
    INSERT INTO public."twiviews"(id, hashtag, twiview, sentiment, signal)
            VALUES (%(twiid)s, %(hashtag)s, %(twiview)s, %(sentiment)s, %(signal)s);
    
    '''

    conn = None
    try:

        conn = pgdbinit.get_conn_rds()

        cur = conn.cursor()

        query_params_list = {'twiid': id, 'hashtag': hashtag, 'twiview': twiview, 'sentiment': sentiment, 'signal': signal}

        cur.execute(sql, query_param_list)

        conn.commit()

        cur.close()

        response = {
            "statusCode": 200,
            "body": json.dumps({"id": id})
        }

        return response

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



