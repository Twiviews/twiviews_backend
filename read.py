from __future__ import print_function # Python 2/3 compatibility
import json
import boto3
import psycopg2

from pgdbinit import pgdbinit
from response import response


def handler(event, context):

    print('received read event{}'.format(event))

    pathParameters = event['pathParameters']
    hashtag = pathParameters['review_hash']

    sql = '''
            SELECT * FROM twiviews where hashtag = %(hashtag)s ORDER BY id desc;
            '''
    conn = None

    get_response = []

    try:

        conn = pgdbinit.get_conn_rds()
        cur = conn.cursor()
        cur.execute(sql,
                    {'hashtag': hashtag})

        row = cur.fetchone()

        while row is not None:
            json_doc = {'id': row[ 0 ],
                       'hashtag': row[ 1 ],
                       'twiview': row[ 2 ],
                       'sentiment': row[ 3 ],
                       'signal': row[ 4 ]
                       }
            get_response.append(json_doc)
            row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return response({'message': get_response}, 200)
