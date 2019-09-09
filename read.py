from __future__ import print_function # Python 2/3 compatibility
import json
import boto3
import psycopg2

from pgdbinit import pgdbinit

def handler(event, context):

    print('received read event{}'.format(event))

    pathParameters = event['pathParameters']
    hashtag = pathParameters['review_hash']

    sql = "SELECT * FROM twiviews ORDER BY hashtag"
    conn = None

    get_response = {}

    try:

        conn = pgdbinit.get_conn_rds()
        cur = conn.cursor()
        cur.execute(sql)

        row = cur.fetchone()

        while row is not None:
            get_response.append(json.dumps(row))
            row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    response = {
        "statusCode": 200,
        "body": json.dumps(get_response)
    }

    return response
