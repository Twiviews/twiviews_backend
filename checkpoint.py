"""Manages search checkpoint (used when stream mode enabled)."""

import os

import boto3
from boto3.dynamodb.conditions import Attr, Or
from botocore.exceptions import ClientError
from pgdbinit import pgdbinit
import psycopg2
import json


def last_id():
    """Return last checkpoint tweet id."""

    last_sinceid_sql = ('select since_id from public."checkpoint";')

    conn = None
    resultset = None
    try:

        conn = pgdbinit.get_conn_rds( )

        cur = conn.cursor( )

        cur.execute(last_sinceid_sql)
        row = cur.fetchone( )

        if row is not None:
            resultset = row

        cur.close( )

        conn.commit( )

        return resultset[ 0 ]

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close( )

    return None


def update(since_id):
    """Update checkpoint to given tweet id."""

    update_sinceid_sql = ''' update public."checkpoint" set since_id =  %s '''

    try:
        conn = pgdbinit.get_conn_rds()

        cur = conn.cursor()

        cur.execute(update_sinceid_sql, (since_id,))
        cur.close()
        conn.commit()

        print("Update of since_id successful!")

    except ClientError as e:
        raise


if __name__ == '__main__':
    if (last_id() == 1):
        update(0)
