from __future__ import print_function # Python 2/3 compatibility
import json
import boto3
from twitter import *
import base64
import re

from twitter.parse_tweet import ParseTweet

import twitter_proxy
import checkpoint

STREAM_MODE_ENABLED = False
since_id = None
BATCH_SIZE = 100
search_keyword=''
ACCESS_KEY= 'AKIAYY54PZ2OXM5PH63Q'
SECRET_KEY= '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'
DeliveryStreamName = 'review-stream'
most_recent_tweet = None
tweets = []

def handler(event, context):
    print('received fetch event{}'.format(event))

    pathParameters = event['pathParameters']
    hashtag = pathParameters['review_hash']

    client = boto3.client('firehose',
                        region_name='us-east-1',
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY
                          )

    try:
        if hashtag.startswith('#'):
            search_keyword = hashtag
        else:
            search_keyword = '#' + hashtag

        tweets = _search(search_keyword)

        for tweet in tweets:
            parsed_tweet = ParseTweet(None, json.dumps(tweet))
            if len(parsed_tweet.Hashtags) == 1:
                if not parsed_tweet.UserHandles:
                    client.put_record(DeliveryStreamName=DeliveryStreamName,Record={'Data': tweet['full_text']})

    except Exception as e: # take care of all those ugly errors if there are some
        print(e)

    response = {
        "statusCode": 200,
        "body": json.dumps({"most_recent_tweet_id": since_id})
    }

    return response


def _search(search_keyword):
    since_id = None
    if STREAM_MODE_ENABLED:
        since_id = checkpoint.last_id()

    tweets = []
    while True:
        result = twitter_proxy.search(search_keyword, since_id)
        if not result['statuses']:
            # no more results
            break

        tweets = result['statuses']
        since_id = result['search_metadata']['max_id']

        if STREAM_MODE_ENABLED:
            checkpoint.update(since_id)

    return tweets
