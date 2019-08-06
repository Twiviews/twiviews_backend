from __future__ import print_function # Python 2/3 compatibility
import json
import boto3
from TwitterSearch import *
import base64
import re

def handler(event, context):
    print('received fetch event{}'.format(event))

    body = json.loads(event['body'])
    text = body['text']

    params = event['pathParameters']
    hashtag = params['fetch']

    search_keyword=''


    ACCESS_KEY= 'AKIAYY54PZ2OXM5PH63Q'
    SECRET_KEY= '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'
    DeliveryStreamName = 'review-stream'
    most_recent_tweet = None


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
        tso = TwitterSearchOrder() # create a TwitterSearchOrder object
        tso.set_keywords([search_keyword]) # let's define all words we would like to have a look for
        tso.set_language('en') # we want to see German tweets only
        tso.set_include_entities(False) # and don't give us all those entity information

        regex_hashtag = re.compile('#[A-Za-z0-9]*')
        regex_url = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+] |[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')



        CONSUMER_KEY = 'DfRunAMlgHjbeDdocQrhK24Ge'
        CONSUMER_SECRET = '33Gy5KhE3so9GinEv4GekY2WeRwUfcATQTW3Cnf95zWskKsZhv'
        OAUTH_TOKEN = '1636335031-sD5EuMUbcn5zIBuf0iOf6EXtmKJigOfKGn0UKdL'
        OAUTH_TOKEN_SECRET = 'Z0RcvP3vhh4MsWYEDm7kMPGJA66cHcDjzlid1moV4BOzI'


        # it's about time to create a TwitterSearch object with our secret tokens
        ts = TwitterSearch(
            consumer_key = CONSUMER_KEY,
            consumer_secret = CONSUMER_SECRET,
            access_token = OAUTH_TOKEN,
            access_token_secret = OAUTH_TOKEN_SECRET
            )

        for tweet in ts.search_tweets_iterable(tso):
            if tweet['truncated'] == False and len(tweet['text']) > 10 and len(regex_hashtag.findall(tweet['text'])) == 1 and len(regex_url.findall(tweet['text'])) == 0:
                print(tweet['text'])
                #client.put_record(DeliveryStreamName=DeliveryStreamName,Record={'Data': tweet['text']})
            most_recent_tweet = tweet


    except TwitterSearchException as e: # take care of all those ugly errors if there are some
        print(e)

    response = {
        "statusCode": 200,
        "body": json.dumps({"most_recent_tweet_id": most_recent_tweet['id']})
    }

    return response
