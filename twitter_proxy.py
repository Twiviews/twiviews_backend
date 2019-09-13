"""Twitter API Helper."""

import os
import twitter

CONSUMER_KEY = 'yTmAkvZz8EaTnm3jZ6XBIWapY'
CONSUMER_SECRET = 'Vh8dY3AuxrDpx4XmBPTg8wEnTcp9hBPJ53RjLp3xxusJgXZR0n'
ACCESS_TOKEN = '1636335031-mKwzh4tdMMQOIi723UXxM0Sn8jJJwdrJ2MQjiCD'
ACCESS_TOKEN_SECRET = 'ht9YnZ2WsZpVEIfC8LX9iOnogk8rtAst4OEy7CP8PtFwi'


def search(search_text, since_id=None):
    """Search for tweets matching the given search text."""
    return twitter_api.GetSearch(term=search_text, count=100, return_json=True, since_id=since_id)


def _create_twitter_api():
    return twitter.Api(
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        access_token_key=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET,
        tweet_mode='extended'
    )


twitter_api = _create_twitter_api( )
