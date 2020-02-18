"""Twitter API Helper."""

import os
import twitter

CONSUMER_KEY = 'oI7F2ur5H3twWroRCDz6krIL0'
CONSUMER_SECRET = 'B2H3t0MTXZJyFTOu7YeOzvP7nEmrubY1E7QYWtSws6LXVrGRD4'
ACCESS_TOKEN = '1636335031-YImLVdnb137tiLUBJ989xf0Hbg3aGdycItMRodY'
ACCESS_TOKEN_SECRET = 'cdZEm7MPFBn3IxRJCFRuQD5WDDs6nOBTd7OfzgCP8kZ5y'


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
