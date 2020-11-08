# System packages
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Own packages
from ..request_tweets import main

# Configurations
load_dotenv()


def test_request_tweets():
    now = datetime.now()
    since = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    until = now.strftime("%Y-%m-%d")
    tweets = main._request_tweets(
        ticker="TEST",
        searches=["ambev", "abev", "abev3"],
        since=since,
        until=until
    )
    assert type(tweets) == pd.DataFrame
    assert not tweets.empty


def test_connect_to_cloud_storage_bucket():
    main._connect_to_cloud_storage_bucket()
