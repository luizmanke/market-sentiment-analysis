# System packages
import pytest
import time
from dotenv import load_dotenv

# Own packages
from ..publish_tweet_requests import main as publish_tweet_requests
from ..request_tweets import main as request_tweets

# Configurations
load_dotenv()


@pytest.mark.integration
def test_ingestion():
    publish_tweet_requests._run({
        "TEST": {"id": "", "searches": ["ambev", "abev", "abev3"]}
    })
    bucket = request_tweets._connect_to_cloud_storage_bucket()

    # Wait for file
    for _ in range(10):
        for blob in bucket.list_blobs():
            if "TEST" in blob.name:
                return
        time.sleep(10)
    raise Exception("Data was not ingested.")
