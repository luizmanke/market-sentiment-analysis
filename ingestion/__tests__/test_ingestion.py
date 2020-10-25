#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
        "TEST": {"id": "01234", "searches": ["test", "#test", "#test"]}
    })
    bucket = request_tweets._connect_to_cloud_storage_bucket()

    # Wait for file
    i = 0
    while bucket.get_blob("TEST.csv") is None:
        time.sleep(60)
        i += 1
        if i == 5:
            raise Exception("Data was not ingested")

    # Delete file
    bucket.delete_blob("TEST.csv", timeout=30)
