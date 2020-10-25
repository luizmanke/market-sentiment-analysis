#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# System packages
import base64
import json
import os
import pandas as pd
import twitter
from google.cloud import storage
from google.oauth2.service_account import Credentials
from io import StringIO


def run(event, context):

    # Decode message
    message = base64.b64decode(event["data"]).decode("utf-8")
    data = json.loads(message)
    print(f" - data: {data}")

    tweets = _request_tweets(data["ticker"], data["searches"], data["since"], data["until"])
    _save_to_cloud_storage(tweets)


def _request_tweets(ticker, searches, since, until):

    # Connect
    api = twitter.Api(
        access_token_key=os.getenv("TWITTER_ACCESS_TOKEN_KEY"),
        access_token_secret=os.getenv("TWITTER_ACCESS_TOKEN_SECRET"),
        consumer_key=os.getenv("TWITTER_CONSUMER_KEY"),
        consumer_secret=os.getenv("TWITTER_CONSUMER_SECRET"),
        tweet_mode="extended"
    )

    # Request
    tweets = []
    max_id = None
    for i in range(500000):
        try:
            response = api.GetSearch(
                term="%20OR%20".join(searches),
                since=since,
                until=until,
                lang="pt",
                return_json=True,
                count=100,
                result_type="recent",
                max_id=max_id
            )
        except Exception as error:
            print(f"{i}: {error}")
            raise Exception(f" - Timeout  {i}: {error}")

        # Select tweets
        for item in response["statuses"]:
            if "retweeted_status" not in item and item["in_reply_to_status_id"] is None:
                tweets.append({
                    "created_at": item["created_at"],
                    "id": item["id"],
                    "text": item["full_text"],
                    "retweet_count": item["retweet_count"],
                    "favorite_count": item["favorite_count"],
                })

        # Update maximum tweet ID
        if len(response["statuses"]) == 0:
            break
        max_id = response["statuses"][-1]["id"] - 1

    return pd.DataFrame(tweets)


def _save_to_cloud_storage(tweets, file_name):
    bucket = _connect_to_cloud_storage_bucket()
    document = StringIO()
    tweets.to_csv(document)
    document.seek(0)
    bucket.blob(f"{file_name}.csv").upload_from_file(document, content_type="text/csv")


def _connect_to_cloud_storage_bucket():
    CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    credentials = Credentials.from_service_account_info(json.loads(CREDENTIALS))
    client = storage.Client(credentials=credentials, project=PROJECT_ID)
    bucket = client.get_bucket("tweets-requested")
    return bucket