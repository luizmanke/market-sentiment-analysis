# System packages
import base64
import emoji
import json
import os
import pandas as pd
import twitter
from datetime import datetime
from google.cloud import storage
from google.oauth2.service_account import Credentials
from io import StringIO


def run(request):

    data = request.get_json()
    print(f" - data: {data}")

    tweets = _request_tweets(data["ticker"], data["searches"], data["since"], data["until"])
    _save_to_cloud_storage(tweets, data["ticker"])


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
                    "tweet_date": item["created_at"],
                    "id": item["id"],
                    "tweet": _encode_string(item["full_text"]),
                    "full_text": item["full_text"],
                    "retweet_count": item["retweet_count"],
                    "favorite_count": item["favorite_count"],
                })

        # Update maximum tweet ID
        if len(response["statuses"]) == 0:
            break
        max_id = response["statuses"][-1]["id"] - 1

    return pd.DataFrame(tweets)


def _encode_string(string):
    string_wo_emoji = emoji.demojize(string)
    binary_string = string_wo_emoji.encode("utf-8")
    b64_string = base64.b64encode(binary_string)
    string_encoded = b64_string.decode("utf-8")
    return string_encoded


def _save_to_cloud_storage(tweets, file_name):
    bucket = _connect_to_cloud_storage_bucket()
    document = StringIO()
    tweets.to_csv(document, index=False)
    document.seek(0)
    date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    bucket.blob(f"{file_name}-{date}.csv").upload_from_file(document, content_type="text/csv")


def _connect_to_cloud_storage_bucket():
    CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    credentials = Credentials.from_service_account_info(json.loads(CREDENTIALS))
    client = storage.Client(credentials=credentials, project=PROJECT_ID)
    bucket = client.get_bucket(f"{PROJECT_ID}/tweets-requested")
    return bucket
