# System packages
import json
import os
import pytest
import time
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Own packages
from ..compute_sentiment_pipeline import main as compute_sentiment

# Configurations
load_dotenv()


@pytest.mark.integration
def test_publish_dataflow():
    GITLAB_COMMIT_TIME = os.getenv("GITLAB_COMMIT_TIME")
    GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")

    credentials = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS))
    bq_client = bigquery.Client(credentials=credentials, project=GOOGLE_PROJECT_ID)
    select_string = f"""
        SELECT *
        FROM `{GOOGLE_PROJECT_ID}.datasets.tweets`
        WHERE created_at > '{GITLAB_COMMIT_TIME}'
    """
    delete_string = f"""
        DELETE
        FROM `{GOOGLE_PROJECT_ID}.datasets.tweets`
        WHERE created_at > '{GITLAB_COMMIT_TIME}'
    """

    for _ in range(15):
        results = bq_client.query(select_string).result()
        time.sleep(60)
        if results.total_rows > 0:
            results = bq_client.query(delete_string)
            return
    raise Exception("Data was not saved.")


def test_Preprocess():
    GROUND_TRUTH = {
        "tweet_date": "Sat Nov 01 23:50:20 +0000 2020",
        "tweet_id": "123456789",
        "tweet": "RXUsIEpvw6NvLCBWaW5pIGUgTGVvIGdvc3RhbW9zIGRlIHByb2dyYW1hcg==",
        "retweet_count": "11",
        "favorite_count": "22",
        "ticker": "TEST"
    }

    input_string = ",".join(GROUND_TRUTH.values())
    function = compute_sentiment.Preprocess()
    item = function.process(input_string)

    assert type(item) == list
    assert type(item[0]) == dict
    assert "created_at" in item[0]
    for key in GROUND_TRUTH.keys():
        assert key in item[0]
    assert item[0]["tweet"] == "Eu, João, Vini e Leo gostamos de programar"


def test_ComputeSentiment():
    INPUT_ITEM = {
        "tweet": "Eu gosto de programar",
        "a": 1
    }
    function = compute_sentiment.ComputeSentiment()
    output_item = function.process(INPUT_ITEM)
    assert "sentiment" in output_item[0]
    for key in INPUT_ITEM.keys():
        assert key in output_item[0]
