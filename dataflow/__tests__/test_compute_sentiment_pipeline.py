# System packages
import apache_beam as beam
import pytest
from dotenv import load_dotenv

# Own packages
from ..compute_sentiment_pipeline import main as compute_sentiment

# Configurations
load_dotenv()


@pytest.mark.integration
def test_publish_dataflow():
    pass


def test_Preprocess():
    GROUND_TRUTH = {
        "tweet_date": "Sat Nov 01 23:50:20 +0000 2020",
        "id": "123456789",
        "tweet": "RXUsIEpvw6NvLCBWaW5pIGUgTGVvIGdvc3RhbW9zIGRlIHByb2dyYW1hcg==",
        "retweet_count": "11",
        "favorite_count": "22"
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
