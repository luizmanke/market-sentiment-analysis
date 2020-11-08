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
    assert type(item[0]) == tuple
    assert item[0][0] == "Eu, João, Vini e Leo gostamos de programar"
    assert type(item[0][1]) == dict
    assert "created_at" in item[0][1]
    for key in GROUND_TRUTH.keys():
        assert key in item[0][1]


def test_CreateDocuments():
    ITEM = (
        "",
        {"tweet": "text"}
    )

    function = compute_sentiment.CreateDocuments()
    doc = function.process(ITEM)

    assert type(doc) == list
    assert type(doc[0]) == beam.ml.gcp.naturallanguageml.Document


def test_RefactorSentimentResponse():
    class Object:
        pass

    response = Object()
    response.document_sentiment = Object()
    response.document_sentiment.score = 0.8
    response.sentences = []
    for text in ["some", "random", "text"]:
        new_sentence = Object()
        new_sentence.text = Object()
        new_sentence.text.content = text
        response.sentences.append(new_sentence)

    function = compute_sentiment.RefactorSentimentResponse()
    item = function.process(response)

    assert type(item) == list
    assert type(item[0]) == tuple
    assert item[0][0] == "some random text"
    assert item[0][1] == {"sentiment": 0.8}


def test_Postprocess():
    DICT_1 = {"a": 1}
    DICT_2 = {"b": 2}
    ITEM = ("", [[DICT_1], [DICT_2]])

    function = compute_sentiment.Postprocess()
    new_item = function.process(ITEM)

    expected_dict = DICT_1.copy()
    expected_dict.update(DICT_2)
    assert type(new_item) == list
    assert new_item[0] == expected_dict
