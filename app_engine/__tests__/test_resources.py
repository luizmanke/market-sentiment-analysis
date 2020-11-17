# System packages
import pandas as pd
from dotenv import load_dotenv

# Own packages
from ..resources import GetTweetVolume

# Configurations
load_dotenv()


def test_query_bigquery():
    _ = GetTweetVolume()._query_bigquery(
        min_date="2020-01-01",
        max_date="2030-01-01"
    )


def test_results_to_dataframe():

    class AuxObject:
        def __init__(self, ticker, tweet_date):
            self.ticker = ticker
            self.tweet_date = tweet_date

    results = [
        AuxObject("A", "2020-01-01"),
        AuxObject("A", "2020-01-02"),
        AuxObject("A", "2020-01-03"),
        AuxObject("A", "2020-01-03"),
        AuxObject("B", "2020-01-01"),
        AuxObject("B", "2020-01-01"),
        AuxObject("B", "2020-01-02"),
        AuxObject("B", "2020-01-03")
    ]
    dataframe = GetTweetVolume()._results_to_dataframe(results)

    assert type(dataframe) == pd.DataFrame
    assert "ticker" in dataframe.columns
    assert "index" in dataframe.columns
    assert len(dataframe) == len(results)


def test_compute_volumes():
    dataframe = pd.DataFrame([
        {"ticker": "A", "index": pd.to_datetime("2020-01-01")},
        {"ticker": "A", "index": pd.to_datetime("2020-01-02")},
        {"ticker": "A", "index": pd.to_datetime("2020-01-03")},
        {"ticker": "A", "index": pd.to_datetime("2020-01-03")},
        {"ticker": "B", "index": pd.to_datetime("2020-01-01")},
        {"ticker": "B", "index": pd.to_datetime("2020-01-01")},
        {"ticker": "B", "index": pd.to_datetime("2020-01-02")},
        {"ticker": "B", "index": pd.to_datetime("2020-01-03")}
    ])
    volumes = GetTweetVolume()._compute_volumes(dataframe)

    assert type(volumes) == dict
    assert "A" in volumes
    assert "B" in volumes
