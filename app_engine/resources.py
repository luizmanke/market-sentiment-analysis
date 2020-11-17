# System packages
import json
import os
import pandas as pd
from flask_restful import reqparse, Resource
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


class GetTweetVolume(Resource):
    def get(self):
        min_date, max_date = self._get_args()
        results = self._query_bigquery(min_date, max_date)
        dataframe = self._results_to_dataframe(results)
        volumes = self._compute_volumes(dataframe)
        return {"tweet_volume": volumes}

    def _get_args(self):
        args = get_args(["min_date", "max_date"])
        min_date = args["min_date"]
        max_date = args["max_date"]
        return min_date, max_date

    def _query_bigquery(self, min_date, max_date):
        GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
        bq_client = connect_to_bigquery()
        query_string = f"""
            SELECT *
            FROM `{GOOGLE_PROJECT_ID}.datasets.tweets`
            WHERE
                tweet_date > '{min_date}'
                AND tweet_date < '{max_date}'
        """
        results = bq_client.query(query_string).result()
        return results

    def _results_to_dataframe(self, results):
        raw_data = []
        for row in results:
            new_data = {
                "ticker": row.ticker,
                "index": pd.to_datetime(row.tweet_date)
            }
            raw_data.append(new_data)
        dataframe = pd.DataFrame(raw_data)
        return dataframe

    def _compute_volumes(self, dataframe):
        dataframe = (
            dataframe
            .set_index("index")
            .groupby("ticker")
            .resample("B")
            .apply({"ticker": "count"})
            .rename(columns={"ticker": "volume"})
        )
        volumes = {}
        for index, row in dataframe.iterrows():
            ticker, timestamp = index
            date = timestamp.strftime("%Y-%m-%d")
            if ticker not in volumes:
                volumes[ticker] = {}
            volumes[ticker][date] = int(row.values[0])
        return volumes


def connect_to_bigquery():
    GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    credentials = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS))
    bqclient = bigquery.Client(credentials=credentials, project=GOOGLE_PROJECT_ID)
    return bqclient


def get_args(keys):
    kwargs = {}

    parser = reqparse.RequestParser()
    for key in keys:
        parser.add_argument(key)

    args = parser.parse_args()
    for key in keys:
        kwargs[key] = args.get(key)

    return kwargs
