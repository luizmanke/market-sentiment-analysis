#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# System packages
import json
import os
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from google.oauth2.service_account import Credentials


def run():
    COMPANIES = {
        "PETR": {"id": "18750", "searches": ["petrobras", "#petr", "#petr3", "#petr4"]}
    }
    since, until = _get_period()
    _publish_tweet_requests(COMPANIES, since, until)


def _get_period():
    now = datetime.now()
    since = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    until = now.strftime("%Y-%m-%d")
    print(f" - since: {since}  until: {until}")
    return since, until


def _publish_tweet_requests(companies, since, until):
    publisher, topic_path = _connect_to_google_pubsub(topic="tweet-request")
    for ticker, values in companies.items():
        data = {
            "ticker": ticker,
            "searches": values["searches"],
            "since": since,
            "until": until
        }
        data = json.dumps(data)
        data = data.encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(
            f" - ticker: {ticker}  "
            f"searches: {values['searches']}  "
            f"future: {future.result()}"
        )


def _connect_to_google_pubsub(topic):
    CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    credentials = Credentials.from_service_account_info(json.loads(CREDENTIALS))
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(PROJECT_ID, topic)
    return publisher, topic_path
