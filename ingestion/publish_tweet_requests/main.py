#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# System packages
import json
import os
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from google.oauth2.service_account import Credentials


def run():
    CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    TOPIC_ID = os.getenv("GOOGLE_TOPIC_ID")
    COMPANIES = {
        "PETR": {"id": "18750", "searches": ["petrobras", "#petr", "#petr3", "#petr4"]}
    }

    # Get date period
    now = datetime.now()
    since = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    until = now.strftime("%Y-%m-%d")
    print(f" - since: {since}  until: {until}")

    # Publish messages
    credentials = Credentials.from_service_account_info(json.loads(CREDENTIALS))
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    for ticker, values in COMPANIES.items():
        data = {
            "ticker": ticker,
            "id": values["id"],
            "searches": values["searches"],
            "since": since,
            "until": until
        }
        data = json.dumps(data)
        data = data.encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(
            f" - ticker: {ticker}  id: {values['id']}  "
            f"searches: {values['searches']}  future: {future.result()}"
        )
