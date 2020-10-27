#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# System packages
import json
import os
from datetime import datetime, timedelta
from google.cloud import tasks_v2
from google.oauth2.service_account import Credentials

# Globals
PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")


def run(request):
    COMPANIES = {
        "PETR": {"id": "18750", "searches": ["petrobras", "#petr", "#petr3", "#petr4"]}
    }
    _run(COMPANIES)


def _run(companies):
    since, until = _get_period()
    _publish_tweet_requests(companies, since, until)


def _get_period():
    now = datetime.now()
    since = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    until = now.strftime("%Y-%m-%d")
    print(f" - since: {since}  until: {until}")
    return since, until


def _publish_tweet_requests(companies, since, until):
    GOOGLE_SERVICE_ACCOUNT_EMAIL = os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL")
    url = f"https://southamerica-east1-{PROJECT_ID}.cloudfunctions.net/request_tweets"
    client, queue_path = _connect_to_google_queue()

    for ticker, values in companies.items():
        data = {
            "ticker": ticker,
            "searches": values["searches"],
            "since": since,
            "until": until
        }
        data = json.dumps(data)
        data = data.encode("utf-8")

        task = {
            "name": (
                f"projects/{PROJECT_ID}/locations/southamerica-east1/"
                f"queues/tweet-request-queue/tasks/2-{ticker}"
            ),
            "http_request": {
                "oidc_token": {"service_account_email": GOOGLE_SERVICE_ACCOUNT_EMAIL},
                "http_method": tasks_v2.HttpMethod.POST,
                "headers": {"Content-type": "application/json"},
                "url": url,
                "body": data,
            }
        }
        response = client.create_task(request={"parent": queue_path, "task": task})

        print(
            f" - ticker: {ticker}  "
            f"searches: {values['searches']}  "
            f"task: {response.name}"
        )


def _connect_to_google_queue():
    CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    credentials = Credentials.from_service_account_info(json.loads(CREDENTIALS))
    client = tasks_v2.CloudTasksClient(credentials=credentials)
    queue_path = client.queue_path(PROJECT_ID, "southamerica-east1", "tweet-request-queue")
    return client, queue_path
