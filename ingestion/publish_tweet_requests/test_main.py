#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# System packages
from datetime import datetime
from dotenv import load_dotenv

# Own packages
import main

# Configurations
load_dotenv()


def test_get_period():
    since, until = main._get_period()
    datetime.strptime(since, "%Y-%m-%d")
    datetime.strptime(until, "%Y-%m-%d")


def test_connect_to_google_pubsub():
    main._connect_to_google_pubsub(topic="tweet-request")
