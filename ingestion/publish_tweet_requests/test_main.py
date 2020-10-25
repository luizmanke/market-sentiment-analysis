#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# System packages
from dotenv import load_dotenv

# Own packages
import main

# Configurations
load_dotenv()


def test_publish_tweet_requests():
    COMPANIES = {
        "TEST": {"id": "01234", "searches": ["test", "#test", "#test3"]}
    }
    main._run(COMPANIES)
