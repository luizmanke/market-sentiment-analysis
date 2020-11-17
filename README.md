# Market Sentiment Analysis

This project tries to find trends on the market based on the sentiment analysis of tweets.

Once a day, Twitter data is collected and the sentiment of the tweets are extracted and saved in BigQuery.

## Architecture

The whole architecture was built using Google Cloud services.
- Cloud Functions and Tasks to orchestrate the ingestion
- Dataflow to process the data
- App engine to host the services
- Cloud Storage and BigQuery to store the data

## Services

Get the volume of every stock:
http://www.market-sentiment-analysis-dev.rj.r.appspot.com/get-tweet-volume?min_date=2020-01-01&max_date=2030-01-01

## Future work

- Compare the volume of tweets with the volume of stocks traded
- Create a web interface
- Retrain the model over time