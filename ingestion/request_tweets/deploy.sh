# Change directory
cd ingestion/request_tweets

# Authenticate
echo $GOOGLE_CREDENTIALS > /tmp/$CI_PIPELINE_ID.json
gcloud auth activate-service-account --key-file /tmp/$CI_PIPELINE_ID.json

# Deploy
echo "GOOGLE_CREDENTIALS: '$GOOGLE_CREDENTIALS'" >> .env.yaml
echo "GOOGLE_PROJECT_ID: '$GOOGLE_PROJECT_ID'" >> .env.yaml
echo "TWITTER_ACCESS_TOKEN_KEY: '$TWITTER_ACCESS_TOKEN_KEY'" >> .env.yaml
echo "TWITTER_ACCESS_TOKEN_SECRET: '$TWITTER_ACCESS_TOKEN_SECRET'" >> .env.yaml
echo "TWITTER_CONSUMER_KEY: '$TWITTER_CONSUMER_KEY'" >> .env.yaml
echo "TWITTER_CONSUMER_SECRET: '$TWITTER_CONSUMER_SECRET'" >> .env.yaml
gcloud functions deploy request_tweets \
    --project $GOOGLE_PROJECT_ID \
    --entry-point run \
    --runtime python37 \
    --trigger-http \
    --memory 512 \
    --timeout 300 \
    --env-vars-file .env.yaml
