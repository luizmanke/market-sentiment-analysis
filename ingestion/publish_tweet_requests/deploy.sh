# Change directory
cd ingestion/publish_tweet_requests

# Authenticate
echo $GOOGLE_CREDENTIALS > /tmp/$CI_PIPELINE_ID.json
gcloud auth activate-service-account --key-file /tmp/$CI_PIPELINE_ID.json

# Deploy
echo "GOOGLE_CREDENTIALS: '$GOOGLE_CREDENTIALS'" >> .env.yaml
echo "GOOGLE_PROJECT_ID: '$GOOGLE_PROJECT_ID'" >> .env.yaml
echo "GOOGLE_SERVICE_ACCOUNT_EMAIL: '$GOOGLE_SERVICE_ACCOUNT_EMAIL'" >> .env.yaml
gcloud functions deploy publish_tweet_requests \
    --project $GOOGLE_PROJECT_ID \
    --entry-point run \
    --runtime python37 \
    --trigger-http \
    --memory 128 \
    --timeout 60 \
    --env-vars-file .env.yaml
