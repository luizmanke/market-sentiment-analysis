# Authenticate
echo $GOOGLE_CREDENTIALS > /tmp/$CI_PIPELINE_ID.json
gcloud auth activate-service-account --key-file /tmp/$CI_PIPELINE_ID.json

# Deploy
echo "GOOGLE_CREDENTIALS: '$GOOGLE_CREDENTIALS'" >> .env.yaml
echo "GOOGLE_PROJECT_ID: '$GOOGLE_PROJECT_ID'" >> .env.yaml
gcloud functions deploy publish_tweet_requests \
    --entry-point run \
    --runtime python38 \
    --trigger-topic tweet-request-trigger \
    --memory 128 \
    --timeout 60 \
    --env-vars-file .env.yaml
