echo $GOOGLE_CREDENTIALS > /tmp/credentials.json
gcloud auth activate-service-account --key-file /tmp/$CI_PIPELINE_ID.json
gcloud functions deploy publish_tweet_requests \
    --entry-point run \
    --runtime python38 \
    --trigger-topic tweet-request-trigger \
    --memory 128 \
    --timeout 60 \
    --set-env-vars GOOGLE_CREDENTIALS=$GOOGLE_CREDENTIALS,GOOGLE_PROJECT_ID=$GOOGLE_PROJECT_ID,GOOGLE_TOPIC_ID=$GOOGLE_TOPIC_ID
