# Change directory
cd dataflow/compute_sentiment_trigger

# Authenticate
echo $GOOGLE_CREDENTIALS > /tmp/$CI_PIPELINE_ID.json
gcloud auth activate-service-account --key-file /tmp/$CI_PIPELINE_ID.json

# Deploy
echo "GOOGLE_CREDENTIALS: '$GOOGLE_CREDENTIALS'" >> .env.yaml
echo "GOOGLE_PROJECT_ID: '$GOOGLE_PROJECT_ID'" >> .env.yaml
echo "GOOGLE_REGION: '$GOOGLE_REGION'" >> .env.yaml
gcloud functions deploy compute_sentiment_trigger \
    --project $GOOGLE_PROJECT_ID \
    --region $GOOGLE_REGION \
    --entry-point run \
    --runtime python37 \
    --trigger-resource gs://$GOOGLE_PROJECT_ID-tweets-requested/ \
    --trigger-event google.storage.object.finalize \
    --memory 128 \
    --timeout 300 \
    --env-vars-file .env.yaml
