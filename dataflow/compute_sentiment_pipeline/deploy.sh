# Change directory
cd dataflow/compute_sentiment_pipeline

# Install dependencies
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

# Authenticate
echo $GOOGLE_CREDENTIALS > /tmp/$CI_PIPELINE_ID.json
gcloud auth activate-service-account --key-file /tmp/$CI_PIPELINE_ID.json

# Deploy
python -m main \
    --runner DataflowRunner \
    --project $GOOGLE_PROJECT_ID \
    --region $GOOGLE_REGION \
    --staging_location gs://$GOOGLE_PROJECT_ID/dataflows/staging \
    --temp_location gs://$GOOGLE_PROJECT_ID/dataflows/temp \
    --template_location gs://$GOOGLE_PROJECT_ID/dataflows/templates/compute_sentiment
