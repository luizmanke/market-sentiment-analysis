# Change directory
cd dataflow/compute_sentiment_pipeline

# Deploy
python -m main \
    --runner DataflowRunner \
    --project $GOOGLE_PROJECT_ID \
    --region $GOOGLE_REGION \
    --staging_location gs://$GOOGLE_PROJECT_ID/dataflows/staging \
    --temp_location gs://$GOOGLE_PROJECT_ID/dataflows/temp \
    --template_location gs://$GOOGLE_PROJECT_ID/dataflows/templates/compute_sentiment
