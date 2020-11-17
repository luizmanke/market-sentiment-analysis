# Change directory
cd app_engine

# Authenticate
echo $GOOGLE_CREDENTIALS > /tmp/$CI_PIPELINE_ID.json
gcloud auth activate-service-account --key-file /tmp/$CI_PIPELINE_ID.json

# Add environment variables
echo "" >> app.yaml
echo "env_variables:" >> app.yaml
echo "  GOOGLE_CREDENTIALS: '$GOOGLE_CREDENTIALS'" >> app.yaml
echo "  GOOGLE_PROJECT_ID: '$GOOGLE_PROJECT_ID'" >> app.yaml

# Deploy
gcloud config set project $GOOGLE_PROJECT_ID
gcloud app deploy --quiet
