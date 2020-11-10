# System packages
import json
import logging
import os
from google.oauth2.service_account import Credentials
from googleapiclient import discovery


def run(event, context):
    GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    GOOGLE_REGION = os.getenv("GOOGLE_REGION")

    # Create inputs
    file_triggered = event["name"]
    input_file_path = f"gs://{GOOGLE_PROJECT_ID}-tweets-requested/{file_triggered}"
    output_table_path = f"{GOOGLE_PROJECT_ID}:datasets.tweets"
    logging.debug(file_triggered)

    credentials = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS))
    dataflow = discovery.build(
        "dataflow",
        "v1b3",
        credentials=credentials,
        cache_discovery=False
    )
    dataflow.projects().templates().launch(
        projectId=GOOGLE_PROJECT_ID,
        gcsPath=f"gs://{GOOGLE_PROJECT_ID}-dataflows/templates/compute_sentiment",
        body={
            "environment": {
                "zone": f"{GOOGLE_REGION}-b",
                "tempLocation": f"gs://{GOOGLE_PROJECT_ID}-dataflows/temp"
            },
            "parameters": {
                "input_file_path": input_file_path,
                "output_table_path": output_table_path
            },
            "jobName": file_triggered[:-4].lower()
        }
    ).execute()
