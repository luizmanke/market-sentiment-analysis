# System packages
import apache_beam as beam
import os
from apache_beam.io.textio import ReadFromText
from apache_beam.ml.gcp import naturallanguageml as nlp
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime


def run(event, context):
    file_name = event["name"]
    _publish_dataflow(file_name)


def _publish_dataflow(file_name):
    GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    NLP_FEATURES = nlp.types.AnnotateTextRequest.Features(
        extract_entities=False,
        extract_document_sentiment=True,
        extract_syntax=False
    )

    # Define pipeline options
    pipeline_options = {
        "project": GOOGLE_PROJECT_ID,
        "region": os.getenv("GOOGLE_REGION"),
        "job_name": file_name[:-4].lower(),
        "save_main_session": True,
        "temp_location": "gs://tweets-requested/dataflow/temp",
        "staging_location": "gs://tweets-requested/dataflow/staging"
    }
    options = PipelineOptions.from_dictionary(pipeline_options)
    options.view_as(StandardOptions).runner = "dataflow"

    # Create pipeline
    p = beam.Pipeline(options=options)
    data_preprocessed = (
        p
        | "Load from Storage" >> ReadFromText(
            f"gs://tweets-requested/{file_name}", skip_header_lines=1)
        | "Preprocess" >> beam.ParDo(Preprocess())
    )
    tweet_sentiments = (
        data_preprocessed
        | "Create NLP Documents" >> beam.ParDo(CreateDocuments())
        | "Compute Sentiments" >> nlp.AnnotateText(NLP_FEATURES)
        | "Refactor Response" >> beam.ParDo(RefactorSentimentResponse())
    )
    _ = (
        (data_preprocessed, tweet_sentiments)
        | "Group Results" >> beam.CoGroupByKey()
        | "Postprocess" >> beam.ParDo(Postprocess())
        | "Save to BigQuery" >> beam.io.WriteToBigQuery(
            f"{GOOGLE_PROJECT_ID}:datasets.tweets",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
    p.run()


class Preprocess(beam.DoFn):
    def process(self, item):
        elements = item.split(",")
        tweet_date, id_ = elements[:2]
        retweet_count, favorite_count, = elements[-2:]

        tweet_date = datetime.strptime(tweet_date, "%Y-%m-%d %H:%M:%S")
        tweet_date = tweet_date.strftime("%Y-%m-%d %H:%M:%S")
        text = ",".join(elements[2:-2])

        data = {
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "tweet_date": tweet_date,
            "id": int(id_),
            "tweet": text,
            "retweet_count": int(retweet_count),
            "favorite_count": int(favorite_count)
        }
        return [(data["tweet"], data)]


class CreateDocuments(beam.DoFn):
    def process(self, item):
        doc = nlp.Document(item[1]["tweet"], language_hint="pt")
        return [doc]


class RefactorSentimentResponse(beam.DoFn):
    def process(self, item):
        sentences = [sentence.text.content for sentence in item.sentences]
        new_item = (
            " ".join(sentences),
            {"sentiment": item.document_sentiment.score}
        )
        return [new_item]


class Postprocess(beam.DoFn):
    def process(self, item):
        new_item = item[1][0][0]
        new_item.update(item[1][1][0])
        return [new_item]
