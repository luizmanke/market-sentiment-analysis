# System packages
import apache_beam as beam
import argparse
import base64
import emoji
import logging
from apache_beam.io.textio import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
from google.cloud import language


def run(argv=None):

    # Parse arguments
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)

    # Create pipeline
    pipeline_options = PipelineOptions()
    custom_args = pipeline_options.view_as(CustomPipelineOptions)
    p = beam.Pipeline(options=pipeline_options)
    _ = (
        p
        | "Load from Storage" >> ReadFromText(
            custom_args.input_file_path, skip_header_lines=1)
        | "Preprocess" >> beam.ParDo(Preprocess())
        | "Compute Sentiments" >> beam.ParDo(ComputeSentiment())
        | "Save to BigQuery" >> beam.io.WriteToBigQuery(
            table=custom_args.output_table_path,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
    p.run()


class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--input_file_path", default="gs://project-id/file")
        parser.add_value_provider_argument("--output_table_path")


class Preprocess(beam.DoFn):
    def process(self, item):
        elements = item.split(",")
        tweet_date, tweet_id = elements[:2]
        retweet_count, favorite_count, ticker = elements[-3:]

        tweet_date = datetime.strptime(tweet_date, "%a %b %d %H:%M:%S +0000 %Y")
        tweet_date = tweet_date.strftime("%Y-%m-%d %H:%M:%S")
        text = ",".join(elements[2:-3])

        data = {
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "tweet_date": tweet_date,
            "tweet_id": int(tweet_id),
            "tweet": _decode_string(text),
            "retweet_count": int(retweet_count),
            "favorite_count": int(favorite_count),
            "ticker": ticker
        }
        return [data]


def _decode_string(string):
    b64_string = string.encode("utf-8")
    binary_string = base64.b64decode(b64_string)
    string_decoded = binary_string.decode("utf-8")
    string_w_emoji = emoji.emojize(string_decoded)
    return string_w_emoji


class ComputeSentiment(beam.DoFn):
    def process(self, item):
        client = language.LanguageServiceClient()
        doc = language.Document(
            content=item["tweet"],
            type_=language.Document.Type.PLAIN_TEXT
        )
        response = client.analyze_sentiment(document=doc)
        item["sentiment"] = response.document_sentiment.score
        return [item]


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
