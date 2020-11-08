# System packages
import apache_beam as beam
import argparse
import base64
import emoji
import logging
from apache_beam.io.textio import ReadFromText
from apache_beam.ml.gcp import naturallanguageml as nlp
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime


def run(argv=None):
    NLP_FEATURES = nlp.types.AnnotateTextRequest.Features(
        extract_entities=False,
        extract_document_sentiment=True,
        extract_syntax=False
    )

    # Parse arguments
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)

    # Create pipeline
    pipeline_options = PipelineOptions()
    custom_args = pipeline_options.view_as(CustomPipelineOptions)
    p = beam.Pipeline(options=pipeline_options)
    data_preprocessed = (
        p
        | "Load from Storage" >> ReadFromText(
            custom_args.input_file_name, skip_header_lines=1)
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
            custom_args.output_table_name,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
    p.run()


class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--input_file_name")
        parser.add_value_provider_argument("--output_table_name")


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
            "tweet": _decode_string(text),
            "retweet_count": int(retweet_count),
            "favorite_count": int(favorite_count)
        }
        return [(data["tweet"], data)]


def _decode_string(string):
    b64_string = string.encode("utf-8")
    binary_string = base64.b64decode(b64_string)
    string_decoded = binary_string.decode("utf-8")
    string_w_emoji = emoji.emojize(string_decoded)
    return string_w_emoji


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


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
