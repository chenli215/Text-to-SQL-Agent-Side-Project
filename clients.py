# clients.py
import os
from google.cloud import bigquery
from openai import OpenAI


def get_bq_client() -> bigquery.Client:
    billing_project = os.getenv("BQ_BILLING_PROJECT")
    if not billing_project:
        raise RuntimeError("Missing BQ_BILLING_PROJECT in .env")
    return bigquery.Client(project=billing_project)


def get_openai_client() -> OpenAI:
    k = os.getenv("OPENAI_API_KEY")
    if not k:
        raise RuntimeError("OPENAI_API_KEY missing in .env")
    return OpenAI(api_key=k)
