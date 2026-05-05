"""Utilities for running a query in BigQuery and getting the results as a table.

TODO(tswast): Implement this in Rust using jobs.query and the
JOB_CREATION_OPTIONAL parameter to improve latency in small query results.
"""

import polars as pl
import requests

import polars_bigquery.core.version
import polars_bigquery.exceptions


_BIGQUERY_ENDPOINT = "https://bigquery.googleapis.com/bigquery/v2"



def _get_user_agent() -> str:
    return f"polars-bigquery/{polars_bigquery.core.version.__version__}"


def _get_jobs_insert_url(quota_project_id: str) -> str:
    return f"{_BIGQUERY_ENDPOINT}/projects/{quota_project_id}/jobs"


def _get_jobs_insert_body(query: str) -> dict:
    # TODO(tswast): include a job ID for safer API-level retries.
    return {
        "configuration": {
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        }
    }


def _get_jobs_get_url(job_id: str, quota_project_id: str) -> str:
    return f"{_BIGQUERY_ENDPOINT}/projects/{quota_project_id}/jobs/{job_id}"


def _get_query_results_url(job_id: str, quota_project_id: str) -> str:
    return f"{_BIGQUERY_ENDPOINT}/projects/{quota_project_id}/queries/{job_id}/results"


def _wait_for_job(job_id: str, quota_project_id: str) -> dict:
    job = requests.get(_get_jobs_get_url(job_id, quota_project_id)).json()

    if job["status"]["state"] == "DONE":
        return job

    if job["status"]["state"] == "ERROR":
        # TODO(tswast): Retry jobs that fail for a retriable reason.
        raise polars_bigquery.exceptions.BigQueryError(job["status"]["errorResult"]["message"])




def run_query(
    query: str, quota_project_id: str, credentials_provider: pl.CredentialsProviderGCP
) -> str:
    pass
