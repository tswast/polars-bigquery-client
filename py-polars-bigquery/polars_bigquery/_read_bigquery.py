from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from . import polars_bigquery
from .core import resource_references

if TYPE_CHECKING:
    from google.cloud import bigquery


def _parse_table_id(table_id: str | bigquery.TableReference | bigquery.Table) -> str:
    """Parse a table ID into a string format project.dataset.table."""
    if (
        hasattr(table_id, "project")
        and hasattr(table_id, "dataset_id")
        and hasattr(table_id, "table_id")
    ):
        return f"{table_id.project}.{table_id.dataset_id}.{table_id.table_id}"

    if not isinstance(table_id, str):
        raise TypeError(
            f"Expected table_id to be a string or BigQuery Table object, got {type(table_id)}"
        )

    ref = resource_references.parse_table_id(table_id)
    if not isinstance(ref, resource_references.BigQueryTableId):
        raise TypeError("BigLake tables are not supported yet.")

    normalized = f"{ref.project_id}.{ref.dataset_id}.{ref.table_id}"
    return normalized


def read_bigquery(
    table_id: str | bigquery.TableReference | bigquery.Table,
) -> pl.DataFrame:
    # TODO(tswast): Take a credentials argument and make sure we can translate
    # that into something that can be used from Rust, ideally including info on
    # how to refresh the credentials.
    parsed_id = _parse_table_id(table_id)
    return polars_bigquery.read_bigquery(parsed_id)
