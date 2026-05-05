from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from . import polars_bigquery


def read_bigquery(
    *,
    table: str | None = None,
    query: str | None = None,
    quota_project_id: str,
    credentials_provider: pl.CredentialsProviderGCP | None = None,
) -> pl.DataFrame:
    if not table and not query:
        raise ValueError("One of `table` or `query` must be provided.")

    if table and query:
        raise ValueError("Only one of `table` or `query` can be provided.")

    if not credentials_provider:
        credentials_provider = pl.CredentialsProviderGCP(quota_project_id=quota_project_id)





    # TODO(tswast): Take a credentials argument and make sure we can translate
    # that into something that can be used from Rust, ideally including info on
    # how to refresh the credentials.
    parsed_id = _parse_table_id(table)
    return polars_bigquery.read_bigquery(parsed_id)
