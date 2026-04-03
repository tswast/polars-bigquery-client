from __future__ import annotations

import polars as pl

from . import polars_bigquery


def read_bigquery(table_id: str) -> pl.DataFrame:
    # TODO(tswast): Take a credentials argument and make sure we can translate
    # that into something that can be used from Rust, ideally including info on
    # how to refresh the credentials.
    return polars_bigquery.read_bigquery(table_id)
