# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import polars
import pytest

import polars_bigquery


TABLE_IDS = [
    "bigquery-public-data.usa_names.usa_1910_2013",
]


@pytest.mark.parametrize("table_id", TABLE_IDS)
def test_read_bigquery_public_data(table_id, benchmark):
    df = benchmark(polars_bigquery.read_bigquery, table_id)
    assert isinstance(df, polars.DataFrame)
    # Make sure we got all of the expected data, not just a subset.
    assert df.height > 5_000_000  # rows
    assert df.width > 0  # columns
