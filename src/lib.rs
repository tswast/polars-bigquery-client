// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod bigquery;

use pyo3::prelude::*;
use pyo3::pyfunction;
use pyo3_polars::PyDataFrame;

use crate::bigquery::read_bigquery_async;

#[pyfunction]
pub fn read_bigquery(table_id: &str) -> pyo3::PyResult<PyDataFrame> {
    let rt = pyo3_async_runtimes::tokio::get_runtime();
    let result = rt.block_on(read_bigquery_async(table_id));
    match result {
        Ok(value) => Ok(pyo3_polars::PyDataFrame(value)),
        Err(err) => Err(pyo3::PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            err.to_string(),
        )),
    }
}

#[pymodule]
fn polars_bigquery(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(read_bigquery)).unwrap();

    Ok(())
}
