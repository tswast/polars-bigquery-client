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
