use polars_bigquery::*;

#[test]
fn test_read_small_public_table() {
    let _ = read_bigquery("bigquery-public-data.usa_names.usa_1910_2013");
}
