use polars_bigquery::*;
use std::env;

#[tokio::test(flavor = "multi_thread")]
async fn test_read_small_public_table() {
    let quota_project_id = env::var("GOOGLE_CLOUD_PROJECT").expect("must set GOOGLE_CLOUD_PROJECT to run integration tests");
    let result = read_bigquery_async(
        "bigquery-public-data.usa_names.usa_1910_2013",
        &quota_project_id,
        false,
        gcloud_sdk::TokenSourceType::Default,
    )
    .await;
    result.expect("public table read should work with default credentials");
}
