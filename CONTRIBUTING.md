# How to contribute

## Setting up a development environment

Install `uv`.

Authenticate with Google Cloud using `gcloud auth application-default login`.

## Running the test suite

Set the `GOOGLE_CLOUD_PROJECT` environment variable.

Run all tests:

```
LD_LIBRARY_PATH="$HOME/.pyenv/versions/3.14.3/lib" \
  source .venv/bin/activate \
  && cargo test
```
