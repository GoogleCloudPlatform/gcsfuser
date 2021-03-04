# gcsfuser: A gcsfuse rewrite in Rust

[gcsfuse]: https://github.com/GoogleCloudPlatform/gcsfuse

**gcsfuser** is a WIP Rust rewrite of [gcsfuse].

## Current status

gcsfuser is not ready for nearly anything (yet). It's also an *exploratory*
project. We already have [gcsfuse], but want to use gcsfuser to mostly as a
peformance comparison that is written in a safe language (e.g., we chose not to
compare with a rewrite in C++, despite our C++ library support for GCS).

Currently, gcsfuser can only handle basic reads and writes (see the included
tests). If you use gcsfuser, it'll probably eat your data, trash your buckets,
and so on. Do not run it against data you care about. There isn't any support,
but patches are welcome (see ```CONTRIBUTING.md```).

## Running the tests

Read-only tests run successfully without a service account, but to write to a
bucket requires a service account JSON file passed via the
```GOOGLE_APPLICATION_CREDENTIALS``` environment variable and specifying your
read-write bucket in ```GCSFUSER_TEST_BUCKET```. For example:

```
GOOGLE_APPLICATION_CREDENTIALS=~/account.json GCSFUSER_TEST_BUCKET=my-private-bucket RUST_LOG=info cargo test -- --nocapture
```

If you don't set ```GOOGLE_APPLICATION_CREDENTIALS``` or don't have write access
to ```GCSFUSER_TEST_BUCKET```, you should expect tests that interact with
private buckets to fail. The current (2021-March-03) set of failures from a
straightforward ```cargo test```:

```
failures:
    bucket::tests::get_private_bucket
    bucket::tests::get_private_object
    bucket::tests::write_object_race
    bucket::tests::write_private_object
    fs::tests::large_write
    fs::tests::small_write
```
