# gcsfuser: A gcsfuse rewrite in Rust

[gcsfuse]: https://github.com/GoogleCloudPlatform/gcsfuse

**gcsfuser** is a WIP Rust rewrite of [gcsfuse].

## Current status

gcsfuser is not ready for nearly anything (yet). It can only handle basic reads
and writes (see the included tests). If you use it, it'll probably eat your
data, trash your buckets, and so on.

## Running the tests

Read-only tests run successfully without a service account, but to
write to a bucket requires a service account JSON file passed via the
```GOOGLE_APPLICATION_CREDENTIALS``` environment variable. For
example:

```
GOOGLE_APPLICATION_CREDENTIALS=~/account.json RUST_BACKTRACE=full RUST_LOG=gcsfuser::fs=debug,fuser=debug cargo test -- --nocapture
```

If you don't set GOOGLE_APPLICATION_CREDENTIALS or don't have access
to the currently hardcoded test bucket, you should expect tests that
interact with those private buckets to fail. The current (2021-Feb-27)
set of failures from a straightforward ```cargo test```:

```
failures:
    bucket::tests::get_private_bucket
    bucket::tests::get_private_object
    bucket::tests::write_object_race
    bucket::tests::write_private_object
    fs::tests::large_write
    fs::tests::small_write
```
