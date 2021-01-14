# gcsfuser: A gcsfuse rewrite in Rust

[gcsfuse]: https://github.com/GoogleCloudPlatform/gcsfuse

**gcsfuser** is a WIP Rust rewrite of [gcsfuse].

## Current status

gcsfuser is not ready for nearly anything (yet). It can only handle basic reads
and writes (see the included tests). If you use it, it'll probably eat your
data, trash your buckets, and so on.

## Running the tests

Running the tests *requires* a service account. My current command is:

```
GOOGLE_APPLICATION_CREDENTIALS=~/account.json RUST_BACKTRACE=full RUST_LOG=gcsfuser::fs=debug,fuser=debug cargo test -- --nocapture
```

