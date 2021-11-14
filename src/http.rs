// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate http;
extern crate hyper;
extern crate hyper_rustls;
extern crate tokio;
extern crate url;

use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::errors::HttpError;

// The docs say "32 or 64" seconds. Let's go with 32.
pub const GCS_DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(32);

// If we've tried for two minutes. It's probably dead.
pub const GCS_DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

pub type GcsHttpClient =
    hyper::client::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>;

pub fn new_client() -> GcsHttpClient {
    // NOTE(boulos): Previously, I was passing some default headers
    // here. Now that bearer_auth is per request, this is less needed,
    // but we can change that later.
    let https = hyper_rustls::HttpsConnector::with_native_roots();
    hyper::Client::builder().build::<_, hyper::Body>(https)
}

// Do a request with retry (using our default parameters), and get the
// response *bytes* out. If you need the full response, use
// request_with_gcs_retry directly.
pub async fn do_gcs_request(
    client: &GcsHttpClient,
    request: hyper::Request<hyper::body::Body>,
) -> Result<hyper::body::Bytes, HttpError> {
    // If we want most callers to exercise the unreliable "just issue
    // a single request" codepath, this feature lets them do it.
    #[cfg(feature = "unreliable")]
    let response = _do_one_request(client, request).await;

    // Otherwise, use our explicit retry with truncated, exponential backoff.
    #[cfg(not(feature = "unreliable"))]
    let response = request_with_gcs_retry(
        client,
        request,
        GCS_DEFAULT_MAX_BACKOFF,
        GCS_DEFAULT_TIMEOUT,
    )
    .await;

    // Print our debugging info, even if it's an error.
    debug!("{:#?}", response);

    // to_bytes() will consume the (potential) stream and wrap it in a Bytes struct.
    let bytes_or_err = hyper::body::to_bytes(response?).await;

    // We don't have the to_bytes errors in HttpError, so can't use ?
    // above. If we have any problems, let's say it was
    // HttpError::Body.
    bytes_or_err.map_err(|_| HttpError::Body)
}

// Do a request with the GCS exponential backoff requirements. See
// https://cloud.google.com/storage/docs/retry-strategy for more info.
// Use this function directly if you need the response object. If you
// just want the bytes, use do_gcs_request.
pub async fn request_with_gcs_retry(
    client: &GcsHttpClient,
    request: hyper::Request<hyper::body::Body>,
    maximum_backoff: std::time::Duration,
    timeout: std::time::Duration,
) -> Result<hyper::Response<hyper::Body>, HttpError> {
    // We'll only wait until beginning + timeout.
    let beginning = Instant::now();
    let mut attempt: u64 = 0;
    // Read from /dev/urandom or equivalent.
    let mut rng = SmallRng::from_entropy();

    // Break up the request into the body and everything else (since
    // we'll have to clone the body for each iteration, but the clone
    // will be a cheap Clone of hyper's Bytes class).
    //let (mut parts, mut body) = request.into_parts();

    // uri() borrows a reference to the URI. Just make a copy for
    // debugging (and maybe restoring purposes).
    let uri_copy = request.uri().clone();

    let (parts, body) = request.into_parts();

    trace!(
        "Requesting {} with at most {:#?} backoff up to {:#?} timeout",
        uri_copy,
        maximum_backoff,
        timeout
    );

    // Sigh. Because the Body *could* be a Stream, request isn't
    // clone-able. Luckily, we can use to_bytes to get the underlying
    // Bytes for our needs (we always have either a single u8 slice or
    // an empty body).
    let body_bytes: hyper::body::Bytes = hyper::body::to_bytes(body).await?;

    loop {
        // Issue a request. If we have a malformed request, Hyper may
        // return an actual error. A 404 though is a successful
        // *request* that has status() => 404.

        // What we *wish* we could do is just make a copy of our
        // request via:

        //let request = hyper::Request::from_parts(parts, body);

        // But instead, we have to make a copy "by hand".
        // TODO(boulos): Contribute an Extensions::is_empty, so that
        // callers can check that the
        // request doesn't have extensions (which are also not copy-able).
        let mut request_builder = hyper::Request::builder()
            .method(parts.method.clone())
            .uri(parts.uri.clone())
            .version(parts.version.clone());
        // And put a copy of the request headers on there.
        *request_builder.headers_mut().unwrap() = parts.headers.clone();

        // Take a *shallow* copy of Bytes (Bytes is cheaply clone-able).
        let bytes_clone = body_bytes.clone();

        // In theory, this only fails if the request itself is malformed.
        let request = request_builder.body(bytes_clone.into())?;

        trace!("Attempt #{} for {}", attempt, uri_copy);

        let response = _do_one_request(client, request).await;

        // If we succeeded, return immediately.
        if response.is_ok() {
            return response;
        }

        // Otherwise, handle the error.
        let err = response.unwrap_err();

        // If we got a hard error, exit immediately.
        if !err.should_retry_gcs() {
            return Err(err);
        }

        // We've had some sort of retry-able error. Retry with
        // truncated, exponential backoff, unless we've tried for too
        // long.
        let elapsed = beginning.elapsed();
        if elapsed > timeout {
            trace!(
                "After retrying for {:#?} ({} attempts), we're giving up (timeout was {:#?})",
                elapsed,
                attempt,
                timeout
            );
            // Return the error we captured above.
            return Err(err);
        }

        // Wait for 2^N seconds + [0..1000] (apparently inclusive!)
        let seconds = 1 << attempt;
        // Don't forget to increment attempt.
        attempt += 1;

        // "random_number_milliseconds is a random number of
        // milliseconds less than or equal to 1000"
        let random_number_milliseconds: u64 = rng.gen_range(0..=1000);

        // Build our duration explicitly from the two parts.
        let backoff =
            Duration::from_secs(seconds) + Duration::from_millis(random_number_milliseconds);

        // Wait at most maximum_backoff.
        let clamped = std::cmp::min(backoff, maximum_backoff);

        // Have *this* task block (hence tokio::time::sleep née
        // tokio::time::delay_for rather than std::time::sleep) until
        // the backoff is over.
        trace!("Sleeping for {:#?} before trying again", clamped);
        tokio::time::sleep(clamped).await;
    }
}

async fn _do_one_request(
    client: &GcsHttpClient,
    request: hyper::Request<hyper::body::Body>,
) -> Result<hyper::Response<hyper::Body>, HttpError> {
    // Sigh. HTTP2 errors (like REFUSED_STREAM) don't get bubbled
    // up as response errors. Instead, we need to go digging into
    // the Error and handle the retryable ones like Golang did in
    // https://github.com/golang/go/issues/20985.
    let response = client.request(request).await;

    match response {
        Err(e) => {
            // Take the Hyper Error and convert to our own HttpError (Hyper flavored).
            Err(HttpError::Hyper(e))
        }
        Ok(response) => {
            // Okay, we got a response.
            let status = response.status();

            // If it's a 2xx, just return success.
            if status.is_success() {
                return Ok(response);
            }

            // Otherwise, use the status code as our underlying
            // error. NOTE(boulos): If clients end up needing the
            // response *headers* we'll have to change this.
            Err(HttpError::Status(status))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate env_logger;

    const LANDSAT_BUCKET: &str = "gcp-public-data-landsat";
    const LANDSAT_PREFIX: &str = "LC08/01/044/034/";

    fn init() {
        // https://docs.rs/env_logger/0.8.2/env_logger/index.html#capturing-logs-in-tests
        let _ = env_logger::builder().is_test(true).try_init();
    }

    // List calls have *much* lower qps limits than other GETs. So
    // they're a good way to exercise our retry code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 64)]
    async fn test_list_high_qps() {
        init();

        let mut handles = Vec::new();
        let client = new_client();

        // Launch up to 1024 in parallel. This test sets worker_threads
        // = 64, to make sure we actually supply enough parallelism in
        // our tokio thread pool. NOTE(boulos): Changing this to 64
        // doesn't cause the unreliable / single shot code to fail.
        for i in 0..1000 {
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                let base_url = "https://www.googleapis.com/storage/v1/b";
                let bucket_url = format!("{}/{}/o", base_url, LANDSAT_BUCKET);

                let mut list_url = url::Url::parse(&bucket_url).unwrap();
                list_url
                    .query_pairs_mut()
                    .append_pair("prefix", LANDSAT_PREFIX);
                list_url.query_pairs_mut().append_pair("delimiter", "/");

                let uri: hyper::Uri = String::from(list_url).parse().unwrap();

                let body = hyper::Body::empty();

                let request = http::Request::builder()
                    .uri(uri)
                    .body(body)
                    .expect("Failed to construct our LIST request");

                // Use expect(), so that any error that bubbles up is a test failure.
                let result = do_gcs_request(&client, request)
                    .await
                    .expect("Expected to succeed!");

                // Just to show progress.
                debug!("{}: Got back {} bytes", i, result.len());
            }));
        }

        for handle in handles {
            // If any task failed, calling unwrap() will make us panic.
            handle.await.unwrap();
        }
    }
}
