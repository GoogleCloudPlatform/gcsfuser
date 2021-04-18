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

// Add a simple union type of our various sources of HTTP-ish errors.
#[derive(Debug)]
pub enum HttpError {
    Hyper(hyper::Error),
    Generic(http::Error),
    Status(hyper::StatusCode),
    Uri,
    Body,
    UploadFailed,
}

impl From<hyper::Error> for HttpError {
    fn from(err: hyper::Error) -> Self {
        HttpError::Hyper(err)
    }
}

impl From<http::uri::InvalidUri> for HttpError {
    fn from(_: http::uri::InvalidUri) -> Self {
        HttpError::Uri
    }
}

impl From<http::Error> for HttpError {
    fn from(err: http::Error) -> Self {
        HttpError::Generic(err)
    }
}

impl HttpError {
    // Encode the GCS rules on retrying.
    pub fn should_retry_gcs(&self) -> bool {
        match self {
            // If hyper says this is a user or parse error, it won't
            // change on a retry.
            Self::Hyper(e) => !e.is_parse() && !e.is_user(),
            // https://cloud.google.com/storage/docs/json_api/v1/status-codes
            // come in handy as a guide. In fact, it lists *408* as an
            // additional thing you should retry on (not just the usual 429).
            Self::Status(code) => match code.as_u16() {
                // All 2xx are good (and aren't expected here, but are okay)
                200..=299 => true,
                // Any 3xx is "bad".
                300..=399 => false,
                // Both 429 *and* 408 are documented as needing retries.
                408 | 429 => true,
                // Other 4xx should not be retried.
                400..=499 => false,
                // Any 5xx is fair game for retry.
                500..=599 => true,
                // Anything else is a real surprise.
                _ => false,
            },
            // TODO(boulos): Add more here. As we need them.
            _ => {
                debug!("Asked for should_retry_gcs({:#?}). Saying no", self);
                false
            }
        }
    }
}
