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
extern crate reqwest;

// Add a simple union type of "Either reqwest::Error" (our Transport)
// or generic Http errors.
#[derive(Debug)]
pub enum HttpError {
    Transport(reqwest::Error),
    Hyper(hyper::Error),
    Generic(http::Error),
    Status(hyper::StatusCode),
    Uri,
    Body,
    UploadFailed,
}

impl From<reqwest::Error> for HttpError {
    fn from(err: reqwest::Error) -> Self {
        HttpError::Transport(err)
    }
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
