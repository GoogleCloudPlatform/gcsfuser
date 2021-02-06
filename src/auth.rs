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
extern crate reqwest;

use lazy_static::lazy_static;
use tame_oauth::gcp::prelude::*;

use crate::errors::HttpError;

lazy_static! {
    static ref TOKEN_PROVIDER: ServiceAccountAccess = {
    // Read in the usual key file.
    let env_key = "GOOGLE_APPLICATION_CREDENTIALS";
    let cred_path = std::env::var(env_key).expect("You must set GOOGLE_APPLICATION_CREDENTIALS environment variable");
    let key_data = std::fs::read_to_string(cred_path).expect("failed to read credential file");
    let acct_info = ServiceAccountInfo::deserialize(key_data).expect("failed to decode credential file");

    ServiceAccountAccess::new(acct_info).expect("failed to create OAuth Token Provider")
    };
}

// Given an http::Request, actually issue it via reqwest, returning
// the http::Response.
pub fn do_http_via_reqwest(
    req: http::Request<Vec<u8>>,
) -> Result<http::Response<Vec<u8>>, HttpError> {
    let (parts, body) = req.into_parts();

    assert_eq!(parts.method, http::Method::POST);

    let client = reqwest::blocking::Client::new();
    let uri = parts.uri.to_string();

    // Go from http::Request => a POST via reqwest
    let response = client
        .post(&uri)
        .headers(parts.headers)
        .body(body)
        .send()
        .expect("Failed to send POST");

    // Convert the response from reqwest => http::Response
    let mut builder = http::Response::builder()
        .status(response.status())
        .version(response.version());

    // There's no way to pass in a header map (only headers_mut and
    // headers_ref), so we go through and map() across it.
    let resp_headers = builder.headers_mut().unwrap();
    resp_headers.extend(
        response
            .headers()
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );

    // NOTE(boulos): This has to come after creating builder, because
    // .bytes() consumes the response (so response.status() above is
    // out of scope).
    let bytes: Vec<u8> = response.bytes().map_err(HttpError::Transport)?.to_vec();

    // Get the response out. (Confusingly ".body()" consumes the builder)
    builder.body(bytes).map_err(HttpError::Generic)
}

// Get a bearer token from our ServiceAccount (potentially performing an Oauth dance via HTTP)
pub fn get_token() -> Result<String, HttpError> {
    // NOTE(boulos): The service account needs both storage viewer (to
    // see objects) and *project* viewer to see the Bucket.
    let scopes = vec!["https://www.googleapis.com/auth/devstorage.read_write"];

    let token_or_req = TOKEN_PROVIDER.get_token(&scopes).unwrap();

    match token_or_req {
        TokenOrRequest::Request {
            request,
            scope_hash,
            ..
        } => {
            let response = do_http_via_reqwest(request)?;
            let token = TOKEN_PROVIDER
                .parse_token_response(scope_hash, response)
                .unwrap();
            Ok(token.access_token)
        }
        TokenOrRequest::Token(token) => Ok(token.access_token),
    }
}
