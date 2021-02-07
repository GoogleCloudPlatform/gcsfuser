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
    static ref TOKEN_PROVIDER: Option<ServiceAccountAccess> = {
    // Read in the usual key file.
    let env_key = "GOOGLE_APPLICATION_CREDENTIALS";
    let cred_env = std::env::var(env_key);
    let cred_path = match cred_env {
        Ok(path) => path,
        Err(std::env::VarError::NotPresent) => {
        info!("{} not set. Assuming public buckets", env_key);
        return None
        },
        Err(error) => panic!("Invalid {}. Error {:?}", env_key, error)
    };

    debug!("Going to get GCP credentials from {}", cred_path);

    let key_data = std::fs::read_to_string(cred_path).expect("failed to read credential file");
    let acct_info = ServiceAccountInfo::deserialize(key_data).expect("failed to decode credential file");

    Some(ServiceAccountAccess::new(acct_info).expect("failed to create OAuth Token Provider"))
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
pub fn get_token() -> Result<Option<String>, HttpError> {
    // NOTE(boulos): The service account needs both storage viewer (to
    // see objects) and *project* viewer to see the Bucket.
    let scopes = vec!["https://www.googleapis.com/auth/devstorage.read_write"];

    if TOKEN_PROVIDER.is_none() {
        // We're doing public auth => no token.
        return Ok(None);
    }

    let provider = TOKEN_PROVIDER.as_ref().unwrap();

    let token_or_req = provider.get_token(&scopes).unwrap();

    let token = match token_or_req {
        TokenOrRequest::Request {
            request,
            scope_hash,
            ..
        } => {
            let response = do_http_via_reqwest(request)?;
            let token = provider.parse_token_response(scope_hash, response).unwrap();
            token
        }
        TokenOrRequest::Token(token) => token,
    };

    Ok(Some(token.access_token))
}

// Optionally add the auth header to a request builder.
pub fn add_auth_header(builder: &mut http::request::Builder) -> Result<(), HttpError> {
    let token = get_token()?;

    if token.is_none() {
        return Ok(());
    }

    let token = token.unwrap();
    let headers = builder.headers_mut().unwrap();
    let formatted = format!("Bearer {}", token);
    let as_value = http::header::HeaderValue::from_str(&formatted);
    match as_value {
        Ok(value) => {
            headers.append(http::header::AUTHORIZATION, value);
            Ok(())
        }
        Err(err) => Err(HttpError::Generic(err.into())),
    }
}
