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

// Given an http::Request, actually issue it (via hyper), returning
// the http::Response.
async fn do_http_request(
    request: http::Request<Vec<u8>>,
) -> Result<http::Response<Vec<u8>>, HttpError> {
    // We only expect a POST.
    assert_eq!(request.method(), http::Method::POST);

    let https = hyper_rustls::HttpsConnector::with_native_roots();
    let client = hyper::Client::builder().build::<_, hyper::Body>(https);

    // Sigh, convert from request of Vec<u8> to request of hyper::Body
    let (req_part, req_body) = request.into_parts();
    let hyper_body: hyper::Body = req_body.into();
    let hyper_req = http::Request::from_parts(req_part, hyper_body);
    let response = client.request(hyper_req).await?;

    // And then back from Response<hyper::Body> => Response<Vec<u8>>
    let (resp_parts, resp_body) = response.into_parts();
    let vec_body: Vec<u8> = hyper::body::to_bytes(resp_body).await?.to_vec();

    Ok(http::Response::from_parts(resp_parts, vec_body))
}

// Get a bearer token from our ServiceAccount (potentially performing an Oauth dance via HTTP)
async fn get_token() -> Result<Option<String>, HttpError> {
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
            let response = do_http_request(request).await?;

            // Parse and unwrap the token.
            provider.parse_token_response(scope_hash, response).unwrap()
        }
        TokenOrRequest::Token(token) => token,
    };

    Ok(Some(token.access_token))
}

// Optionally add the auth header to a request builder.
pub async fn add_auth_header(builder: &mut http::request::Builder) -> Result<(), HttpError> {
    let token = get_token().await?;

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
