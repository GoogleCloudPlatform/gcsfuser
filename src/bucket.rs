extern crate http;
extern crate hyper;
extern crate hyper_rustls;
extern crate reqwest;
extern crate serde;
extern crate serde_json;
extern crate serde_with;
extern crate tokio;
extern crate url;

use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use lazy_static::lazy_static;
use std::convert::TryInto;
use tame_oauth::gcp::prelude::*;
use url::Url;

pub type GcsHttpClient =
    hyper::client::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    id: String,
    pub name: String,
    pub location: String,
    self_link: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Object {
    id: String,
    pub name: String,
    pub bucket: String,
    self_link: String,
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub size: u64,
    pub time_created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ListObjectsResponse {
    next_page_token: Option<String>,
    prefixes: Option<Vec<String>>,
    items: Option<Vec<Object>>,
}

pub struct ResumableUploadCursor {
    pub name: String,
    pub bucket: String,
    // The session URI
    pub session_uri: String,
    // The amount written so far.
    pub offset: u64,
    // We need to have a buffer to build up writes in multiples of 256 KiB. Sigh.
    pub buffer: Vec<u8>,
}

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

pub fn new_client() -> GcsHttpClient {
    // NOTE(boulos): Previously, I was passing some default headers
    // here. Now that bearer_auth is per request, this is less needed,
    // but we can change that later.
    let https = hyper_rustls::HttpsConnector::with_native_roots();
    hyper::Client::builder().build::<_, hyper::Body>(https)
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

async fn do_async_request(
    client: GcsHttpClient,
    request: hyper::Request<hyper::Body>,
) -> Result<hyper::body::Bytes, HttpError> {
    let response = client.request(request).await.unwrap();
    debug!("{:#?}", response);
    Ok(hyper::body::to_bytes(response).await.unwrap())
}

async fn get_bucket(bucket_str: &str) -> Result<Bucket, HttpError> {
    debug!("Looking to request: {:#?}", bucket_str);
    let client = new_client();
    let token = get_token()?;

    let base_url = "https://www.googleapis.com/storage/v1/b";
    let bucket_url = format!("{}/{}", base_url, bucket_str);
    let uri: hyper::Uri = bucket_url.parse()?;

    let body = hyper::Body::default();

    let request = hyper::Request::builder()
        .uri(uri)
        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
        .body(body)
        .expect("Failed to construct request");

    debug!("{:#?}", request);

    let bytes = do_async_request(client, request).await.unwrap();

    let bucket: Bucket = serde_json::from_slice(&bytes).unwrap();
    debug!("{:#?}", bucket);
    Ok(bucket)
}

async fn get_object(url: Url) -> Result<Object, HttpError> {
    debug!("Looking to request: {:#?}", url);

    let client = new_client();
    let token = get_token()?;
    let uri: hyper::Uri = url.into_string().parse()?;

    let body = hyper::Body::default();

    let request = hyper::Request::builder()
        .uri(uri)
        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
        .body(body)
        .expect("Failed to construct request");

    let response = client.request(request).await.unwrap();
    debug!("{:#?}", response);

    let bytes = hyper::body::to_bytes(response).await.unwrap();

    let object: Object = serde_json::from_slice(&bytes).unwrap();
    debug!("{:#?}", object);
    Ok(object)
}

async fn get_bytes(obj: &Object, offset: u64, how_many: u64) -> Result<Vec<u8>, HttpError> {
    let client = new_client();
    return get_bytes_with_client(&client, obj, offset, how_many).await;
}

pub async fn get_bytes_with_client(
    client: &GcsHttpClient,
    obj: &Object,
    offset: u64,
    how_many: u64,
) -> Result<Vec<u8>, HttpError> {
    debug!(
        "Asking for {} bytes at {} from the origin for {} (self link = {}",
        how_many, offset, obj.name, obj.self_link
    );

    // Use the self_link from the object as the url, but add ?alt=media
    let mut object_url = Url::parse(&obj.self_link).unwrap();
    object_url.query_pairs_mut().append_pair("alt", "media");

    let byte_range = format!("bytes={}-{}", offset, offset + how_many - 1);

    let now = std::time::Instant::now();

    let token = get_token()?;

    let uri: hyper::Uri = object_url.into_string().parse()?;

    let body = hyper::Body::default();

    let request = http::Request::builder()
        .uri(uri)
        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
        // NOTE(boulos): RANGE *not* CONTENT-RANGE.
        // https://cloud.google.com/storage/docs/xml-api/reference-headers#range
        .header(http::header::RANGE, byte_range)
        .body(body)
        .expect("Failed to construct request");
    debug!("Performing range request {:#?}", request);

    let response = client.request(request).await.unwrap();

    let written = hyper::body::to_bytes(response).await.unwrap();
    debug!(
        "Got back {} bytes. Took {:#?}",
        written.len(),
        now.elapsed()
    );

    // Range requests *can* be ignored and given a 200 "here's the
    // whole thing". If we got back more bytes than expected, trim.
    if written.len() > how_many.try_into().unwrap() {
        let start: usize = offset.try_into().unwrap();
        let how_many_usize: usize = how_many.try_into().unwrap();
        let end: usize = start + how_many_usize - 1;
        Ok(written.slice(start..end).to_vec())
    } else {
        Ok(written.to_vec())
    }
}

pub async fn create_object_with_client(
    client: &GcsHttpClient,
    bucket: &str,
    name: &str,
) -> Result<ResumableUploadCursor, HttpError> {
    debug!(
        "Going to start a resumable upload for object {} in bucket {}",
        name, bucket
    );

    let token = get_token()?;

    let base_url = "https://storage.googleapis.com/upload/storage/v1/b";
    let full_url = format!(
        "{base_url}/{bucket}/o?uploadType=resumable",
        base_url = base_url,
        bucket = bucket
    );

    let upload_url: hyper::Uri = full_url.parse()?;
    // For uploads, you just need the object name as JSON.
    let object_json = serde_json::json!({
    "name": name,
    });

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(upload_url)
        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(hyper::Body::from(object_json.to_string()))
        .expect("Failed to construct upload request");

    debug!("{:#?}", request);

    let response = client.request(request).await.unwrap();
    debug!("{:#?}", response);

    if response.status() != hyper::StatusCode::OK {
        return Err(HttpError::Status(response.status()));
    }

    if !response.headers().contains_key(hyper::header::LOCATION) {
        debug!("Didn't get back a LOCATION header!");
        return Err(HttpError::UploadFailed);
    }

    let session_uri = response.headers().get(hyper::header::LOCATION).unwrap();
    debug!("Got resumable upload URI {:#?}", session_uri);

    Ok(ResumableUploadCursor {
        name: name.to_string(),
        bucket: bucket.to_string(),
        session_uri: session_uri.to_str().unwrap().to_string(),
        offset: 0,
        // Our 256 KiB buffer.
        buffer: Vec::with_capacity(256 * 1024),
    })
}

async fn _do_resumable_upload(
    client: &GcsHttpClient,
    session_uri: &str,
    offset: u64,
    data: &[u8],
    finalize: bool,
) -> Result<Option<Object>, HttpError> {
    if data.len() == 0 && !finalize {
        error!("Empty data for non-finalize");
        return Err(HttpError::Body);
    }

    let last_byte = match data.len() {
        0 => offset,
        _ => offset + (data.len() as u64) - 1,
    };

    let object_size = if finalize {
        format!("{}", offset + data.len() as u64)
    } else {
        String::from("*")
    };
    // NOTE(boulos): *CONTENT_RANGE* has the format bytes X-Y/Z. While RANGE is bytes=X-Y.
    let byte_range = format!("bytes {}-{}/{}", offset, last_byte, object_size);
    let verb = if finalize { "finalize" } else { "issue" };
    debug!(
        "Going to {} resumable upload with range {}",
        verb, byte_range
    );

    let token = get_token()?;
    let upload_url: hyper::Uri = session_uri.parse()?;
    // Hopefully this works with empty bodies.
    let body = hyper::body::Bytes::copy_from_slice(data);
    let chunked: Vec<Result<_, std::io::Error>> = vec![Ok(body)];
    let chunk_stream = stream::iter(chunked);
    let body = hyper::Body::wrap_stream(chunk_stream);

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(upload_url)
        //.header(http::header::AUTHORIZATION, format!("Bearer {}", token))
        .header(http::header::CONTENT_RANGE, byte_range)
        //.header(http::header::TRANSFER_ENCODING, "chunked")
        .header(
            http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(body)
        .expect("Failed to construct upload request");

    debug!("{:#?}", request);

    let response = client.request(request).await.unwrap();
    debug!("{:#?}", response);

    if !finalize {
        // Check that our upload worked. Google uses 308 (Permanent
        // Redirect) as "Resume Incomplete"
        if response.status().as_u16() != 308 {
            return Err(HttpError::Status(response.status()));
        }

        // TODO(boulos): Check the range output in case our bytes are missing.

        // Worked out! But we don't have an Object, yet.
        return Ok(None);
    }

    // Let's deal with finalizing. We get either a 200 or 201.
    if !response.status().is_success() {
        let result = HttpError::Status(response.status());
        let bytes = hyper::body::to_bytes(response).await.unwrap();
        debug!("error bytes {:#?}", bytes);

        return Err(result);
    }

    let bytes = hyper::body::to_bytes(response).await.unwrap();
    debug!("response bytes {:#?}", bytes);

    let object: Object = serde_json::from_slice(&bytes).unwrap();
    debug!("{:#?}", object);
    Ok(Some(object))
}

pub async fn append_bytes_with_client(
    client: &GcsHttpClient,
    cursor: &mut ResumableUploadCursor,
    data: &[u8],
) -> Result<usize, HttpError> {
    debug!("Asking to append {} bytes to our cursor", data.len());

    let buffer_remaining = cursor.buffer.capacity() - cursor.buffer.len();
    if data.len() <= buffer_remaining {
        // Just append.
        cursor.buffer.extend_from_slice(data);
        return Ok(data.len());
    }

    let remaining = if cursor.buffer.len() > 0 {
        // First fill up the buffer.
        let (left, right) = data.split_at(buffer_remaining);
        cursor.buffer.extend_from_slice(left);

        debug!("Flushing the buffer of size {} to GCS", cursor.buffer.len());
        let flush = _do_resumable_upload(
            client,
            &cursor.session_uri,
            cursor.offset,
            cursor.buffer.as_slice(),
            false, /* not finalizing */
        )
        .await;

        match flush {
            Err(e) => return Err(e),
            _ => debug!("Flush succeeded!"),
        }

        // Move the offset forward and clear our buffer.
        cursor.offset += cursor.buffer.len() as u64;
        cursor.buffer.clear();

        right
    } else {
        // Don't do anything, so we can catch the full chunks without buffering.
        data
    };

    // The current buffer is empty and we might have several chunks we can
    // ship without buffering.
    let num_chunks: usize = remaining.len() / (256 * 1024);
    let chunked_bytes = 256 * 1024 * num_chunks;
    let (full_chunks, final_append) = remaining.split_at(chunked_bytes);

    if num_chunks > 0 {
        // Write out the full chunks in one shot.
        debug!(
            "Shipping {} full chunks ({} total bytes)",
            num_chunks, chunked_bytes
        );
        let flush = _do_resumable_upload(
            client,
            &cursor.session_uri,
            cursor.offset,
            remaining,
            false, /* not finalizing */
        )
        .await;
        match flush {
            Err(e) => return Err(e),
            _ => debug!("Flushing full chunks succeeded!"),
        }

        cursor.offset += chunked_bytes as u64;
    }

    // Push whatever is left over (if any) into our buffer.
    cursor.buffer.extend_from_slice(final_append);
    Ok(data.len())
}

pub async fn finalize_upload_with_client(
    client: &GcsHttpClient,
    cursor: &mut ResumableUploadCursor,
) -> Result<Object, HttpError> {
    debug!(
        "Finializing our object. {} bytes left!",
        cursor.buffer.len()
    );

    let result = _do_resumable_upload(
        client,
        &cursor.session_uri,
        cursor.offset,
        cursor.buffer.as_slice(),
        true, /* We're the last ones! */
    )
    .await;
    // Clear our buffer, even if we had an error.
    cursor.buffer.clear();
    // Pop up the result.
    if result.is_err() {
        return Err(result.err().unwrap());
    }

    let obj: Object = result.unwrap().unwrap();
    Ok(obj)
}

fn _do_one_list_object(
    bucket: &str,
    prefix: Option<&str>,
    delim: Option<&str>,
    token: Option<&str>,
) -> Result<ListObjectsResponse, HttpError> {
    let base_url = "https://www.googleapis.com/storage/v1/b";
    let bucket_url = format!("{}/{}/o", base_url, bucket);

    let mut list_url = Url::parse(&bucket_url).unwrap();

    if let Some(prefix_str) = prefix {
        list_url.query_pairs_mut().append_pair("prefix", prefix_str);
    }

    if let Some(delim_str) = delim {
        list_url
            .query_pairs_mut()
            .append_pair("delimiter", delim_str);
    }

    if let Some(token_str) = token {
        list_url
            .query_pairs_mut()
            .append_pair("pageToken", token_str);
    }

    let op = || -> Result<reqwest::blocking::Response, HttpError> {
        let client = reqwest::blocking::Client::new();
        let token = get_token()?;

        let response = client
            .get(list_url)
            .bearer_auth(token)
            .send()
            .expect("Failed to send request");

        if response.status().is_client_error() {
            debug!("Got back {:#?}", response.status());
        }

        debug!("  List obj response is {:#?}", response);

        Ok(response)
    };

    // TODO(boulos): Do jittered exponential backoff
    let response = op()?;

    //debug!(" ListObject body => {:#?}", response.text()?);

    let list_response = response.json::<ListObjectsResponse>();
    return list_response.map_err(HttpError::Transport);
}

pub fn list_objects(
    bucket: &str,
    prefix: Option<&str>,
    delim: Option<&str>,
) -> Result<(Vec<Object>, Vec<String>), HttpError> {
    debug!(
        "Asking for a list from bucket '{}' with prefix '{:#?}' and delim = '{:#?}'",
        bucket, prefix, delim
    );

    let mut objects: Vec<Object> = vec![];
    let mut prefixes: Vec<String> = vec![];

    let mut page_token: String = String::from("");

    loop {
        let resp = match page_token.is_empty() {
            true => _do_one_list_object(bucket, prefix, delim, None)?,
            false => _do_one_list_object(bucket, prefix, delim, Some(&page_token))?,
        };

        if resp.items.is_some() {
            objects.append(&mut resp.items.unwrap());
        }

        if resp.prefixes.is_some() {
            prefixes.append(&mut resp.prefixes.unwrap());
        }

        match resp.next_page_token {
            Some(temp_token_str) => page_token = temp_token_str.clone(),
            None => break,
        }

        // Sleep a bit between requests
        println!("  Sleeping for 10 ms, to avoid dos block");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    Ok((objects, prefixes))
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate env_logger;
    fn init() {
        // https://docs.rs/env_logger/0.8.2/env_logger/index.html#capturing-logs-in-tests
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_landsat() {
        init();

        let landsat = "gcp-public-data-landsat";
        let bucket = get_bucket(landsat).await.unwrap();
        println!("Got back bucket {:#?}", bucket)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_private_bucket() {
        init();

        let private_bucket = "boulos-vm-ml";
        let bucket = get_bucket(private_bucket).await.unwrap();
        println!("Got back bucket {:#?}", bucket)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_private_object() {
        init();

        let object_url = Url::parse(
            "https://www.googleapis.com/storage/v1/b/boulos-hadoop/o/bdutil-staging%2fhadoop-m%2f20150202-172447-58j%2fbq-mapred-template.xml"
        ).unwrap();
        let object: Object = get_object(object_url).await.unwrap();
        println!("Object has {} bytes", object.size);

        let bytes: Vec<u8> = get_bytes(&object, 0, 769).await.unwrap();
        println!("Got back:\n {}", String::from_utf8(bytes).unwrap());

        let offset_bytes: Vec<u8> = get_bytes(&object, 6, 769).await.unwrap();
        println!("Got back:\n {}", String::from_utf8(offset_bytes).unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_public_object() {
        init();

        let object_url = Url::parse(
            "https://www.googleapis.com/storage/v1/b/gcp-public-data-landsat/o/LC08%2FPRE%2F044%2F034%2FLC80440342017101LGN00%2FLC80440342017101LGN00_MTL.txt"
        ).unwrap();
        let object: Object = get_object(object_url).await.unwrap();
        println!("Object has {} bytes", object.size);

        println!("Object debug is {:#?}", object);

        let bytes: Vec<u8> = get_bytes(&object, 0, 4096).await.unwrap();
        println!("Got back:\n {}", String::from_utf8(bytes).unwrap());

        let offset_bytes: Vec<u8> = get_bytes(&object, 4099, 1024).await.unwrap();
        println!("Got back:\n {}", String::from_utf8(offset_bytes).unwrap());
    }

    #[test]
    fn test_list_objects() {
        init();

        let bucket = "boulos-hadoop";
        let prefix = "bdutil-staging";
        let delim = "/";

        let (objects, _) = list_objects(bucket, Some(prefix), Some(delim)).unwrap();
        println!("Got {} objects", objects.len());
        println!("Dump:\n\n{:#?}", objects);

        let (all_objects, _) = list_objects(bucket, None, None).unwrap();
        println!("Got {} objects", all_objects.len());
        println!("Dump:\n\n{:#?}", all_objects);

        let (only_top_level, prefixes) = list_objects(bucket, None, Some(delim)).unwrap();
        println!("Got {} objects", only_top_level.len());
        println!("Dump:\n\n{:#?}", only_top_level);
        println!("Prefixes:\n\n{:#?}", prefixes);
    }

    #[test]
    fn test_list_paginated() {
        init();

        let bucket = "gcp-public-data-landsat";
        let prefix = "LC08/PRE/044/034/";
        let delim = "/";

        let (objects, prefixes) = list_objects(bucket, Some(prefix), Some(delim)).unwrap();
        println!("Got {} objects", objects.len());
        println!("prefixes: {:#?}", prefixes);
        println!("objects: {:#?}", objects)
    }
}
