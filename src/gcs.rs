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
extern crate serde;
extern crate serde_json;
extern crate serde_with;
extern crate tokio;
extern crate url;

use chrono::{DateTime, Utc};
use std::convert::TryInto;
use url::Url;

use crate::auth::add_auth_header;

use crate::http::{
    do_gcs_request, new_client, request_with_gcs_retry, GcsHttpClient, GCS_DEFAULT_MAX_BACKOFF,
    GCS_DEFAULT_TIMEOUT,
};

use crate::errors::HttpError;

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
    // The docs at
    // https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    // refer to generation and metageneration as "long" represented as
    // a strings.
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub generation: i64,
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub metageneration: i64,
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

#[allow(dead_code)]
// get_bucket isn't used by fs, but I don't want to mark it test only.
async fn get_bucket(bucket_str: &str) -> Result<Bucket, HttpError> {
    debug!("Looking to request: {:#?}", bucket_str);
    let client = new_client();

    let base_url = "https://www.googleapis.com/storage/v1/b";
    let bucket_url = format!("{}/{}", base_url, bucket_str);
    let uri: hyper::Uri = bucket_url.parse()?;

    let mut builder = hyper::Request::builder().uri(uri);
    add_auth_header(&mut builder).await?;

    let body = hyper::Body::empty();
    let request = builder.body(body).expect("Failed to construct request");

    debug!("{:#?}", request);

    let bytes = do_gcs_request(&client, request).await?;
    let bucket: Bucket = serde_json::from_slice(bytes.as_ref()).unwrap();
    debug!("{:#?}", bucket);
    Ok(bucket)
}

#[allow(dead_code)]
// get_object isn't used by fs, but I don't want to mark it test only.
async fn get_object(url: Url) -> Result<Object, HttpError> {
    debug!("Looking to request: {:#?}", url);

    let client = new_client();
    let uri: hyper::Uri = String::from(url).parse()?;
    let body = hyper::Body::empty();

    let mut builder = hyper::Request::builder().uri(uri);
    add_auth_header(&mut builder).await?;

    let request = builder.body(body).expect("Failed to construct request");
    let bytes = do_gcs_request(&client, request).await?;
    let object: Object = serde_json::from_slice(&bytes).unwrap();
    debug!("{:#?}", object);
    Ok(object)
}

#[allow(dead_code)]
// compose_objects isn't (yet) used by fs, but I don't want to mark it test only.
async fn compose_objects_with_client(
    client: &GcsHttpClient,
    src1: &Object,
    src2: &Object,
    bucket: &str,
    name: &str,
) -> Result<Object, HttpError> {
    debug!(
        "Going to compose two objects {:#?} and {:#?} into {} in bucket {}",
        src1, src2, name, bucket
    );

    // https://cloud.google.com/storage/docs/json_api/v1/objects/compose#http-request
    let base_url = "https://storage.googleapis.com/storage/v1/b";
    let full_url = format!(
        "{base_url}/{bucket}/o/{destination_object}/compose",
        base_url = base_url,
        bucket = bucket,
        destination_object = name
    );

    let mut compose_url = Url::parse(&full_url).unwrap();

    let generation_str = format!("{}", src1.generation);
    let metageneration_str = format!("{}", src1.metageneration);

    // If the destination is the same, add an ifGenerationMatch to
    // protect the overwrite. NOTE(boulos): compose is all within a
    // single bucket, so we don't have to validate that here (GCS will
    // do it for us).
    if src1.name == name {
        compose_url
            .query_pairs_mut()
            .append_pair("ifGenerationMatch", &generation_str);
        compose_url
            .query_pairs_mut()
            .append_pair("ifMetagenerationMatch", &metageneration_str);
    }

    // Make a Compose Request
    // (https://cloud.google.com/storage/docs/json_api/v1/objects/compose#request-body). The
    // destination Object just needs the "name" field.
    let req_json = serde_json::json!({
    "kind": "storage#composeRequest",
    "destination": {
        "name": name
    },
    "sourceObjects": [
        {
        "name": src1.name,
        "generation": src1.generation,
        "objectPreconditions": {
            "ifGenerationMatch": src1.generation,
            // TODO(boulos): Can these also take metageneration?
        }
        },
        {
        "name": src2.name,
        "generation": src2.generation,
        "objectPreconditions": {
            "ifGenerationMatch": src2.generation,
        }
        }
    ]
    });

    let uri: hyper::Uri = String::from(compose_url).parse()?;

    let mut builder = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header(http::header::CONTENT_TYPE, "application/json");

    add_auth_header(&mut builder).await?;

    let request = builder
        .body(hyper::Body::from(req_json.to_string()))
        .expect("Failed to construct compose request");

    debug!("{:#?}", request);

    let bytes = do_gcs_request(client, request).await?;

    let compose_response: Object = serde_json::from_slice(&bytes).unwrap();
    debug!("{:#?}", compose_response);

    Ok(compose_response)
}

#[allow(dead_code)]
// get_bytes isn't used by fs, but I don't want to mark it test only.
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

    if how_many == 0 {
        debug!("how_many = 0. You must ask for at least one byte");
        return Err(HttpError::Body);
    }

    // TODO(boulos): Should we add logic here to test that offset +
    // how_many <= obj.size? It's *likely* a client error, but who are
    // we to decide? (Note: obj.size shouldn't be out of date, because
    // we also enforce generation/meta-generation match, but we want
    // to make that optional). Leaving it free allows clients to
    // "overfetch" and let GCS decide and return the short read, or an
    // error if offset > obj.size.

    // Use the self_link from the object as the url, but add ?alt=media
    let mut object_url = Url::parse(&obj.self_link).unwrap();
    let byte_range = format!("bytes={}-{}", offset, offset + how_many - 1);
    // Make sure we're getting the data from the version we intend.
    // TODO(boulos): Allow people to read stale data (generation=) if
    // they prefer, rather than require that the latest version is up
    // to date.
    let generation_str = format!("{}", obj.generation);
    let metageneration_str = format!("{}", obj.metageneration);

    object_url.query_pairs_mut().append_pair("alt", "media");
    object_url
        .query_pairs_mut()
        .append_pair("ifGenerationMatch", &generation_str);
    object_url
        .query_pairs_mut()
        .append_pair("ifMetagenerationMatch", &metageneration_str);

    let now = std::time::Instant::now();

    let uri: hyper::Uri = String::from(object_url).parse()?;

    let body = hyper::Body::empty();

    let mut builder = http::Request::builder()
        .uri(uri)
        // NOTE(boulos): RANGE *not* CONTENT-RANGE.
        // https://cloud.google.com/storage/docs/xml-api/reference-headers#range
        .header(http::header::RANGE, byte_range);

    add_auth_header(&mut builder).await?;

    let request = builder.body(body).expect("Failed to construct request");
    debug!("Performing range request {:#?}", request);

    // Do the request (reliably) but bail on error.
    let written = do_gcs_request(&client, request).await?;

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

    let mut builder = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(upload_url)
        .header(http::header::CONTENT_TYPE, "application/json");

    add_auth_header(&mut builder).await?;

    let request = builder
        .body(hyper::Body::from(object_json.to_string()))
        .expect("Failed to construct upload request");

    debug!("{:#?}", request);

    let response = request_with_gcs_retry(
        &client,
        request,
        GCS_DEFAULT_MAX_BACKOFF,
        GCS_DEFAULT_TIMEOUT,
    )
    .await?;
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
    if data.is_empty() && !finalize {
        error!("Empty data for non-finalize");
        return Err(HttpError::Body);
    }

    if data.len() % (256 * 1024) != 0 && !finalize {
        error!(
            "Asked to append {} bytes which isn't a multiple of 256 KiB",
            data.len()
        );
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

    let upload_url: hyper::Uri = session_uri.parse()?;
    // Hopefully this works with empty bodies.
    let body = hyper::body::Bytes::copy_from_slice(data);
    let chunked: Vec<Result<_, std::io::Error>> = vec![Ok(body)];
    let chunk_stream = futures::stream::iter(chunked);
    let body = hyper::Body::wrap_stream(chunk_stream);

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(upload_url)
        .header(http::header::CONTENT_RANGE, byte_range)
        .header(
            http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(body)
        .expect("Failed to construct upload request");

    debug!("{:#?}", request);

    let response = do_gcs_request(&client, request).await;

    if !finalize {
        // Check that our upload worked. We're actually looking for a
        // 308 "Resume Incomplete" error.
        if !response.is_err() {
            debug!("Unexpected Ok() from multi-part upload!");
            return Err(HttpError::UploadFailed);
        }

        let err = response.unwrap_err();

        match err {
            HttpError::Status(status) => {
                if status.as_u16() == 308 {
                    // We got a 308! That's "success" for us.
                    // TODO(boulos): We need to check that the Range
                    // header response says we uploaded all our
                    // bytes. But we should do that inside of
                    // do_gcs_request (somehow) or just use
                    // request_with_gcs_retry directly here as well.
                    return Ok(None);
                }
                return Err(err);
            }
            _ => {
                // Anything else is also an error.
                return Err(err);
            }
        };
    }

    // This was our final segment. The response should be an Object.
    let bytes = response?;
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

    let remaining = if !cursor.buffer.is_empty() {
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
            full_chunks,
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

// Do a single list object request (i.e., don't follow the next page token).
async fn _do_one_list_object(
    client: &GcsHttpClient,
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

    let uri: hyper::Uri = String::from(list_url).parse()?;

    let mut builder = http::Request::builder().uri(uri);
    add_auth_header(&mut builder).await?;

    let body = hyper::Body::empty();
    let request = builder
        .body(body)
        .expect("Failed to construct list request");

    debug!("{:#?}", request);
    let bytes = do_gcs_request(client, request).await?;
    let list_response: ListObjectsResponse = serde_json::from_slice(&bytes).unwrap();
    debug!("{:#?}", list_response);

    Ok(list_response)
}

// Perform a list objects request, including following any
// nextPageToken responses, so that we get the full set of Objects and
// list of Prefixes (as a Vec<String>).
pub async fn list_objects(
    client: &GcsHttpClient,
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
            true => _do_one_list_object(client, bucket, prefix, delim, None).await?,
            false => _do_one_list_object(client, bucket, prefix, delim, Some(&page_token)).await?,
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
    }

    Ok((objects, prefixes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    extern crate env_logger;

    const LANDSAT_BUCKET: &str = "gcp-public-data-landsat";
    const LANDSAT_PREFIX: &str = "LC08/01/044/034/";
    const LANDSAT_SUBDIR: &str = "LC08_L1GT_044034_20130330_20170310_01_T2";
    const LANDSAT_B7_TIF: &str = "LC08_L1GT_044034_20130330_20170310_01_T2_B7.TIF";
    const LANDSAT_B7_MTL: &str = "LC08_L1GT_044034_20130330_20170310_01_T2_MTL.txt";

    fn init() {
        // https://docs.rs/env_logger/0.8.2/env_logger/index.html#capturing-logs-in-tests
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn test_bucket() -> String {
        std::env::var("GCSFUSER_TEST_BUCKET").expect("You must provide a read/write bucket")
    }

    fn landsat_obj_url(object_str: &str) -> Url {
        let mut object_url = Url::parse("https://www.googleapis.com/storage/v1/b").unwrap();
        // Each *push* url encodeds the argument and then also adds a
        // *real* slash *before* appending.
        object_url
            .path_segments_mut()
            .unwrap()
            .push(LANDSAT_BUCKET)
            .push("o")
            .push(object_str);

        object_url
    }

    fn landsat_big_obj_url() -> Url {
        // Make the full object "name".
        let object_str = format!("{}{}/{}", LANDSAT_PREFIX, LANDSAT_SUBDIR, LANDSAT_B7_TIF);

        landsat_obj_url(&object_str)
    }
    fn landsat_small_obj_url() -> Url {
        // Make the full object "name".
        let object_str = format!("{}{}/{}", LANDSAT_PREFIX, LANDSAT_SUBDIR, LANDSAT_B7_MTL);

        landsat_obj_url(&object_str)
    }

    async fn put_object(bucket: &str, name: &str, media: &[u8]) -> Result<Object, HttpError> {
        // Make a client.
        let client = &new_client();
        // Then start the object.
        let mut object_cursor = create_object_with_client(client, bucket, name)
            .await
            .unwrap();
        // Send the bytes.
        append_bytes_with_client(client, &mut object_cursor, media)
            .await
            .unwrap();
        // Then finalize and get our Result.
        finalize_upload_with_client(client, &mut object_cursor).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_landsat_bucket() {
        init();

        let bucket = get_bucket(LANDSAT_BUCKET).await.unwrap();
        println!("Got back bucket {:#?}", bucket)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_private_bucket() {
        init();

        let private_bucket = test_bucket();
        let bucket = get_bucket(&private_bucket).await.unwrap();
        println!("Got back bucket {:#?}", bucket)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_private_object() {
        init();

        // define the private object
        let private_bucket = test_bucket();
        let want_name = "get_private_object.txt";

        // set up test data
        let want_len: u64 = 1234;
        let want_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(want_len as usize)
            .map(char::from)
            .collect();
        let want_bytes = want_string.into_bytes();
        let _ = put_object(&private_bucket, &want_name, &want_bytes.to_owned()).await;

        // test getting the private object
        let url = format!(
            "https://www.googleapis.com/storage/v1/b/{}/o/{}",
            private_bucket, want_name
        );

        // get object metadata
        let object_url = Url::parse(&url).unwrap();
        let object = get_object(object_url).await.unwrap();
        assert_eq!(object.size, want_len);
        assert_eq!(object.name, want_name);

        // get whole object
        let got_bytes: Vec<u8> = get_bytes(&object, 0, want_len).await.unwrap();
        let got_len = got_bytes.len() as u64;
        assert_eq!(got_len, want_len);
        assert_eq!(got_bytes, want_bytes);

        // get partial object
        let start_offset: u64 = 6;
        let want_bytes = &want_bytes[start_offset as usize..];
        let want_len = want_len - start_offset;
        let got_bytes: Vec<u8> = get_bytes(&object, start_offset, want_len).await.unwrap();
        let got_len = got_bytes.len() as u64;
        assert_eq!(got_len, want_len);
        assert_eq!(got_bytes, want_bytes);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_public_object() {
        init();

        let object_url = landsat_small_obj_url();
        println!("Going to request object {}", object_url.as_str());

        let object: Object = get_object(object_url).await.unwrap();
        println!("Object has {} bytes", object.size);

        println!("Object debug is {:#?}", object);

        let bytes: Vec<u8> = get_bytes(&object, 0, 4096).await.unwrap();
        println!("Got back:\n {}", String::from_utf8(bytes).unwrap());

        let offset_bytes: Vec<u8> = get_bytes(&object, 4099, 1024).await.unwrap();
        println!("Got back:\n {}", String::from_utf8(offset_bytes).unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_object_invalid() {
        init();

        let object_url = landsat_small_obj_url();
        let object: Object = get_object(object_url).await.unwrap();
        println!("Object has {} bytes", object.size);

        println!("Object debug is {:#?}", object);

        let bytes: Vec<u8> = get_bytes(&object, 0, 4096).await.unwrap();
        println!("Got back:\n {}", String::from_utf8(bytes).unwrap());

        // Now, make a copy of that object and change the self link.
        let mut modified: Object = object;
        // Take the last character off.
        modified.self_link.pop();

        let expect_404 = get_bytes(&modified, 0, 4096).await;
        assert_eq!(expect_404.is_err(), true);
        // I cannot figure out how to make this work. But I wish I could.
        //assert_eq!(expect_404.unwrap_err(),
        //	   HttpError::Status(http::StatusCode::NOT_FOUND));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_public_bytes_bad_range() {
        init();

        let object_url = landsat_small_obj_url();
        let object: Object = get_object(object_url).await.unwrap();

        // If we don't ask for any bytes, we get an error.
        let result = get_bytes(&object, 0, 0).await;
        assert!(result.is_err());

        // Changing the offset but no bytes, still gets an error.
        let result = get_bytes(&object, 100, 0).await;
        assert!(result.is_err());

        // But *overfetching* (the small obj is 8454 bytes), we shouldn't get an error.
        let result = get_bytes(&object, 0, 10000).await;
        assert!(result.is_ok());

        // Trying to *start* past the end => 416 "Range Not Satisifiable".
        let result = get_bytes(&object, 10000, 1).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_public_bytes_large_read() {
        init();

        let client = new_client();

        let object_url = landsat_big_obj_url();
        println!("Going to request obj (url) {}", object_url);

        let object: Object = get_object(object_url).await.unwrap();
        println!("Object has {} bytes", object.size);

        assert!(
            object.size > 1024 * 1024,
            "Object must be at least 1MB in size!"
        );

        // Issue a 1MB read. TODO(boulos): Ensure that we're doing it
        // in "one shot" (we aren't currently! hyper is breaking it up
        // into 16 KiB reads).
        let bytes: Vec<u8> = get_bytes_with_client(&client, &object, 0, 1024 * 1024)
            .await
            .unwrap();

        // Make sure we got back the entire 1MB read.
        assert_eq!(bytes.len(), 1024 * 1024);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_private_object() {
        init();

        let client = new_client();

        let bucket_str = test_bucket();

        let filename = "write_private_obj.txt";

        // Get us a handle to a resumable upload.
        let mut cursor = create_object_with_client(&client, &bucket_str, filename)
            .await
            .unwrap();

        let bytes = "Hello, GCS!";

        let written = append_bytes_with_client(&client, &mut cursor, bytes.as_bytes())
            .await
            .unwrap();
        // Make sure we get back the right length.
        assert_eq!(written, bytes.len());
        // Now finalize
        let result = finalize_upload_with_client(&client, &mut cursor).await;

        match result {
            Ok(obj) => println!(
                "Obj has size {} and generation {}",
                obj.size, obj.generation
            ),
            Err(e) => panic!("Got error {:#?}", e),
        };
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_object_chunks() {
        init();

        let client = new_client();

        let bucket_str = test_bucket();

        let filename = "write_chunk_obj.txt";

        // Get us a handle to a resumable upload.
        let mut cursor = create_object_with_client(&client, &bucket_str, filename)
            .await
            .unwrap();

        let lengths = vec![
            20,
            350 * 1024,
            512 * 1024 - (350 * 1024 - 20),
            1024 * 1024,
            384 * 1024,
        ];
        let total_length: u64 = lengths.iter().sum();

        for length in lengths.iter() {
            // Make ascii text
            let bytes: Vec<u8> = (0..*length).map(|x| (48 + (x % 10)) as u8).collect();
            let written = append_bytes_with_client(&client, &mut cursor, &bytes)
                .await
                .unwrap();
            // Make sure we get back the right length.
            assert_eq!(written, bytes.len());
        }

        // Now finalize
        let obj = finalize_upload_with_client(&client, &mut cursor)
            .await
            .expect("Expected an object!");

        // Check that the length is the same as all our appends.
        assert_eq!(obj.size, total_length);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_object_race() {
        init();

        let client = new_client();

        let bucket_str = test_bucket();

        let filename = "write_object_race.txt";

        let original = "Original value";
        let new_value = "New values";

        let original_obj = {
            // Write out the original value to our file.
            let mut cursor = create_object_with_client(&client, &bucket_str, filename)
                .await
                .unwrap();
            let written = append_bytes_with_client(&client, &mut cursor, original.as_bytes())
                .await
                .unwrap();
            // Make sure we get back the right length.
            assert_eq!(written, original.len());
            // Now finalize
            let result = finalize_upload_with_client(&client, &mut cursor).await;

            match result {
                Ok(obj) => obj,
                Err(_) => panic!("Expected to get back an object..."),
            }
        };

        println!("Wrote {} with object {:#?}", filename, original_obj);

        let new_obj = {
            // Now, write over the object again with the new data.
            let mut cursor = create_object_with_client(&client, &bucket_str, filename)
                .await
                .unwrap();

            let written = append_bytes_with_client(&client, &mut cursor, new_value.as_bytes())
                .await
                .unwrap();
            // Make sure we get back the right length.
            assert_eq!(written, new_value.len());
            // And finalize
            let result = finalize_upload_with_client(&client, &mut cursor).await;

            match result {
                Ok(obj) => obj,
                Err(_) => panic!("Expected to get back an object..."),
            }
        };

        println!("Overwrote {} with object {:#?}", filename, new_obj);

        // Now, if we try to read the original one, it's gone.
        let read_orig = get_bytes_with_client(&client, &original_obj, 0, original_obj.size).await;

        // We should have gotten some sort of error.
        assert_eq!(read_orig.is_err(), true);

        let read_new = get_bytes_with_client(&client, &new_obj, 0, new_obj.size).await;

        // We shouldn't have gotten an error.
        assert_eq!(read_new.is_err(), false);

        let read_result = read_new.unwrap();

        // Make sure we got the correct bytes out.
        assert_eq!(read_result, new_value.as_bytes());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_list_paginated() {
        init();

        let client = new_client();

        let bucket = LANDSAT_BUCKET;
        let prefix = LANDSAT_PREFIX;
        let delim = "/";

        let (objects, prefixes) = list_objects(&client, bucket, Some(prefix), Some(delim))
            .await
            .unwrap();
        println!("Got {} objects", objects.len());
        println!("prefixes: {:#?}", prefixes);
        println!("objects: {:#?}", objects);

        let (only_top_level, prefixes) = list_objects(&client, bucket, None, Some(delim))
            .await
            .unwrap();
        println!("Got {} objects", only_top_level.len());
        println!("Dump:\n\n{:#?}", only_top_level);
        println!("Prefixes:\n\n{:#?}", prefixes);

        // No delimeter
        let (all_objects, prefixes) = list_objects(&client, bucket, Some(prefix), None)
            .await
            .unwrap();
        println!("Got {} objects", objects.len());
        println!("prefixes: {:#?}", prefixes);
        println!("objects: {:#?}", all_objects);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn basic_compose() {
        init();

        let client = new_client();

        let bucket_str = test_bucket();

        let src1_name = "src1.txt";
        let src2_name = "src2.txt";
        let dst_name = "dst.txt";

        let src1_data = "Hello, ";
        let src2_data = "world!";

        let combo = format!("{}{}", src1_data, src2_data);

        let obj1: Object = put_object(&bucket_str, &src1_name, src1_data.as_bytes())
            .await
            .unwrap();
        let obj2: Object = put_object(&bucket_str, &src2_name, src2_data.as_bytes())
            .await
            .unwrap();

        let dst: Object =
            compose_objects_with_client(&client, &obj1, &obj2, &bucket_str, &dst_name)
                .await
                .unwrap();

        println!("Composed {:#?}", dst);

        // Make sure the size matches.
        assert_eq!(dst.size, combo.len() as u64);

        // Then see if the bytes match.
        let bytes: Vec<u8> = get_bytes(&dst, 0, combo.len() as u64).await.unwrap();
        assert_eq!(bytes, combo.as_bytes());
    }

    // Similar to above, but use compose for append (dst == src1.name).
    #[tokio::test(flavor = "multi_thread")]
    async fn compose_append() {
        init();

        let client = new_client();

        let bucket_str = test_bucket();

        let src1_name = "append.txt";
        let src2_name = "append2.txt";
        let dst_name = src1_name;

        let src1_data = "Hello, ";
        let src2_data = "world!";

        let combo = format!("{}{}", src1_data, src2_data);

        let obj1: Object = put_object(&bucket_str, &src1_name, src1_data.as_bytes())
            .await
            .unwrap();
        let obj2: Object = put_object(&bucket_str, &src2_name, src2_data.as_bytes())
            .await
            .unwrap();

        let dst: Object =
            compose_objects_with_client(&client, &obj1, &obj2, &bucket_str, &dst_name)
                .await
                .unwrap();

        println!("Composed {:#?}", dst);

        // Make sure the size matches.
        assert_eq!(dst.size, combo.len() as u64);

        // Then see if the bytes match.
        let bytes: Vec<u8> = get_bytes(&dst, 0, combo.len() as u64).await.unwrap();
        assert_eq!(bytes, combo.as_bytes());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compose_cross_bucket_fails() {
        init();

        let client = new_client();
        let bucket_str = test_bucket();

        // Get the small landsat object
        let input: Object = get_object(landsat_small_obj_url()).await.unwrap();

        // And then try to concat it into out bucket.
        let dst_name = "compose_cross.txt";

        let dst =
            compose_objects_with_client(&client, &input, &input, &bucket_str, &dst_name).await;

        // We should have gotten an error.
        assert_eq!(dst.is_err(), true);
    }
}
