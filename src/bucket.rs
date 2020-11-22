extern crate reqwest;
extern crate serde;
extern crate serde_json;
extern crate serde_with;
extern crate url;

use url::Url;
use lazy_static::lazy_static;
use tame_oauth::gcp::prelude::*;
use chrono::{DateTime, Utc};


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
    pub updated: DateTime<Utc>
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ListObjectsResponse {
    next_page_token: Option<String>,
    prefixes: Option<Vec<String>>,
    items: Option<Vec<Object>>,
}

// Add a simple union type of "Either reqwest::Error" (our Transport)
// or generic Http errors.
#[derive(Debug)]
pub enum HttpError {
    Transport(reqwest::Error),
    Generic(http::Error),
}

impl From<reqwest::Error> for HttpError {
    fn from(err: reqwest::Error) -> Self {
	HttpError::Transport(err)
    }
}

impl From<http::Error> for HttpError {
    fn from(err: http::Error) -> Self {
	HttpError::Generic(err)
    }
}


lazy_static!{
    static ref TOKEN_PROVIDER: ServiceAccountAccess = {
	// Read in the usual key file.
	let env_key = "GOOGLE_APPLICATION_CREDENTIALS";
	let cred_path = std::env::var(env_key).expect("You must set GOOGLE_APPLICATION_CREDENTIALS environment variable");
	let key_data = std::fs::read_to_string(cred_path).expect("failed to read credential file");
	let acct_info = ServiceAccountInfo::deserialize(key_data).expect("failed to decode credential file");

	ServiceAccountAccess::new(acct_info).expect("failed to create OAuth Token Provider")
    };
}

pub fn new_client() -> Result<reqwest::blocking::Client, reqwest::Error> {
    // NOTE(boulos): Previously, I was passing some default headers
    // here. Now that bearer_auth is per request, this is less needed,
    // but we can change that later.
    return reqwest::blocking::Client::builder()
        .build();
}


// Given an http::Request, actually issue it via reqwest, returning
// the http::Response.
pub fn do_http_via_reqwest(req: http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>, HttpError> {
    let (parts, body) = req.into_parts();

    assert_eq!(parts.method, http::Method::POST);

    let client = new_client()?;

    let uri = parts.uri.to_string();

    // Go from http::Request => a POST via reqwest
    let response = client.post(&uri)
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
    resp_headers.extend(response.headers().into_iter()
			.map(|(k, v)| (k.clone(), v.clone())));

    // NOTE(boulos): This has to come after creating builder, because
    // .bytes() consumes the response (so response.status() above is
    // out of scope).
    let bytes : Vec<u8> = response.bytes().map_err(HttpError::Transport)?.to_vec();

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
	TokenOrRequest::Request { request, scope_hash, .. } => {
	    let response = do_http_via_reqwest(request)?;
	    let token = TOKEN_PROVIDER.
		parse_token_response(scope_hash, response).unwrap();
	    Ok(token.access_token)
	}
	TokenOrRequest::Token(token) => Ok(token.access_token)
    }
}



fn get_bucket(url: Url) -> Result<Bucket, HttpError> {
    debug!("Looking to request: {:#?}", url);

    let client = new_client()?;
    let token = get_token()?;

    let mut response = client.get(url)
        .bearer_auth(token)
        .send()
        .expect("Failed to send request");

    debug!("{:#?}", response);

    let bucket = response.json::<Bucket>();
    debug!("{:#?}", bucket);
    return bucket.map_err(HttpError::Transport)
}

fn get_object(url: Url) -> Result<Object, HttpError> {
    debug!("Looking to request: {:#?}", url);

    let client = new_client()?;
    let token = get_token()?;

    let mut response = client.get(url)
        .bearer_auth(token)
        .send()
        .expect("Failed to send request");

    debug!("{:#?}", response);

    let object = response.json::<Object>();
    debug!("{:#?}", object);
    return object.map_err(HttpError::Transport)
}


pub fn get_bytes(obj: &Object, offset: u64, how_many: u64) -> Result<Vec<u8>, HttpError> {
    let client = new_client()?;
    return get_bytes_with_client(&client, obj, offset, how_many);
}

pub fn get_bytes_with_client(client: &reqwest::blocking::Client, obj: &Object, offset: u64, how_many: u64) -> Result<Vec<u8>, HttpError> {
    debug!("Asking for {} bytes at {} from the origin for {} (self link = {}", how_many, offset, obj.name, obj.self_link);

    // Use the self_link from the object as the url, but add ?alt=media
    let mut object_url = Url::parse(&obj.self_link).unwrap();
    object_url.query_pairs_mut().append_pair("alt", "media");

    let byte_range = format!("bytes={}-{}", offset, offset + how_many - 1);

    let now = std::time::Instant::now();

    let token = get_token()?;

    let mut response = client.get(object_url)
        .header(reqwest::header::RANGE, byte_range)
        .bearer_auth(token)
        .send()
        .expect("Failed to send request");

    let mut buf: Vec<u8> = vec![];
    let written = response.copy_to(&mut buf)?;
    debug!("Got back {} bytes. Took {:#?}", written, now.elapsed());
    Ok(buf)
}

fn _do_one_list_object(bucket: &str, prefix: Option<&str>, delim: Option<&str>, token: Option<&str>) -> Result<ListObjectsResponse, HttpError> {
    let mut list_url = Url::parse(bucket).unwrap();

    if let Some(prefix_str) = prefix {
        list_url.query_pairs_mut().append_pair("prefix", prefix_str);
    }

    if let Some(delim_str) = delim {
        list_url.query_pairs_mut().append_pair("delimiter", delim_str);
    }

    if let Some(token_str) = token {
        list_url.query_pairs_mut().append_pair("pageToken", token_str);
    }

    let mut op = || -> Result<reqwest::blocking::Response, HttpError> {

        let client = new_client()?;
	let token = get_token()?;

        let mut response = client.get(list_url)
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
    let mut response = op()?;

    //debug!(" ListObject body => {:#?}", response.text()?);

    let list_response = response.json::<ListObjectsResponse>();
    return list_response.map_err(HttpError::Transport);
}

pub fn list_objects(bucket: &str, prefix: Option<&str>, delim: Option<&str>) -> Result<(Vec<Object>, Vec<String>), HttpError> {
    debug!("Asking for a list from bucket '{}' with prefix '{:#?}' and delim = '{:#?}'", bucket, prefix, delim);

    let mut objects: Vec<Object> = vec![];
    let mut prefixes: Vec<String> = vec![];

    let mut page_token: String = String::from("");

    loop {
        let mut resp = match page_token.is_empty() {
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

    #[test]
    fn get_landsat() {
        let landsat_url = Url::parse(
            "https://www.googleapis.com/storage/v1/b/gcp-public-data-landsat"
        ).unwrap();
        get_bucket(landsat_url);
    }

    #[test]
    fn get_private_bucket() {
        let bucket_url = Url::parse(
            "https://www.googleapis.com/storage/v1/b/boulos-vm-ml"
        ).unwrap();
        get_bucket(bucket_url);
    }

    #[test]
    fn get_private_object() {
        let object_url = Url::parse(
            "https://www.googleapis.com/storage/v1/b/boulos-hadoop/o/bdutil-staging%2fhadoop-m%2f20150202-172447-58j%2fbq-mapred-template.xml"
        ).unwrap();
        let object: Object = get_object(object_url).unwrap();
        println!("Object has {} bytes", object.size);

        let bytes: Vec<u8> = get_bytes(&object, 0, 769).unwrap();
        println!("Got back:\n {}", String::from_utf8(bytes).unwrap());

        let offset_bytes: Vec<u8> = get_bytes(&object, 6, 769).unwrap();
        println!("Got back:\n {}", String::from_utf8(offset_bytes).unwrap());
    }

    #[test]
    fn get_public_object() {
        let object_url = Url::parse(
            "https://www.googleapis.com/storage/v1/b/gcp-public-data-landsat/o/LC08%2FPRE%2F044%2F034%2FLC80440342017101LGN00%2FLC80440342017101LGN00_MTL.txt"
        ).unwrap();
        let object: Object = get_object(object_url).unwrap();
        println!("Object has {} bytes", object.size);

	println!("Object debug is {:#?}", object);

        let bytes: Vec<u8> = get_bytes(&object, 0, 4096).unwrap();
        println!("Got back:\n {}", String::from_utf8(bytes).unwrap());

        let offset_bytes: Vec<u8> = get_bytes(&object, 4099, 1024).unwrap();
        println!("Got back:\n {}", String::from_utf8(offset_bytes).unwrap());
    }


    #[test]
    fn test_list_objects() {
        let object_url = "https://www.googleapis.com/storage/v1/b/boulos-hadoop/o";
        let prefix = "bdutil-staging";
        let delim = "/";

        let (objects, _) = list_objects(object_url, Some(prefix), Some(delim)).unwrap();
        println!("Got {} objects", objects.len());
        println!("Dump:\n\n{:#?}", objects);

        let (all_objects, _) = list_objects(object_url, None, None).unwrap();
        println!("Got {} objects", all_objects.len());
        println!("Dump:\n\n{:#?}", all_objects);

        let (only_top_level, prefixes) = list_objects(object_url, None, Some(delim)).unwrap();
        println!("Got {} objects", only_top_level.len());
        println!("Dump:\n\n{:#?}", only_top_level);
        println!("Prefixes:\n\n{:#?}", prefixes);
    }

    #[test]
    fn test_list_paginated() {
        let object_url = "https://www.googleapis.com/storage/v1/b/gcp-public-data-landsat/o";
        let prefix = "LC08/PRE/044/034/";
        let delim = "/";

        let (objects, prefixes) = list_objects(object_url, Some(prefix), Some(delim)).unwrap();
        println!("Got {} objects", objects.len());
        println!("prefixes: {:#?}", prefixes);
        println!("objects: {:#?}", objects)
    }
}
