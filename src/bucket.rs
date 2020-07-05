extern crate reqwest;
extern crate serde;
extern crate serde_json;
extern crate serde_with;
extern crate url;

use url::Url;
use std::{thread, time};

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
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ListObjectsResponse {
    next_page_token: Option<String>,
    prefixes: Option<Vec<String>>,
    items: Option<Vec<Object>>,
}


pub fn get_token() -> Result<&'static str, reqwest::Error> {
    // NOTE(boulos): The service account needs both storage viewer (to see objects) and *project* viewer to see the Bucket.
    static GCLOUD_TOKEN: &'static str = "IMABADPERSON_HARDCODED_HERE_gcloud_auth_print_access_token_for_the_service_account";

    Ok(GCLOUD_TOKEN)
}


pub fn new_client() -> Result<reqwest::blocking::Client, reqwest::Error> {
    // NOTE(boulos): Previously, I was passing some default headers
    // here. Now that bearer_auth is per request, this is less needed,
    // but we can change that later.
    return reqwest::blocking::Client::builder()
        .build();
}

fn get_bucket(url: Url) -> Result<Bucket, reqwest::Error> {
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
    return bucket
}

fn get_object(url: Url) -> Result<Object, reqwest::Error> {
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
    return object
}


pub fn get_bytes(obj: &Object, offset: u64, how_many: u64) -> Result<Vec<u8>, reqwest::Error> {
    let client = new_client()?;
    return get_bytes_with_client(&client, obj, offset, how_many);
}

pub fn get_bytes_with_client(client: &reqwest::blocking::Client, obj: &Object, offset: u64, how_many: u64) -> Result<Vec<u8>, reqwest::Error> {
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

fn _do_one_list_object(bucket: &str, prefix: Option<&str>, delim: Option<&str>, token: Option<&str>) -> Result<ListObjectsResponse, reqwest::Error> {
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

    let mut op = || -> Result<reqwest::blocking::Response, reqwest::Error> {

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
    return list_response;
}

pub fn list_objects(bucket: &str, prefix: Option<&str>, delim: Option<&str>) -> Result<(Vec<Object>, Vec<String>), reqwest::Error> {
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
        std::thread::sleep(time::Duration::from_millis(10));
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
