extern crate reqwest;
extern crate serde;
extern crate serde_json;
extern crate serde_with;
extern crate url;

use reqwest::header::{Authorization, Bearer, Range};
use url::Url;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    id: String,
    name: String,
    location: String,
    self_link: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Object {
    id: String,
    name: String,
    bucket: String,
    self_link: String,
    #[serde(with = "serde_with::rust::display_fromstr")]
    size: u64,
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ListObjectsResponse {
    next_page_token: Option<String>,
    items: Vec<Object>,
}

fn new_client() -> Result<reqwest::Client, reqwest::Error> {
    // NOTE(boulos): The service account needs both storage viewer (to see objects) and *project* viewer to see the Bucket.
    let creds = Bearer {
        token: "IMABADPERSON_HARDCODED_HERE_gcloud_auth_print_access_token_for_the_service_account".to_owned()
    };

    let mut headers = reqwest::header::Headers::new();
    headers.set(Authorization(creds));

    return reqwest::Client::builder()
        .default_headers(headers)
        .build();
}

fn get_bucket(url: Url) -> Result<Bucket, reqwest::Error> {
    println!("Looking to request: {:#?}", url);

    let client = new_client()?;

    let mut response = client.get(url)
        .send()
        .expect("Failed to send request");

    println!("{:#?}", response);

    let bucket = response.json::<Bucket>();
    println!("{:#?}", bucket);
    return bucket
}

fn get_object(url: Url) -> Result<Object, reqwest::Error> {
    println!("Looking to request: {:#?}", url);

    let client = new_client()?;

    let mut response = client.get(url)
        .send()
        .expect("Failed to send request");

    println!("{:#?}", response);

    let object = response.json::<Object>();
    println!("{:#?}", object);
    return object
}

pub fn get_bytes(obj: &Object, offset: u64, how_many: u64) -> Result<Vec<u8>, reqwest::Error> {
    println!("Asking for {} bytes at {} from the origin for {}", how_many, offset, obj.name);

    // Use the self_link from the object as the url, but add ?alt=media
    let mut object_url = Url::parse(&obj.self_link).unwrap();
    object_url.query_pairs_mut().append_pair("alt", "media");

    let client = new_client()?;

    let mut response = client.get(object_url)
        .header(Range::bytes(offset, offset + how_many))
        .send()
        .expect("Failed to send request");

    let mut buf: Vec<u8> = vec![];
    let written = response.copy_to(&mut buf)?;
    println!("Got back {} bytes", written);
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

    let client = new_client()?;

    let mut response = client.get(list_url)
        .send()
        .expect("Failed to send request");

    let list_response = response.json::<ListObjectsResponse>();
    return list_response;
}

fn list_objects(bucket: &str, prefix: Option<&str>, delim: Option<&str>) -> Result<Vec<Object>, reqwest::Error> {
    println!("Asking for a list from bucket '{}' with prefix '{:#?}' and delim = '{:#?}'", bucket, prefix, delim);

    let mut results: Vec<Object> = vec![];

    let mut page_token: String = String::from("");

    loop {
        let mut resp = match page_token.is_empty() {
            true => _do_one_list_object(bucket, prefix, delim, None)?,
            false => _do_one_list_object(bucket, prefix, delim, Some(&page_token))?,
        };

        results.append(&mut resp.items);

        match resp.next_page_token {
            Some(temp_token_str) => page_token = temp_token_str.clone(),
            None => break,
        }
    }

    Ok(results)
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
    fn test_list_objects() {
        let object_url = "https://www.googleapis.com/storage/v1/b/boulos-hadoop/o";
        let prefix = "bdutil-staging";
        let delim = "%2F";

        let objects: Vec<Object> = list_objects(object_url, Some(prefix), Some(delim)).unwrap();
        println!("Got {} objects", objects.len());
        println!("Dump:\n\n{:#?}", objects);

        let all_objects: Vec<Object> = list_objects(object_url, None, None).unwrap();
        println!("Got {} objects", all_objects.len());
        println!("Dump:\n\n{:#?}", all_objects);

        let only_top_level: Vec<Object> = list_objects(object_url, None, Some(delim)).unwrap();
        println!("Got {} objects", only_top_level.len());
        println!("Dump:\n\n{:#?}", only_top_level);
    }

    #[test]
    fn test_list_paginated() {
        let object_url = "https://www.googleapis.com/storage/v1/b/gcp-public-data-landsat/o";
        let prefix = "LC08/PRE/044/034/";
        let delim = "%2F";

        let objects: Vec<Object> = list_objects(object_url, Some(prefix), Some(delim)).unwrap();
        println!("Got {} objects", objects.len());
    }
}
