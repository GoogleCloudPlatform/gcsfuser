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

fn get_bytes(obj: &Object, offset: u64, how_many: u64) -> Result<Vec<u8>, reqwest::Error> {
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
}
