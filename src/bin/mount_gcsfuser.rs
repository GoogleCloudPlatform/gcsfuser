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

use clap::{crate_version, App, Arg};
use fuser::MountOption;

// Split up a GCS "path-like" into the Bucket and optional Prefix.
fn split_gcs_path(path: String) -> Result<(String, Option<String>), &'static str> {
    // If it's not a path-like, just treat it as a bucket
    if !path.starts_with("gs://") {
        return Ok((path, None));
    }

    // We just tested that this is there.
    let remainder = path.strip_prefix("gs://").unwrap();

    if let Some(pair) = remainder.split_once('/') {
        // Destructure.
        let (bucket, after_slash) = pair;

        // If the after_slash is empty, it's also just a bucket but
        // they included a trailing slash.
        if after_slash.is_empty() {
            return Ok((bucket.to_string(), None));
        }

        // Make sure the suffix has a trailing slash.
        if !after_slash.ends_with('/') {
            return Err("The gs:// path must end with a trailing slash");
        }

        return Ok((bucket.to_string(), Some(after_slash.to_string())));
    } else {
        // Just a bucket name
        return Ok((remainder.to_string(), None));
    }
}

fn build_mount_options(option_strs: Vec<&str>) -> Result<Vec<MountOption>, &'static str> {
    // Build up our MountOptions
    let mut options = vec![MountOption::FSName("gcsfuser".to_string())];

    // We don't want to support atime (GCS doesn't have it as of July 2021).
    options.push(MountOption::NoAtime);

    // Always ask for AutoUnmount.
    options.push(MountOption::AutoUnmount);

    #[cfg(target_os = "macos")]
    options.push(MountOption::CUSTOM("noappledouble".into()));

    for option in option_strs {
        match option {
            "auto_umount" =>
            /* Already added */
            {
                ()
            }
            "ro" => options.push(MountOption::RO),
            "rw" => options.push(MountOption::RW),
            "atime" => return Err("atime unsupported"),
            "noatime" =>
            /* already added */
            {
                ()
            }
            "dirsync" => options.push(MountOption::DirSync),
            "sync" => options.push(MountOption::Sync),
            "async" => options.push(MountOption::Async),
            // TODO(boulos): Handle UID / GID / allow_other, etc.
            _ => return Err("Unsupported option"),
        }
    }

    // We could check that the options don't conflict / have repeats,
    // but let the mount call do that for us.
    Ok(options)
}

fn main() {
    let _ = env_logger::init();

    // Support mount.gcsfuser < gcs_path > < mount_point > -o < mount_options >
    let matches = App::new("mount.gcsfuser")
        .version(crate_version!())
        .author("Solomon Boulos")
        .arg(
            Arg::with_name("gcs_path")
		// Make
                .required(true)
                .takes_value(true)
                .help("GCS path to mount. Either the bucket name (e.g., gcp-public-data-landsat) or a gs://bucket/path string")
        )
        .arg(
            Arg::with_name("mount_point")
                .required(true)
                .takes_value(true)
                .help("Local path for the mount point")
        )
	// TODO(boulos): Handle this as -o parsing.
        .arg(
            Arg::with_name("options")
		.short("o")
		.use_delimiter(true)
                .required(false)
                .takes_value(true)
                .help("Mount options (see `man mount`)"),
        )
        .get_matches();

    // Both the path and mountpoint are required, so unwrap is fine.
    let gcs_path: String = matches.value_of("gcs_path").unwrap().to_string();
    let mountpoint: String = matches.value_of("mount_point").unwrap().to_string();
    // Get the options, or an empty array.
    let option_strs = matches.values_of("options").unwrap_or_default().collect();

    // Compute our bucket and optional prefix.
    let (bucket, prefix) = split_gcs_path(gcs_path).expect("Failed to parse gcs_path");

    // And build up the mount options, before we go reading from GCS.
    let options = build_mount_options(option_strs).expect("Failed to parse mount options");

    // We've got reasonable args, let's do it! First, make our GCS handler.
    let fs = gcsfuser::GCSFS::new(bucket, prefix);

    // Then mount it.
    fuser::mount2(fs, &mountpoint, &options).unwrap();
}
