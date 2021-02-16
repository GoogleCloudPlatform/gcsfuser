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

use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyWrite, Request,
};
use libc::{EIO, ENOENT, ENOTDIR, O_DIRECT};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::Object;
use crate::bucket::list_objects;

pub type Inode = u64;

// TODO(boulos): What's a reasonable TTL? Since we're focused on
// read-only, let's set at least 30s. Amusingly, Free BSD now treats
// *all* numbers > 0 as "cache forever" (which is probably what we
// want, with invalidation)
const TTL_30s: Duration = Duration::from_secs(30);

// It's not clear if anything cares about this.
const HARDCODED_BLOCKSIZE: u32 = 512;

struct PsuedoDir {
    name: String,
    // inline
    entries: Vec<(String, Inode)>,
}

// TODO(boulos): Decide if we just use BTreeMap rather than HashMaps and Vecs...
pub struct GCSFS {
    // Inode => Attr
    inode_to_attr: RwLock<HashMap<Inode, FileAttr>>,

    // From inode to raw GCS Object.
    inode_to_obj: RwLock<HashMap<Inode, Object>>,
    // From inode => the path name
    directory_map: RwLock<HashMap<Inode, PsuedoDir>>,

    // We probably need some way to implement forget, but whatever.
    inode_counter: Mutex<Inode>,

    // So we can refer into the file handle map.
    fh_counter: AtomicU64,

    file_handles: RwLock<HashMap<u64, super::bucket::ResumableUploadCursor>>,

    // GCS configuration
    gcs_bucket: String,
    gcs_prefix: Option<String>,

    // Persistent client
    gcs_client: super::bucket::GcsHttpClient,

    // And our runtime for waiting out async.
    tokio_rt: tokio::runtime::Runtime,
}

impl GCSFS {
    pub fn new(bucket: String, prefix: Option<String>) -> Self {
        info!("Making a GCSFS!");
        GCSFS {
            inode_to_attr: RwLock::new(HashMap::new()),
            inode_to_obj: RwLock::new(HashMap::new()),
            directory_map: RwLock::new(HashMap::new()),
            inode_counter: Mutex::new(0),
            // Start at 1, so we can hand out fh=0 as "no fh"
            fh_counter: AtomicU64::new(1),
            file_handles: RwLock::new(HashMap::new()),
            gcs_bucket: bucket,
            gcs_prefix: prefix,
            gcs_client: super::bucket::new_client(),
            tokio_rt: tokio::runtime::Runtime::new().unwrap(),
        }
    }

    fn get_inode(&self) -> Inode {
        // Grab an inode. We pre-increment so that the root inode gets 1.
        let mut data = self.inode_counter.lock().unwrap();
        *data += 1;
        return *data;
    }

    fn make_fh(&self, cursor: super::bucket::ResumableUploadCursor) -> u64 {
        let fh = self.fh_counter.fetch_add(1, Ordering::SeqCst);
        // Put the cursor into our hash map.
        self.file_handles.write().unwrap().insert(fh, cursor);
        // return the handle.
        fh
    }

    fn drop_fh(&self, fh: u64) {
        let _ = self.file_handles.write().unwrap().remove(&fh);
        return;
    }

    fn load_file(&self, full_path: String, obj: Object) -> Inode {
        let inode = self.get_inode();

        // NOTE(boulos): This is pretty noisy on boot and env_logger doesn't seem to have
        // some sort of "super noisy debug". So comment in/out if you need to debugg the
        // loading process.

        //debug!("   GCSFS. Loading {}", full_path);

        let mtime: SystemTime = obj.updated.into();
        let ctime: SystemTime = obj.time_created.into();
        // GCS doesn't have atime, use mtime
        let atime = mtime;

        let file_attr: FileAttr = FileAttr {
            ino: inode,
            size: obj.size,
            blocks: 1, /* grr. obj.size / blksize? */
            atime: atime,
            mtime: mtime,
            ctime: ctime,
            crtime: ctime,
            kind: FileType::RegularFile,
            perm: 0o755, /* Mark everything as 755 */
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
            blksize: HARDCODED_BLOCKSIZE,
            padding: 0,
        };

        self.inode_to_attr.write().unwrap().insert(inode, file_attr);
        self.inode_to_obj.write().unwrap().insert(inode, obj);

        return inode;
    }

    // Given a bucket, and a prefix (the directory), load it
    fn load_dir(&self, prefix: Option<String>, parent: Option<Inode>) -> Inode {
        let bucket_clone = self.gcs_bucket.clone();
        let prefix_clone = prefix.clone();

        let prefix_for_load: String = match prefix {
            Some(prefix_str) => prefix_str,
            None => String::from(""),
        };

        // As above (this is too noisy for debugging regular FS operation).

        //debug!("   GCSFS. DIR {}", prefix_for_load);

        // Always use / as delim.
        let (single_level_objs, subdirs) = list_objects(
            bucket_clone.as_ref(),
            prefix_clone.as_ref().map(String::as_str),
            Some("/"),
        )
        .unwrap();

        let dir_inode = self.get_inode();
        let dir_time: SystemTime = UNIX_EPOCH + Duration::new(1534812086, 0); // 2018-08-20 15:41 Pacific

        let dir_attr: FileAttr = FileAttr {
            ino: dir_inode,
            size: 0,
            blocks: 0,
            atime: dir_time,
            mtime: dir_time,
            ctime: dir_time,
            crtime: dir_time,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: (single_level_objs.len() + 2) as u32,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
            blksize: HARDCODED_BLOCKSIZE,
            padding: 0,
        };

        self.inode_to_attr
            .write()
            .unwrap()
            .insert(dir_inode, dir_attr);

        let parent_inode = match parent {
            Some(parent_val) => parent_val,
            None => dir_inode,
        };

        let mut dir_entries: Vec<(String, Inode)> = vec![
            (String::from("."), dir_inode), /* self link */
            (String::from(".."), parent_inode),
        ];

        // GCS returns paths relative to the root of the bucket for
        // obj.name. Strip off the prefix to get the "filename".
        let base_dir_index = prefix_for_load.len();

        // Load the subdirectories in parallel, gathering up their results in order.
        let subdir_len = subdirs.len();
        let inodes: Arc<RwLock<Vec<Inode>>> = Arc::new(RwLock::new(Vec::with_capacity(subdir_len)));
        // Pre-fill the array with 0s, so we can write into each slot blindly later.
        inodes
            .write()
            .unwrap()
            .resize_with(subdir_len, Default::default);

        rayon::scope(|s| {
            // NOTE(boulos): We have to do this so that the move
            // closure below doesn't capture the real self / we
            // indicate that its lifetime matches that of this Rayon
            // scope.
            let shadow_self = &self;
            // Loop over all the subdirs, recursively loading them.
            for (i, dir) in subdirs.iter().enumerate() {
                let inodes_clone = Arc::clone(&inodes);
                s.spawn(move |_| {
                    let inode = shadow_self.load_dir(Some(dir.to_string()), Some(dir_inode));
                    let mut write_context = inodes_clone.write().unwrap();

                    if let Some(elem) = write_context.get_mut(i) {
                        *elem = inode;
                    } else {
                        println!(
                            "ERROR: Tried to write inode '{}' to index {} \
                                 for directory {} (subdirs has len {})",
                            inode, i, dir, subdir_len
                        );
                    }
                });
            }
        });

        for (i, dir) in subdirs.iter().enumerate() {
            // To insert the "directory name", we get the basedir and
            // strip the trailing slash.
            let last_slash = dir.len() - 1;
            let dir_str = dir[base_dir_index..last_slash].to_string();
            let read_context = inodes.read().unwrap();

            dir_entries.push((dir_str, read_context[i]));
        }

        // Loop over all the direct objects, adding them to our maps
        for obj in single_level_objs {
            // Extract just the portion that is the "file name".
            let file_str = obj.name[base_dir_index..].to_string();
            let full_path = format!("{}{}", prefix_for_load, file_str);

            let inode = self.load_file(full_path, obj);

            dir_entries.push((file_str, inode));
        }

        //debug!("  Created dir_entries: {:#?}", dir_entries);

        self.directory_map.write().unwrap().insert(
            dir_inode,
            PsuedoDir {
                name: prefix_for_load.to_string(),
                entries: dir_entries,
            },
        );

        return dir_inode;
    }
}

impl Filesystem for GCSFS {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> Result<(), i32> {
        info!("init!");
        debug!("debug_logger: init!");
        debug!("Kernelconfig is {:#?}", config);

        let prefix = self.gcs_prefix.clone();

        // Trigger a load from the root of the bucket
        let root_inode = self.load_dir(prefix, None);
        debug!("root inode => {}", root_inode);

        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: Inode, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent={}, name={})", parent, name.to_str().unwrap());

        if let Some(dir_ent) = self.directory_map.read().unwrap().get(&parent) {
            // TODO(boulos): Is this the full name, or just the portion? (I believe just portion)
            let search_name = name.to_str().unwrap().to_string();
            for child_pair in dir_ent.entries.iter() {
                debug!(
                    "  Is search target '{}' == dir_entry '{}'?",
                    search_name, child_pair.0
                );
                if child_pair.0 == search_name {
                    if let Some(attr) = self.inode_to_attr.read().unwrap().get(&child_pair.1) {
                        // Found it! Return the info for the inode.
                        debug!(
                            "  Found it! search target '{}' is inode {}",
                            child_pair.0, child_pair.1
                        );
                        reply.entry(&TTL_30s, &attr, 0);
                        return;
                    }
                }
            }
            debug!("  Never found  '{}'", search_name);
        }

        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, inode: Inode, reply: ReplyAttr) {
        debug!("Trying to getattr() on inode {}", inode);

        if let Some(attr) = self.inode_to_attr.read().unwrap().get(&inode) {
            reply.attr(&TTL_30s, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        inode: Inode,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        debug!(
            "Trying to read() {} on {} at offset {}",
            _size, inode, offset
        );
        if let Some(obj) = self.inode_to_obj.read().unwrap().get(&inode) {
            debug!("  Performing read for obj: {:#?}", obj);
            let result = self.tokio_rt.block_on(async {
                super::bucket::get_bytes_with_client(
                    &self.gcs_client,
                    obj,
                    offset as u64,
                    _size as u64,
                )
                .await
            });

            match result {
                Ok(bytes) => {
                    reply.data(&bytes);
                }
                Err(e) => {
                    debug!("  get_bytes failed. Error {:#?}", e);
                    reply.error(EIO);
                }
            }
        } else {
            debug!("  failed to find the object for inode {}", inode);
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        inode: Inode,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("Trying to readdir on {} with offset {}", inode, offset);
        if let Some(dir_ent) = self.directory_map.read().unwrap().get(&inode) {
            debug!(
                "  directory {} has {} entries ({:#?})",
                inode,
                dir_ent.entries.len(),
                dir_ent.entries
            );
            let mut absolute_index = offset + 1;
            for (idx, ref child_pair) in dir_ent.entries.iter().skip(offset as usize).enumerate() {
                debug!(
                    "    looking at entry {}, got back pair {:#?}",
                    idx, child_pair
                );

                if let Some(child_ent) = self.inode_to_attr.read().unwrap().get(&child_pair.1) {
                    debug!(
                        "  readdir for inode {}, adding '{}' as inode {}",
                        inode, child_pair.0, child_pair.1
                    );
                    if reply.add(
                        child_pair.1,
                        absolute_index as i64,
                        child_ent.kind,
                        &child_pair.0,
                    ) {
                        // We've filled up our reply buffer. Exit.
                        break;
                    }
                    absolute_index += 1;
                } else {
                    debug!("  readdir for inode {}, could not find inode {} which was given in dir_ent as '{}'", inode, child_pair.1, child_pair.0);
                    reply.error(ENOENT);
                    return;
                }
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        debug!(
            "create(parent_dir = {}, path = {:#?}, mode = {}, flags = {:o})",
            parent, name, mode, flags
        );

        let file_type = mode & libc::S_IFMT as u32;

        if file_type != libc::S_IFREG && file_type != libc::S_IFDIR {
            warn!(
                "create called for file_type {}. But we only handle FILE/DIR",
                file_type
            );
            reply.error(libc::EINVAL);
            return;
        }

        // Grab a scoped lock for the directory map (we'll update the directory)
        let mut dir_map_lock = self.directory_map.write().unwrap();

        // Find the parent in the directory map.
        let mut parent_ent = dir_map_lock.get_mut(&parent);

        // NOTE(boulos): I think FUSE does this check already.
        if parent_ent.is_none() {
            debug!(" -- warning/error:fuse_create called w/o parent directory");
            reply.error(ENOTDIR);
            return;
        }

        let mut parent_dir = parent_ent.unwrap();
        let mut dir_entries = &mut parent_dir.entries;

        let search_name = name.to_str().unwrap().to_string();
        let full_name = match parent_dir.name.len() {
            // Don't include the leading / for the root directory.
            0 => search_name.clone(),
            _ => format!("{}/{}", parent_dir.name, search_name),
        };

        #[cfg(debug)]
        // FUSE isn't supposed to call this for existing entries. Double check in debug mode.
        for child_pair in dir_entries.iter() {
            if child_pair.0 == search_name {
                debug!(" -- warning/error: File {} already exists!", search_name);
                reply.error(EEXIST);
                return;
            }
        }

        // Make a new inode for our new file or directory.
        let inode = self.get_inode();
        let now = SystemTime::now();

        let kind = match file_type {
            libc::S_IFREG => FileType::RegularFile,
            libc::S_IFDIR => FileType::Directory,
            _ => unreachable!(),
        };

        // If it's going to be a regular file, try to initiate the
        // Upload. If this fails, we've burned an inode, but whatever.
        let fh = match kind {
            FileType::RegularFile => {
                let create_result = self.tokio_rt.block_on(async {
                    super::bucket::create_object_with_client(
                        &self.gcs_client,
                        &self.gcs_bucket,
                        &full_name,
                    )
                    .await
                });

                if create_result.is_err() {
                    // We failed (and warned internally). Bubble up an EIO.
                    reply.error(libc::EIO);
                    return;
                }

                // Make ourselves a file handle!
                self.make_fh(create_result.unwrap())
            }
            // Directories don't need FHs for now.
            _ => 0,
        };

        let attrs: FileAttr = FileAttr {
            ino: inode,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: kind,
            perm: 0o755, /* We could use mode, but whatever */
            nlink: match kind {
                FileType::RegularFile => 1,
                FileType::Directory => 2,
                _ => unreachable!(),
            },
            uid: req.uid(),
            gid: req.gid(),
            rdev: 0,
            flags: 0,
            blksize: HARDCODED_BLOCKSIZE,
            padding: 0,
        };

        // Put the node into our inode map
        self.inode_to_attr.write().unwrap().insert(inode, attrs);

        // Put the node into our parent directory listing.
        dir_entries.push((search_name.clone(), inode));

        if kind == FileType::Directory {
            // Make our own PsuedoDir
            let sub_entries: Vec<(String, Inode)> = vec![
                (String::from("."), inode), /* Self link */
                (String::from(".."), parent as Inode),
            ];

            dir_map_lock.insert(
                inode,
                PsuedoDir {
                    name: search_name.clone(),
                    entries: sub_entries,
                },
            );
        } else {
            // Otherwise, maybe prepare a stub Object?
        }

        reply.created(&TTL_30s, &attrs, 0, fh, 0);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let now = SystemTime::now();
        debug!(
            "Got a write request. inode={}, fh={}, offset={}",
            inode, fh, offset
        );

        let fh_clone = fh.clone();
        let mut fh_map = self.file_handles.write().unwrap();
        let mut cursor_or_none = fh_map.get_mut(&fh_clone);

        if cursor_or_none.is_none() {
            error!("write(): didn't find the fh {}", fh);
            reply.error(libc::EBADF);
            return;
        }

        let inode_clone = inode.clone();
        let mut attr_map = self.inode_to_attr.write().unwrap();
        let mut attr_or_none = attr_map.get_mut(&inode_clone);
        if attr_or_none.is_none() {
            error!("write(): Didn't find inode {}", inode);
            reply.error(libc::EBADF);
            return;
        }

        let mut cursor = cursor_or_none.unwrap();
        let remaining = cursor.buffer.capacity() - cursor.buffer.len();
        let cursor_position = cursor.offset + (cursor.buffer.len() as u64);

        debug!(
            "cursor has URI {}, offset {}, 'position' {}, and remaining {}",
            cursor.session_uri, cursor.offset, cursor_position, remaining
        );

        // Only allow writing at the offset.
        if (offset as u64) != cursor_position {
            error!(
                "Got a write for offset {}. But cursor is at {} (we only support appending",
                offset, cursor_position
            );
            reply.error(libc::EINVAL);
            return;
        }

        let result = self.tokio_rt.block_on(async {
            super::bucket::append_bytes_with_client(&self.gcs_client, cursor, data).await
        });

        if result.is_err() {
            reply.error(libc::EIO);
            return;
        }

        let len = result.unwrap() as u64;

        // We wrote the bytes! Update our attrs.
        reply.written(len as u32);
        let mut attr = attr_or_none.unwrap();
        // Update our atime/mtime.
        attr.atime = now;
        attr.mtime = now;
        // Update the bytes written.
        attr.size += len;
    }

    // NOTE(boulos): Despite the name 'flush', this is called on *close*.
    fn flush(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        debug!("flush called for inode {} / fh {}", inode, fh);
        if fh == 0 {
            // Ignore fh 0
            reply.ok();
            return;
        }

        let now = SystemTime::now();

        let fh_clone = fh.clone();
        let mut fh_map = self.file_handles.write().unwrap();
        let mut cursor_or_none = fh_map.get_mut(&fh_clone);

        if cursor_or_none.is_none() {
            error!("flush(): didn't find the fh {}", fh);
            reply.error(libc::EBADF);
            return;
        }

        let inode_clone = inode.clone();
        let mut attr_map = self.inode_to_attr.write().unwrap();
        let mut attr_or_none = attr_map.get_mut(&inode_clone);
        if attr_or_none.is_none() {
            error!("flush(): Didn't find inode {}", inode);
            reply.error(libc::EBADF);
            return;
        }

        let mut cursor = cursor_or_none.unwrap();
        let result = self.tokio_rt.block_on(async {
            super::bucket::finalize_upload_with_client(&self.gcs_client, cursor).await
        });

        if result.is_err() {
            error!("finalze upload failed with err {:#?}", result);
            reply.error(libc::EIO);
            return;
        }

        // We've got an Object now! Update our FileAttr and map.
        let obj: Object = result.unwrap();
        debug!("Got back our object! {:#?}", obj);

        let mut attr = attr_or_none.unwrap();
        // Update our atime/mtime.
        attr.atime = now;
        attr.mtime = now;
        // Finalize the object size.
        attr.size = obj.size;

        // Put the Object into our map (allownig reads!).
        self.inode_to_obj.write().unwrap().insert(inode, obj);

        reply.ok();
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
    extern crate tempdir;
    extern crate tempfile;

    use super::*;
    use std;
    use std::env;
    use std::fs;
    use std::fs::File;
    use std::io;
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::process::Command;
    use std::thread;
    use std::time;
    use tempdir::TempDir;
    use tempfile::NamedTempFile;

    fn init() {
        // https://docs.rs/env_logger/0.8.2/env_logger/index.html#capturing-logs-in-tests
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn run_ls(cwd: &str) {
        info!("about to run ls -lFGa in cwd: {}", cwd);

        let output = Command::new("ls")
            .arg("-l")
            .arg("-a")
            .arg("-F")
            .arg("-G")
            .current_dir(cwd)
            .output()
            .expect("ls failed");

        info!("status: {}", output.status);
        info!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());
    }

    fn run_cp(src_path: &str, dst_path: &str, cwd: &str) {
        info!("about to run cp {} {} in cwd: {}", src_path, dst_path, cwd);

        let now = std::time::Instant::now();
        let output = Command::new("cp")
            .arg(src_path)
            .arg(dst_path)
            .current_dir(cwd)
            .output()
            .expect("cp failed");

        info!("status: {}", output.status);
        info!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        info!("inside run_cp: {:#?}", now.elapsed());
        assert!(output.status.success());
    }

    fn run_dd(src_path: &str, dst_path: &str, blksize_bytes: usize, cwd: &str) {
        info!(
            "about to run dd if={} of={} bs={} in cwd: {}",
            src_path, dst_path, blksize_bytes, cwd
        );

        let now = std::time::Instant::now();
        let output = Command::new("dd")
            .arg(format!("bs={}", blksize_bytes))
            .arg(format!("if={}", src_path))
            .arg(format!("of={}", dst_path))
            .current_dir(cwd)
            .output()
            .expect("dd failed");

        info!("status: {}", output.status);
        info!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        info!("inside run_dd: {:#?}", now.elapsed());
        assert!(output.status.success());
    }

    fn run_stat(path: &str, cwd: &str) {
        info!("about to run stat {} in cwd: {}", path, cwd);

        let now = std::time::Instant::now();
        let output = Command::new("stat")
            .arg(path)
            .current_dir(cwd)
            .output()
            .expect("stat failed");

        info!("status: {}", output.status);
        info!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        info!("inside run_cp: {:#?}", now.elapsed());
        assert!(output.status.success());
    }

    pub unsafe fn mount_bucket<'a>(
        object_url: String,
        prefix: Option<String>,
        mountpoint: String,
        read_only: bool,
    ) {
        let fs = GCSFS::new(object_url, prefix);

        let options = [
            "-o",
            "rw",
            "-o",
            "auto_unmount",
            "-o",
            "noatime",
            "-o",
            "fsname=gcsfs",
            /* "-o", "noappledouble" /* Disable ._. and .DS_Store files */ */
        ]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

        info!(
            "Attempting to mount gcsfs @ {} with {:#?}",
            mountpoint, options
        );

        fuser::mount(fs, &mountpoint, &options).unwrap();
        panic!("We should never get here, right...?");
    }

    pub unsafe fn mount_tempdir_ro<'a>(mountpoint: PathBuf) {
        let bucket = "gcp-public-data-landsat";
        // Simple single dir.
        //let prefix = "LC08/PRE/044/034/LC80440342017101LGN00/";
        // One level up to test subdir loading.
        let prefix = "LC08/PRE/044/034/";

        mount_bucket(
            bucket.to_string(),
            Some(prefix.to_string()),
            mountpoint.to_str().unwrap().to_string(),
            true,
        );
    }

    pub unsafe fn mount_tempdir_rw<'a>(mountpoint: PathBuf) {
        let bucket = "boulos-rustgcs";

        mount_bucket(
            bucket.to_string(),
            None,
            mountpoint.to_str().unwrap().to_string(),
            false,
        );
    }

    #[test]
    fn just_mount<'a>() {
        init();

        let dir = TempDir::new("just_mount").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| unsafe {
            mount_tempdir_ro(mnt);
        });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);
    }

    #[test]
    fn mount_and_read<'a>() {
        init();

        let dir = TempDir::new("mount_and_read").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| unsafe {
            mount_tempdir_ro(mnt);
        });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");

        let txt_file = "LC80440342017101LGN00/LC80440342017101LGN00_MTL.txt";
        let to_open = format!("{}/{}", mnt_str, txt_file);
        info!("Try to open '{}'", to_open);
        let result = fs::read_to_string(to_open).unwrap();
        info!(" got back {}", result);
        drop(daemon);
    }

    #[test]
    fn large_read<'a>() {
        init();

        let dir = TempDir::new("large_read").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| unsafe {
            mount_tempdir_ro(mnt);
        });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");

        let tif_file = "LC80440342017101LGN00_B7.TIF";
        let sub_dir = "LC80440342017101LGN00";
        let to_open = format!("{}/{}/{}", mnt_str, sub_dir, tif_file);
        info!("Try to open '{}'", to_open);

        use std::os::unix::fs::OpenOptionsExt;

        let mut fh = std::fs::OpenOptions::new()
            .read(true)
            .open(to_open)
            .expect("Failed to open file");

        let mut buffer = [0; 1024 * 1024];
        info!("About to read 1MB from {:#?}", fh);
        let result = fh.read(&mut buffer);
        info!(" got back {:#?}", result);
        drop(daemon);
    }

    #[test]
    fn direct_read<'a>() {
        init();

        let dir = TempDir::new("direct_read").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| unsafe {
            mount_tempdir_ro(mnt);
        });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");

        let tif_file = "LC80440342017101LGN00_B7.TIF";
        let sub_dir = "LC80440342017101LGN00";
        let to_open = format!("{}/{}/{}", mnt_str, sub_dir, tif_file);
        info!("Try to open '{}'", to_open);

        use std::os::unix::fs::OpenOptionsExt;

        let mut fh = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(to_open)
            .expect("Failed to open file");

        let mut buffer = [0; 1024 * 1024];
        info!("About to read 1MB from {:#?}", fh);
        let result = fh.read(&mut buffer);
        info!(" got back {:#?}", result);
        drop(daemon);
    }

    #[test]
    fn small_write<'a>() {
        init();

        let dir = TempDir::new("mount_and_write").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| unsafe {
            mount_tempdir_rw(mnt);
        });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");

        let mut tmp_file = NamedTempFile::new_in(mnt_str).unwrap();
        info!("Opened '{:#?}'", tmp_file.path());
        let mut txt_file = tmp_file.as_file_mut();

        let write_result = txt_file.write_all(b"My first words!");
        info!(" got back {:#?}", write_result);
        assert!(write_result.is_ok());
        // sync the file to see if we have any other errors.
        let sync_result = txt_file.sync_all();
        info!(" sync result => {:#?}", sync_result);
        assert!(sync_result.is_ok());
        // drop the file to close it.
        drop(txt_file);
        info!("Sleeping for 250ms, to wait for the FS to be flush, because shitty");
        std::thread::sleep(Duration::from_millis(250));

        // Drop the daemon to clean up.
        drop(daemon);
    }

    #[test]
    fn large_write<'a>() {
        init();

        let dir = TempDir::new("large_write").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| unsafe {
            mount_tempdir_rw(mnt);
        });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");

        let mut tmp_file = NamedTempFile::new_in(mnt_str).unwrap();
        info!("Opened '{:#?}'", tmp_file.path());
        let mut file = tmp_file.as_file_mut();

        // Make lots of 0123456789 piles and test out our rounding code.
        let small_amt = 20;
        // This should trigger a flush, and then append.
        let medium_amt = 350 * 1024;
        // This will fill the buffer and flush.
        let round_up = 512 * 1024;
        // This will send several *without* buffering.
        let several_chunks = 2048 * 1024;
        // This will then send 256 KiB, leave some of left over for a final close.
        let plus_leftover = several_chunks + 384 * 1024;

        let small_write: Vec<u8> = (0..small_amt).map(|x| (48 + (x % 10)) as u8).collect();
        let big_write: Vec<u8> = (small_amt..medium_amt)
            .map(|x| (48 + (x % 10)) as u8)
            .collect();
        let round_up_write: Vec<u8> = (medium_amt..round_up)
            .map(|x| (48 + (x % 10)) as u8)
            .collect();
        let chunk_write: Vec<u8> = (round_up..several_chunks)
            .map(|x| (48 + (x % 10)) as u8)
            .collect();
        let final_write: Vec<u8> = (several_chunks..plus_leftover)
            .map(|x| (48 + (x % 10)) as u8)
            .collect();

        let all_writes = vec![
            small_write,
            big_write,
            round_up_write,
            chunk_write,
            final_write,
        ];

        for write_test in all_writes {
            let write_result = file.write_all(&write_test);
            info!(" got back {:#?}", write_result);
            assert!(write_result.is_ok());
        }

        // sync the file to see if we have any other errors.
        let sync_result = file.sync_all();
        info!(" sync result => {:#?}", sync_result);
        assert!(sync_result.is_ok());
        // drop the file to close and unlink it.
        drop(file);
        info!("Sleeping for 250ms, to wait for the FS to be flush, because shitty");
        std::thread::sleep(Duration::from_millis(250));

        // Drop the daemon to clean up.
        drop(daemon);
    }

    #[test]
    fn mount_and_ls<'a>() {
        init();

        // Mount the filesystem and run ls.
        info!("Running mount_and_ls");

        let dir = TempDir::new("mount_and_ls").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());

        let fs = thread::spawn(|| unsafe {
            mount_tempdir_ro(mnt);
        });
        info!("mounted fs at {} on thread {:#?}", mnt_str, fs);
        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");
        run_ls(&mnt_str);

        let subdir = format!("{}/{}", mnt_str, "LC80440342013170LGN00");
        info!("now ls in the subdir {}", subdir);
        run_ls(&subdir);
        drop(fs);
    }

    #[test]
    #[ignore]
    // This test copies the entire 70MB file, so don't usually run it.
    fn mount_and_cp<'a>() {
        init();

        let dir = TempDir::new("mount_and_cp").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| unsafe {
            mount_tempdir_ro(mnt);
        });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");

        let tif_file = "LC80440342017101LGN00_B7.TIF";
        let sub_dir = "LC80440342017101LGN00";
        let full_path = format!("{}/{}/{}", mnt_str, sub_dir, tif_file);
        let dst_path = format!(
            "{}/{}",
            dirs::home_dir().unwrap().to_str().unwrap(),
            tif_file
        );

        let stat_time = std::time::Instant::now();
        info!("Calling stat to trigger init");
        run_stat(&full_path, &mnt_str);
        info!("stat completed in {:#?}", stat_time.elapsed());

        let now = std::time::Instant::now();
        run_cp(&full_path, &dst_path, &mnt_str);

        println!("cp took {:#?}", now.elapsed());
        drop(daemon);
    }

    #[test]
    #[ignore]
    // This test also copies the whole 70MB file, but via dd.
    fn test_dd<'a>() {
        init();
        let dir = TempDir::new("test_dd").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| unsafe {
            mount_tempdir_ro(mnt);
        });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");

        let tif_file = "LC80440342017101LGN00_B7.TIF";
        let sub_dir = "LC80440342017101LGN00";
        let full_path = format!("{}/{}/{}", mnt_str, sub_dir, tif_file);

        let dst_path = format!(
            "{}/{}",
            dirs::home_dir().unwrap().to_str().unwrap(),
            tif_file
        );

        let stat_time = std::time::Instant::now();
        info!("Calling stat to trigger init");
        run_stat(&full_path, &mnt_str);
        info!("stat completed in {:#?}", stat_time.elapsed());

        let now = std::time::Instant::now();
        run_dd(&full_path, &dst_path, 1 * 1024 * 1024, &mnt_str);

        println!("dd took {:#?}", now.elapsed());
        drop(daemon);
    }
}
