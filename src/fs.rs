use fuser::{FileType, FileAttr, Filesystem, KernelConfig, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry};
use fuser::FUSE_ROOT_ID;
use libc::{EIO, ENOENT};
use rayon::scope;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{sleep};
use std::time;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::Object;
use crate::bucket::list_objects;


pub type Inode = u64;

// TODO(boulos): What's a reasonable TTL? Since we're focused on read-only, let's set at least 30s.
const TTL_30s: Duration = Duration::from_secs(30);

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

    // From string => inode for any entry.
    inode_map: RwLock<HashMap<String, Inode>>,

    // Sigh. It'd be nice to just be able to consistently generate an
    // inode from a path...
    inode_counter: Mutex<Inode>,

    // GCS configuration
    base_object_url: String,
    gcs_prefix: Option<String>,

    // Persistent client
    gcs_client: super::bucket::GcsHttpClient,

    // And our runtime for waiting out async.
    tokio_rt: tokio::runtime::Runtime,
}

impl GCSFS {
    // TODO(boulos): Take the bucket, prefix, maybe even auth token?
    pub fn new(object_url: String, prefix: Option<String>) -> Self {
        info!("Making a GCSFS!");
        GCSFS {
            inode_to_attr: RwLock::new(HashMap::new()),
            inode_to_obj: RwLock::new(HashMap::new()),
            directory_map: RwLock::new(HashMap::new()),
            inode_map: RwLock::new(HashMap::new()),
            inode_counter: Mutex::new(0),
            base_object_url: object_url,
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

    fn load_file(&self, full_path: String, obj: Object) -> Inode {
        let inode = self.get_inode();

        debug!("   GCSFS. Loading {}", full_path);

	let mtime: SystemTime = obj.updated.into();
	let ctime: SystemTime = obj.time_created.into();
	// GCS doesn't have atime, use mtime
	let atime = mtime;

        let file_attr: FileAttr = FileAttr {
            ino: inode,
            size: obj.size,
            blocks: 1 /* grr. obj.size / blksize? */,
            atime: atime,
            mtime: mtime,
            ctime: ctime,
            crtime: ctime,
            kind: FileType::RegularFile,
            perm: 0o755,
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
	    blksize: 512,
	    padding: 0,
        };

        self.inode_to_attr.write().unwrap().insert(inode, file_attr);
        self.inode_to_obj.write().unwrap().insert(inode, obj);
        self.inode_map.write().unwrap().insert(full_path, inode);

        return inode;
    }

    // Given a bucket, and a prefix (the directory), load it
    fn load_dir(&self, prefix: Option<String>, parent: Option<Inode>) -> Inode {
        // TODO(boulos): Gross. Because load_dir is mutable (it's
        // changing the struct!) and there's no way to mark the config
        // as immutable, I have to lie or make a copy.
        let bucket_url = self.base_object_url.clone();
        let dir_inode = self.get_inode();

        let prefix_clone = prefix.clone();

        let prefix_for_load: String = match prefix {
            Some(prefix_str) => prefix_str,
            None => String::from(""),
        };

        debug!("   GCSFS. DIR {}", prefix_for_load);

        // Always use / as delim.
        let (single_level_objs, subdirs) = list_objects(bucket_url.as_ref(), prefix_clone.as_ref().map(String::as_str), Some("/")).unwrap();

	let dir_time: SystemTime = UNIX_EPOCH + Duration::new(1534812086, 0);    // 2018-08-20 15:41 Pacific

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
	    blksize: 512,
	    padding: 0,
        };

        self.inode_to_attr.write().unwrap().insert(dir_inode, dir_attr);

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
        inodes.write().unwrap().resize_with(subdir_len, Default::default);

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
                        println!("ERROR: Tried to write inode '{}' to index {} \
                                 for directory {} (subdirs has len {})",
                                 inode, i, dir, subdir_len);
                    }
                });
            }
        });

        for (i, dir) in subdirs.iter().enumerate() {
            // To insert the "directory name", we get the basedir and
            // strip the trailing slash.
            let last_slash = dir.len() - 1;
            let dir_str = dir[base_dir_index..last_slash].to_string();
            let cloned = Arc::clone(&inodes);
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

        debug!("  Created dir_entries: {:#?}", dir_entries);

        self.directory_map.write().unwrap().insert(dir_inode, PsuedoDir {
            name: prefix_for_load.to_string(),
            entries: dir_entries,
        });

        return dir_inode
    }

}

impl Filesystem for GCSFS {

    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), i32> {
        info!("init!");
        debug!("debug_logger: init!");

        let prefix = self.gcs_prefix.clone();

        // Trigger a load from the root of the bucket
        let root_inode = self.load_dir(prefix, None);
        self.inode_map.write().unwrap().insert(".".to_string(), root_inode);

        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: Inode, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent={}, name={})", parent, name.to_str().unwrap());

        if let Some(dir_ent) = self.directory_map.read().unwrap().get(&parent) {
            // TODO(boulos): Is this the full name, or just the portion? (I believe just portion)
            let search_name = name.to_str().unwrap().to_string();
            for child_pair in dir_ent.entries.iter() {
                debug!("  Is search target '{}' == dir_entry '{}'?", search_name, child_pair.0);
                if child_pair.0 == search_name {
                    if let Some(attr) = self.inode_to_attr.read().unwrap().get(&child_pair.1) {
                        // Found it! Return the info for the inode.
                        debug!("  Found it! search target '{}' is inode {}", child_pair.0, child_pair.1);
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

    fn read(&mut self, _req: &Request, inode: Inode, _fh: u64, offset: i64, _size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        debug!("Trying to read() {} on {} at offset {}", _size, inode, offset);
        if let Some(obj) = self.inode_to_obj.read().unwrap().get(&inode) {
            debug!("  Performing read for obj: {:#?}", obj);
            let result = self.tokio_rt.block_on(async {
		super::bucket::get_bytes_with_client(&self.gcs_client,
						     obj,
						     offset as u64,
						     _size as u64).await
	    });

	    match result {
		Ok(bytes) => {
		    reply.data(&bytes);
		},
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

    fn readdir(&mut self, _req: &Request, inode: Inode, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        debug!("Trying to readdir on {} with offset {}", inode, offset);
        if let Some(dir_ent) = self.directory_map.read().unwrap().get(&inode) {
            debug!("  directory {} has {} entries ({:#?})", inode, dir_ent.entries.len(), dir_ent.entries);
            let mut absolute_index = offset + 1;
            for (idx, ref child_pair) in dir_ent.entries.iter().skip(offset as usize).enumerate() {
                debug!("    looking at entry {}, got back pair {:#?}", idx, child_pair);

                if let Some(child_ent) = self.inode_to_attr.read().unwrap().get(&child_pair.1) {
                    debug!("  readdir for inode {}, adding '{}' as inode {}", inode, child_pair.0, child_pair.1);
                    if reply.add(child_pair.1, absolute_index as i64, child_ent.kind, &child_pair.0) {
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
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
    extern crate tempdir;

    use super::*;
    use std;
    use std::env;
    use std::fs::File;
    use std::io;
    use std::io::Read;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Once;
    use std::thread;
    use std::time;
    use tempdir::TempDir;

    static START: Once = Once::new();

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

    pub unsafe fn mount_bucket<'a>(object_url: String,
				   prefix: Option<String>,
				   mountpoint: String,
				   read_only: bool) {
        let fs = GCSFS::new(object_url, prefix);

        info!("Attempting to mount gcsfs @ {}", mountpoint);
        let options = ["-o", "rw",
		       "-o", "auto_unmount",
                       "-o", "noatime",
		       "-o", "iosize=33554432" /* 32MB */,
                       "-o", "max_read=33554432",
                       "-o", "max_readahead=8388608" /* 8 MB readahead */,
		       "-o", "big_writes",
		       "-o", "fsname=gcsfs",
                       /* "-o", "noappledouble" /* Disable ._. and .DS_Store files */ */
		       ]
            .iter()
            .map(|o| o.as_ref())
            .collect::<Vec<&OsStr>>();

        fuser::mount(fs, &mountpoint, &options).unwrap();
        panic!("We should never get here, right...?");
    }
				   

    pub unsafe fn mount_tempdir_ro<'a>(mountpoint: PathBuf) {
        let object_url = "https://www.googleapis.com/storage/v1/b/gcp-public-data-landsat/o";
        // Simple single dir.
        //let prefix = "LC08/PRE/044/034/LC80440342017101LGN00/";
        // One level up to test subdir loading.
        let prefix = "LC08/PRE/044/034/";
	
	mount_bucket(object_url.to_string(),
		     Some(prefix.to_string()),
		     mountpoint.to_str().unwrap().to_string(),
		     true);
    }

    pub unsafe fn mount_tempdir_rw<'a>(mountpoint: PathBuf) {
        let object_url = "https://www.googleapis.com/storage/v1/b/boulos-rustgcs/o";

	mount_bucket(object_url.to_string(),
		     None,
		     mountpoint.to_str().unwrap().to_string(),
		     false);
    }
	

    #[test]
    fn just_mount<'a>() {
        START.call_once(|| {
            env_logger::init();
        });

        let dir = TempDir::new("just_mount").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| { unsafe { mount_tempdir_ro(mnt); } });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);
    }

    #[test]
    fn mount_and_read<'a>() {
        START.call_once(|| {
            env_logger::init();
        });

        let dir = TempDir::new("mount_and_read").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| { unsafe { mount_tempdir_ro(mnt); } });

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
    #[ignore] // Not ready yet!
    fn mount_and_write<'a>() {
        START.call_once(|| {
            env_logger::init();
        });

        let dir = TempDir::new("mount_and_write").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| { unsafe { mount_tempdir_rw(mnt); } });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");

        let txt_file = "mount_and_write.txt";
        let to_open = format!("{}/{}", mnt_str, txt_file);
        info!("Try to open '{}'", to_open);
        let result = fs::write(to_open, "My first words!");
        info!(" got back {:#?}", result);
        drop(daemon);
    }
    

    #[test]
    fn mount_and_ls<'a>() {
        START.call_once(|| {
            env_logger::init();
        });

        // Mount the filesystem and run ls.
        info!("Running mount_and_ls");

        let dir = TempDir::new("mount_and_ls").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());

        let fs = thread::spawn(|| { unsafe { mount_tempdir_ro(mnt); } });
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
    fn mount_and_cp<'a>() {
        START.call_once(|| {
            env_logger::init();
        });

        let dir = TempDir::new("mount_and_cp").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| { unsafe { mount_tempdir_ro(mnt); } });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(Duration::from_millis(250));
        info!("Awake!");


        let tif_file = "LC80440342017101LGN00_B7.TIF";
        let sub_dir = "LC80440342017101LGN00";
        let full_path = format!("{}/{}/{}", mnt_str, sub_dir, tif_file);
	let dst_path = format!("{}/{}", dirs::home_dir().unwrap().to_str().unwrap(), tif_file);

        let stat_time = std::time::Instant::now();
        info!("Calling stat to trigger init");
        run_stat(&full_path, &mnt_str);
        info!("stat completed in {:#?}", stat_time.elapsed());

        let now = std::time::Instant::now();
        run_cp(&full_path, &dst_path, &mnt_str);

        println!("cp took {:#?}", now.elapsed());
        drop(daemon);
    }
}
