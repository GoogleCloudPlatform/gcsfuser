use fuse::{FileType, FileAttr, Filesystem, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry};
use fuse::FUSE_ROOT_ID;
use libc::{ENOENT};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use time::Timespec;

use super::Object;
use bucket::list_objects;

pub type Inode = u64;

// TODO(boulos): What's a reasonable TTL? Since we're focused on read-only, let's set at least 30s.
const TTL_30s: Timespec = Timespec { sec: 30, nsec: 0 };

struct PsuedoDir {
    name: String,
    // inline
    entries: Vec<(String, Inode)>,
}

// TODO(boulos): Decide if we just use BTreeMap rather than HashMaps and Vecs...
pub struct GCSFS {
    // Inode => Attr
    inode_to_attr: HashMap<Inode, FileAttr>,

    // From inode to raw GCS Object.
    inode_to_obj: HashMap<Inode, Object>,
    // From inode => the path name
    directory_map: HashMap<Inode, PsuedoDir>,

    // From string => inode for any entry.
    inode_map: HashMap<String, Inode>,

    // Sigh. It'd be nice to just be able to consistently generate an
    // inode from a path...
    inode_counter: Inode,

    // GCS configuration
    base_object_url: String,
    gcs_prefix: Option<String>,
}

impl GCSFS {
    // TODO(boulos): Take the bucket, prefix, maybe even auth token?
    pub fn new(object_url: String, prefix: Option<String>) -> Self {
        info!("Making a GCSFS!");
        GCSFS {
            inode_to_attr: HashMap::new(),
            inode_to_obj: HashMap::new(),
            directory_map: HashMap::new(),
            inode_map: HashMap::new(),
            inode_counter: 0,
            base_object_url: object_url,
            gcs_prefix: prefix,
        }
    }

    fn get_inode(&mut self) -> Inode {
        // Grab an inode. We pre-increment so that the root inode gets 1.
        self.inode_counter += 1;
        return self.inode_counter;
    }

    fn load_file(&mut self, full_path: String, obj: Object) -> Inode {
        let inode = self.get_inode();

        debug!("   GCSFS. Loading {}", full_path);

        let file_time: Timespec = Timespec { sec: 1534812086, nsec: 0 };    // 2018-08-20 15:41 Pacific

        let file_attr: FileAttr = FileAttr {
            ino: inode,
            size: obj.size,
            blocks: 0 /* do I need this? */,
            atime: file_time,
            mtime: file_time,
            ctime: file_time,
            crtime: file_time,
            kind: FileType::RegularFile,
            perm: 0o755,
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
        };

        self.inode_to_attr.insert(inode, file_attr);
        self.inode_to_obj.insert(inode, obj);
        self.inode_map.insert(full_path, inode);

        return inode;
    }

    // Given a bucket, and a prefix (the directory), load it
    fn load_dir(&mut self, bucket_url: &str, prefix: Option<&str>, parent: Option<Inode>) -> Inode {
        let dir_inode = self.get_inode();

        let prefix_for_load = match prefix {
            Some(prefix_str) => prefix_str,
            None => "",
        };

        debug!("   GCSFS. DIR {}", prefix_for_load);

        // Always use / as delim.
        let (single_level_objs, subdirs) = list_objects(bucket_url, prefix, Some("/")).unwrap();

        let dir_time: Timespec = Timespec { sec: 1534812086, nsec: 0 };    // 2018-08-20 15:41 Pacific

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
        };

        self.inode_to_attr.insert(dir_inode, dir_attr);

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

        // Loop over all the subdirs, recursively load them.
        for dir in subdirs {
            // To insert the "directory name", we get the basedir and
            // strip the trailing slash.
            let last_slash = dir.len() - 1;
            let dir_str = dir[base_dir_index..last_slash].to_string();

            let inode = self.load_dir(bucket_url, Some(&dir), Some(dir_inode));
            dir_entries.push((dir_str, inode));
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

        self.directory_map.insert(dir_inode, PsuedoDir {
            name: prefix_for_load.to_string(),
            entries: dir_entries,
        });

        return dir_inode
    }

}

impl Filesystem for GCSFS {

    fn init(&mut self, _req: &Request) -> Result<(), i32> {
        info!("init!");
        debug!("debug_logger: init!");

        // TODO(boulos): Gross. Because load_dir is mutable (it's
        // changing the struct!) and there's no way to mark the config
        // as immutable, I have to lie or make a copy.
        let obj_url = self.base_object_url.clone();
        let prefix = self.gcs_prefix.clone();
        let prefix_str = prefix.as_ref().map(String::as_str);

        // Trigger a load from the root of the bucket
        let root_inode = self.load_dir(obj_url.as_ref(), prefix_str, None);
        self.inode_map.insert(".".to_string(), root_inode);

        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: Inode, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent={}, name={})", parent, name.to_str().unwrap());

        if let Some(dir_ent) = self.directory_map.get(&parent) {
            // TODO(boulos): Is this the full name, or just the portion? (I believe just portion)
            let search_name = name.to_str().unwrap().to_string();
            for child_pair in dir_ent.entries.iter() {
                debug!("  Is search target '{}' == dir_entry '{}'?", search_name, child_pair.0);
                if child_pair.0 == search_name {
                    if let Some(attr) = self.inode_to_attr.get(&child_pair.1) {
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

        if let Some(attr) = self.inode_to_attr.get(&inode) {
            reply.attr(&TTL_30s, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn read(&mut self, _req: &Request, inode: Inode, _fh: u64, offset: i64, _size: u32, reply: ReplyData) {
        debug!("Trying to read() {} on {} at offset {}", _size, inode, offset);
        if let Some(obj) = self.inode_to_obj.get(&inode) {
            debug!("  Performing read for obj: {:#?}", obj);
            let bytes = super::bucket::get_bytes(obj, offset as u64, _size as u64).unwrap_or(vec![]);
            reply.data(bytes.as_slice());
        } else {
            debug!("  read FAILED, inode {} didn't return a GCS Object", inode);
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, inode: Inode, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        debug!("Trying to readdir on {} with offset {}", inode, offset);
        if let Some(dir_ent) = self.directory_map.get(&inode) {
            debug!("  directory {} has {} entries ({:#?})", inode, dir_ent.entries.len(), dir_ent.entries);
            let mut absolute_index = offset + 1;
            for (idx, ref child_pair) in dir_ent.entries.iter().skip(offset as usize).enumerate() {
                debug!("    looking at entry {}, got back pair {:#?}", idx, child_pair);

                if let Some(child_ent) = self.inode_to_attr.get(&child_pair.1) {
                    debug!("  readdir for inode {}, adding '{}' as inode {}", inode, child_pair.0, child_pair.1);
                    reply.add(child_pair.1, absolute_index as i64, child_ent.kind, &child_pair.0);
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
    extern crate fuse;
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

        let output = Command::new("cp")
            .arg(src_path)
            .arg(dst_path)
            .current_dir(cwd)
            .output()
            .expect("cp failed");

        info!("status: {}", output.status);
        info!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());
    }

    pub unsafe fn mount_tempdir_ro<'a>(mountpoint: PathBuf) {
        let object_url = "https://www.googleapis.com/storage/v1/b/gcp-public-data-landsat/o";
        // Simple single dir.
        //let prefix = "LC08/PRE/044/034/LC80440342017101LGN00/";
        // One level up to test subdir loading.
        let prefix = "LC08/PRE/044/034/";

        let fs = GCSFS::new(object_url.to_string(), Some(prefix.to_string()));

        info!("Attempting to mount gcsfs @ {}", mountpoint.to_str().unwrap());
        let options = ["-o", "rw", "-o", "auto_umount", "-o", "iosize=4194304", /*"-o", "fsname=gcsfs", "-o", "big_writes" */]
            .iter()
            .map(|o| o.as_ref())
            .collect::<Vec<&OsStr>>();

        fuse::mount(fs, &mountpoint, &options).unwrap();
        panic!("We should never get here, right...?");
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
    fn mount_and_open<'a>() {
        START.call_once(|| {
            env_logger::init();
        });

        let dir = TempDir::new("mount_and_open").unwrap();
        let mnt = dir.into_path();
        let mnt_str = String::from(mnt.to_str().unwrap());
        let daemon = thread::spawn(|| { unsafe { mount_tempdir_ro(mnt); } });

        info!("mounted fs at {} in thread {:#?}", mnt_str, daemon);

        info!("Sleeping for 250ms, to wait for the FS to be ready, because shitty");
        std::thread::sleep(time::Duration::from_millis(250));
        info!("Awake!");

        let txt_file = "LC80440342017101LGN00/LC80440342017101LGN00_MTL.txt";
        let to_open = format!("{}/{}", mnt_str, txt_file);
        info!("Try to open '{}'", to_open);
        let result = fs::read_to_string(to_open).unwrap();
        info!(" got back {}", result);
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
        std::thread::sleep(time::Duration::from_millis(250));
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
        std::thread::sleep(time::Duration::from_millis(250));
        info!("Awake!");

        let tif_file = "LC80440342017101LGN00_B7.TIF";
        let sub_dir = "LC80440342017101LGN00";
        let full_path = format!("{}/{}/{}", mnt_str, sub_dir, tif_file);
        let dst_path = format!("/Users/boulos/{}", tif_file);
        run_cp(&full_path, &dst_path, &mnt_str);
        drop(daemon);
    }
}
