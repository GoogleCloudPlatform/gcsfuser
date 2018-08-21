use fuse::{FileType, FileAttr, Filesystem, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry};
use fuse::FUSE_ROOT_ID;
use libc::{ENOENT};
use std::collections::HashMap;
use std::ffi::OsStr;
use time::Timespec;

use super::Object;


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
}

impl GCSFS {
    // TODO(boulos): Take the bucket, prefix, maybe even auth token?
    pub fn new() -> Self {
        info!("Made a GCSFS!");
        GCSFS {
            inode_to_attr: HashMap::new(),
            inode_to_obj: HashMap::new(),
            directory_map: HashMap::new(),
            inode_map: HashMap::new(),
            inode_counter: 0,
        }
    }
}

impl Filesystem for GCSFS {
    fn init(&mut self, _req: &Request) -> Result<(), i32> {
        info!("init!");
        debug!("debug_logger: init!");
        let root_time: Timespec = Timespec { sec: 1534812086, nsec: 0 };    // 2018-08-20 15:41 Pacific

        let root_inode: FileAttr = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: root_time,
            mtime: root_time,
            ctime: root_time,
            crtime: root_time,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
        };

        self.inode_to_attr.insert(1, root_inode);
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: Inode, name: &OsStr, reply: ReplyEntry) {
        info!("lookup(parent={}, name={})", parent, name.to_str().unwrap());

        if let Some(dir_ent) = self.directory_map.get(&parent) {
            // TODO(boulos): Is this the full name, or just the portion? (I believe just portion)
            let search_name = name.to_str().unwrap().to_string();
            for child_pair in dir_ent.entries.iter() {
                if child_pair.0 == search_name {
                    if let Some(attr) = self.inode_to_attr.get(&child_pair.1) {
                        // Found it! Return the info for the inode.
                        reply.entry(&TTL_30s, &attr, 0);
                        return;
                    }
                }
            }
        }

        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, inode: Inode, reply: ReplyAttr) {
        info!("Trying to getattr() on inode {}", inode);

        if let Some(attr) = self.inode_to_attr.get(&inode) {
            reply.attr(&TTL_30s, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn read(&mut self, _req: &Request, inode: Inode, _fh: u64, offset: i64, _size: u32, reply: ReplyData) {
        info!("Trying to read() {} on {} at offset {}", _size, inode, offset);
        if let Some(obj) = self.inode_to_obj.get(&inode) {
            reply.data(super::bucket::get_bytes(obj, offset as u64, _size as u64).unwrap_or(vec![]).as_slice());
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, inode: Inode, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        info!("Trying to readdir on {}", inode);
        if let Some(dir_ent) = self.directory_map.get(&inode) {
            for (idx, ref child_pair) in dir_ent.entries.iter().skip(offset as usize).enumerate() {
                if let Some(child_ent) = self.inode_to_attr.get(&child_pair.1) {
                    reply.add(child_pair.1, (idx + (offset as usize)) as i64, child_ent.kind, &child_pair.0);
                } else {
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
        info!("about to run ls -la in cwd: {}", cwd);

        let output = Command::new("ls")
            .arg("-l")
            .arg("-a")
            .current_dir(cwd)
            .output()
            .expect("ls failed");

        info!("status: {}", output.status);
        info!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());
    }

    pub unsafe fn mount_tempdir_ro<'a>(mountpoint: PathBuf) {
        let fs = GCSFS::new();

        info!("Attempting to mount gcsfs @ {}", mountpoint.to_str().unwrap());
        let options = ["-o", "rw", "-o", "auto_umount", /*"-o", "fsname=gcsfs", "-o", "big_writes" */]
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

        let mut buffer = String::new();
        let to_open = format!("{}/{}", mnt_str, "hello.txt");
        info!("Try to open '{}'", to_open);
        File::open(to_open).unwrap().read_to_string(&mut buffer);
        info!("Reading hello.txt for fun: {}", buffer);
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
        drop(fs);
    }
}
