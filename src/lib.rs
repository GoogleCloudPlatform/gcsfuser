#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;

#[macro_use] extern crate log;

extern crate env_logger;

extern crate url;

extern crate reqwest;

extern crate libc;
extern crate rayon;
extern crate time;
extern crate tempdir;

mod bucket;
mod fs;
//mod object;

pub use self::bucket::Bucket;
pub use self::bucket::Object;
pub use self::fs::GCSFS;
//pub use self::bucket::GetObject;
//pub use self::object::Object;
