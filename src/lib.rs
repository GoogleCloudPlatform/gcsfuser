#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;

extern crate url;

extern crate reqwest;

mod bucket;
//mod object;

pub use self::bucket::Bucket;
pub use self::bucket::Object;
//pub use self::bucket::GetObject;
//pub use self::object::Object;

