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

mod bucket;
mod fs;
//mod object;

pub use self::bucket::Bucket;
pub use self::bucket::Object;
pub use self::fs::GCSFS;
//pub use self::bucket::GetObject;
//pub use self::object::Object;
