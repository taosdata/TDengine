use anyhow::Result;
use linked_hash_map::LinkedHashMap as HashMap;
use mdsn::Dsn;
use serde::{Deserialize, Serialize};

pub struct XSource {
    plugin: String,
    dsn: Dsn,
    options: HashMap<String, String>,
}

pub struct XTransformer {
    pub name: String,
    pub options: HashMap<String, String>,
}

pub struct XSink {
    dsn: Dsn,
    options: HashMap<String, String>,
}
pub struct XStream {
    pub from: Vec<XSource>,
    pub transformer: Vec<XTransformer>,
    pub to: Vec<XSink>,
}

#[cfg(test)]
mod tests {

    use super::*;
    use anyhow::Result;
}
