use anyhow::Result;
use linked_hash_map::LinkedHashMap as HashMap;
use mdsn::Dsn;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AddTagOpts {
    Value { value: String },
    Template { template: String },
}

impl AddTagOpts {
    pub fn value(value: impl Into<String>) -> Self {
        Self::Value {
            value: value.into(),
        }
    }
    pub fn template(template: impl Into<String>) -> Self {
        Self::Template {
            template: template.into(),
        }
    }
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RenameOpts {
    Prefix { prefix: String },
    Suffix { suffix: String },
    Template { template: String },
}

impl RenameOpts {
    pub fn prefix(prefix: impl Into<String>) -> Self {
        Self::Prefix {
            prefix: prefix.into(),
        }
    }
    pub fn suffix(input: impl Into<String>) -> Self {
        Self::Suffix {
            suffix: input.into(),
        }
    }
    pub fn template(input: impl Into<String>) -> Self {
        Self::Template {
            template: input.into(),
        }
    }
}

fn default_tag_length() -> usize {
    100
}

#[derive(Debug, Deserialize)]
pub struct AddTag {
    pub name: String,
    #[serde(flatten)]
    pub opts: AddTagOpts,

    #[serde(default = "default_tag_length")]
    pub len: usize,
}
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Select {
    Subset { subset: Vec<String> },
    Exclude { exclude: Vec<String> },
    Rename { rename: HashMap<String, String> },
}

impl Select {
    pub fn subset(subset: Vec<String>) -> Self {
        Self::Subset { subset }
    }

    pub fn exclude(exclude: Vec<String>) -> Self {
        Self::Exclude { exclude }
    }

    pub fn rename<I: IntoIterator<Item = (String, String)>>(rename: I) -> Self {
        Self::Rename {
            rename: rename.into_iter().collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action")]
pub enum Action {
    Select(Select),
    AddTag(AddTag),
    RenameTable(RenameOpts),
    RenameChildTable(RenameOpts),
    RenameSuperTable(RenameOpts),
}

pub trait Transform {
    fn transform(&mut self, action: &Action) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {

    use super::*;
    use anyhow::Result;
    #[test]
    fn action() -> Result<()> {
        let json = r#"[
            { "action": "AddTag", "name": "f1", "value": "f2" },
            { "action": "AddTag", "name": "f1", "template": "{{ host }}" },
            { "action": "Select", "subset": ["a", "b"] },
            { "action": "Select", "rename": { "a": "a1", "b": "b1" } }
        ]"#;
        let addtag: Vec<Action> = serde_json::from_str(json)?;
        dbg!(addtag);
        Ok(())
    }
}
