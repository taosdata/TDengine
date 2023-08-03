use std::{hash::Hash, str::FromStr};

use linked_hash_map::LinkedHashMap as HashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use taos::{Field, JsonMeta, MetaCreate, MetaDrop, TagWithValue, Ty};

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
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

const fn default_tag_length() -> usize {
    100
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct AddTag {
    pub name: String,
    #[serde(flatten)]
    pub opts: AddTagOpts,

    #[serde(default = "default_tag_length")]
    pub len: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum AddTagParseError {
    #[error("Empty AddTag option")]
    Empty,
    #[error("Invalid AddTag option: {0}")]
    Invalid(String),
}

impl FromStr for AddTag {
    type Err = AddTagParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(AddTagParseError::Empty);
        }
        use lazy_static::lazy_static;

        lazy_static! {
            static ref RE: Regex =
                Regex::new(r"(?P<f>[^=()\s]+)(\((?P<l>\d+)\))?=((?P<t>.*\{\{.*\}\}.*)|(?P<v>.*))")
                    .unwrap();
        }
        // RE.matches(s).into_iter()
        if let Some(cap) = RE.captures(s) {
            let name = cap["f"].to_string();
            let len = cap
                .name("l")
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or_else(default_tag_length);
            if let Some(v) = cap.name("v") {
                Ok(AddTag {
                    name,
                    len,
                    opts: AddTagOpts::value(v.as_str()),
                })
            } else if let Some(t) = cap.name("t") {
                Ok(AddTag {
                    name,
                    len,
                    opts: AddTagOpts::template(t.as_str()),
                })
            } else {
                unreachable!()
            }
        } else {
            Err(AddTagParseError::Invalid(s.to_string()))
        }
    }
}

#[test]
fn parse_add_tag() {
    let errors = [
        ("", AddTagParseError::Empty),
        ("a", AddTagParseError::Invalid("a".to_string())),
    ];
    for (s, e) in errors {
        let ee = AddTag::from_str(s).unwrap_err();
        assert_eq!(ee.to_string(), e.to_string());
    }

    let add_tags = [
        (
            "f1=v1",
            AddTag {
                name: "f1".to_string(),
                opts: AddTagOpts::Value {
                    value: "v1".to_string(),
                },
                len: 100,
            },
        ),
        (
            "f1(200)=v1",
            AddTag {
                name: "f1".to_string(),
                opts: AddTagOpts::Value {
                    value: "v1".to_string(),
                },
                len: 200,
            },
        ),
        (
            "f1(200)={{ host }}",
            AddTag {
                name: "f1".to_string(),
                opts: AddTagOpts::Template {
                    template: "{{ host }}".to_string(),
                },
                len: 200,
            },
        ),
    ];
    for (s, a) in add_tags {
        let aa = AddTag::from_str(s).unwrap();
        assert_eq!(aa, a);
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum RenameOpts {
    Prefix { prefix: String },
    Suffix { suffix: String },
    Template { template: String },
    // ReplaceWithRegex { regex: String, replace_with: String, }
    ReplaceWithRegex { config: String, }
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

    pub fn replace_with_regex(input: impl Into<String>) -> Self {
        Self::ReplaceWithRegex { config: input.into() }
    }

    pub fn apply(&self, name: &str) -> String {
        match self {
            RenameOpts::Prefix { prefix } => format!("{prefix}{name}"),
            RenameOpts::Suffix { suffix } => format!("{name}{suffix}"),
            RenameOpts::Template { template } => template
                .replace("{ ", "{")
                .replace(" }", "}")
                .replace("{name}", name),
            RenameOpts::ReplaceWithRegex { config } => {
                // TODO should return result
                let split: Vec<&str> = config.split("::").collect();
                // size should be 2
                // TODO escape
                let regex = Regex::new(split.get(0).unwrap()).unwrap();
                regex.replace_all(name, split.get(1).unwrap().to_string()).to_string()
            }
        }
    }

    pub fn apply_in_place(&self, name: &mut String) {
        let new = self.apply(&name);
        name.clear();
        name.extend(new.chars());
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RenameParseError {
    #[error("Empty rename option is not valid")]
    Empty,
    #[error("Empty rename option for variant: {0}")]
    EmptyOptionForVariant(&'static str),
    #[error(
        "Invalid rename option: {0} which should match pattern `<prefix|suffix|template>:<value>`"
    )]
    FormatError(String),
    #[error("Invalid rename variant: {0} while parsing `{1}`")]
    InvalidVariant(String, String),
}

impl FromStr for RenameOpts {
    type Err = RenameParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use RenameParseError::*;
        if s.is_empty() {
            return Err(Empty);
        }
        let (variant, option) = s.split_once(':').ok_or(FormatError(s.to_string()))?;
        match variant {
            "prefix" => {
                if option.is_empty() {
                    Err(EmptyOptionForVariant("prefix"))
                } else {
                    Ok(RenameOpts::prefix(option))
                }
            }
            "suffix" => {
                if option.is_empty() {
                    Err(EmptyOptionForVariant("suffix"))
                } else {
                    Ok(RenameOpts::suffix(option))
                }
            }
            "template" => {
                if option.is_empty() {
                    Err(EmptyOptionForVariant("template"))
                } else {
                    Ok(RenameOpts::template(option))
                }
            }
            "replace_with_regex" => {
                if option.is_empty() {
                    Err(EmptyOptionForVariant("replace_with_regex"))
                } else {
                    Ok(RenameOpts::replace_with_regex(option))
                }
            }
            variant => Err(InvalidVariant(variant.to_string(), s.to_string())),
        }
    }
}

#[test]
fn test_rename_opts_from_str() {
    use RenameParseError::*;
    let errors = [
        ("", Empty),
        ("a", FormatError("a".to_string())),
        ("a:", InvalidVariant("a".to_string(), "a:".to_string())),
        ("prefix:", EmptyOptionForVariant("prefix")),
        ("suffix:", EmptyOptionForVariant("suffix")),
        ("template:", EmptyOptionForVariant("template")),
    ];
    for (s, e) in errors {
        let ee = RenameOpts::from_str(s).unwrap_err();
        assert_eq!(ee.to_string(), e.to_string());
    }

    let actions = [
        ("prefix:v1_", RenameOpts::prefix("v1_")),
        ("suffix:_v1", RenameOpts::suffix("_v1")),
        ("template:v1_v1", RenameOpts::template("v1_v1")),
    ];
    for (s, a) in actions {
        let aa = RenameOpts::from_str(s).unwrap();
        assert_eq!(aa, a);
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
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

    pub fn rename<I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>>(
        rename: I,
    ) -> Self {
        Self::Rename {
            rename: rename
                .into_iter()
                .map(|(a, b)| (a.into(), b.into()))
                .collect(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SelectParseError {
    #[error("Empty select option")]
    Empty,
    #[error("Empty select option for variant: {0}")]
    EmptyOptionForVariant(&'static str),
    #[error("Invalid select option: {0}, use like `select:<type>:<opts>`")]
    InvalidSelect(String),
    #[error("Invalid select variant: {0} while parsing `{1}`")]
    InvalidVariant(String, String),
    #[error("Invalid rename option: {0} while parsing `{1}`")]
    InvalidRename(String, String),
}

impl FromStr for Select {
    type Err = SelectParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(SelectParseError::Empty);
        }
        let (select, fields) = s
            .split_once(':')
            .ok_or(SelectParseError::InvalidSelect(s.to_string()))?;
        match select {
            "subset" => {
                if fields.is_empty() {
                    return Err(SelectParseError::EmptyOptionForVariant("subset"));
                }
                let v = fields.split(",").map(ToString::to_string).collect();
                Ok(Select::subset(v))
            }
            "exclude" => {
                if fields.is_empty() {
                    return Err(SelectParseError::EmptyOptionForVariant("exclude"));
                }
                let v = fields.split(",").map(ToString::to_string).collect();
                Ok(Select::exclude(v))
            }
            "rename" => {
                if fields.is_empty() {
                    return Err(SelectParseError::EmptyOptionForVariant("rename"));
                }
                use itertools::Itertools;
                let v: Vec<_> = fields
                    .split(",")
                    .map(|f| {
                        f.split_once("=")
                            .ok_or(SelectParseError::InvalidRename(
                                f.to_string(),
                                s.to_string(),
                            ))
                            .map(|(f, v)| (f.to_string(), v.to_string()))
                    })
                    .try_collect()?;
                Ok(Select::rename(v))
            }
            t => Err(SelectParseError::InvalidVariant(
                t.to_string(),
                s.to_string(),
            )),
        }
    }
}

#[test]
fn test_select_from_str() {
    let errors = [
        ("", SelectParseError::Empty),
        ("a", SelectParseError::InvalidSelect("a".to_string())),
        (
            "a:",
            SelectParseError::InvalidVariant("a".to_string(), "a:".to_string()),
        ),
        ("subset:", SelectParseError::EmptyOptionForVariant("subset")),
        (
            "exclude:",
            SelectParseError::EmptyOptionForVariant("exclude"),
        ),
        ("rename:", SelectParseError::EmptyOptionForVariant("rename")),
        (
            "rename:a,b",
            SelectParseError::InvalidRename("a".to_string(), "rename:a,b".to_string()),
        ),
    ];
    for (s, e) in errors {
        let ee = Select::from_str(s).unwrap_err();
        assert_eq!(ee.to_string(), e.to_string());
    }

    let selects = [
        (
            "subset:a,b",
            Select::subset(vec!["a".to_string(), "b".to_string()]),
        ),
        (
            "exclude:a,b",
            Select::exclude(vec!["a".to_string(), "b".to_string()]),
        ),
        ("rename:a=b,c=d", Select::rename([("a", "b"), ("c", "d")])),
    ];
    for (s, a) in selects {
        let aa = Select::from_str(s).unwrap();
        assert_eq!(aa, a);
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(tag = "action")]
pub enum Action {
    Select(Select),
    AddTag(AddTag),
    RenameTable(RenameOpts),
    RenameChildTable(RenameOpts),
    RenameSuperTable(RenameOpts),
    // regex will apply to all tables include super table and child table
    RenameReplaceWithRegex(RenameOpts),
}

impl Action {
    fn as_type(&self) -> ActionType {
        match self {
            Action::Select(_) => ActionType::Select,
            Action::AddTag(_) => ActionType::AddTag,
            Action::RenameTable(_) => ActionType::RenameTable,
            Action::RenameChildTable(_) => ActionType::RenameChildTable,
            Action::RenameSuperTable(_) => ActionType::RenameSuperTable,
            Action::RenameReplaceWithRegex(_) => ActionType::RenameReplaceWithRegex,
        }
    }

    pub fn mutate_meta(&self, meta: &mut JsonMeta) -> anyhow::Result<()> {
        let action = self;
        match action {
            Action::Select(_) => {
                anyhow::bail!("unsupported transform action: {:?}", action);
            }
            Action::AddTag(action) => {
                // dbg!(action);
                let len = match action.len {
                    0 => 100,
                    16374.. => 16374,
                    a => a,
                };
                let field = Field::new(&action.name, Ty::VarChar, len as u32);
                match meta {
                    JsonMeta::Create(create) => match create {
                        MetaCreate::Super {
                            table_name: _,
                            columns: _,
                            tags,
                        } => {
                            tags.push(field);
                        }
                        MetaCreate::Child {
                            table_name: _,
                            using: _,
                            tags,
                            tag_num: _,
                        } => {
                            let value = match &action.opts {
                                crate::transform::AddTagOpts::Value { value } => {
                                    serde_json::json!(format!("\"{value}\""))
                                }
                                crate::transform::AddTagOpts::Template { template: _ } => {
                                    anyhow::bail!("unsupported transform action: {:?}", action)
                                }
                            };
                            tags.push(TagWithValue { field, value });
                        }
                        _ => (),
                    },
                    _ => (),
                }
            }
            Action::RenameTable(action) => match meta {
                JsonMeta::Create(create) => match create {
                    MetaCreate::Super {
                        table_name,
                        columns: _,
                        tags: _,
                    } => {
                        let s = action.apply(table_name);
                        table_name.clear();
                        table_name.extend(s.chars());
                    }
                    MetaCreate::Child {
                        table_name,
                        using,
                        tags: _,
                        tag_num: _,
                    } => {
                        // change child table name and super table name.
                        let s = action.apply(table_name);
                        table_name.clear();
                        table_name.extend(s.chars());

                        let s = action.apply(&using);
                        using.clear();
                        using.extend(s.chars());
                    }
                    MetaCreate::Normal {
                        table_name,
                        columns: _,
                    } => {
                        let s = action.apply(table_name);
                        table_name.clear();
                        table_name.extend(s.chars());
                    }
                },
                JsonMeta::Alter(alter) => {
                    let new = action.apply(&alter.table_name);
                    alter.table_name.clear();
                    alter.table_name.extend(new.chars());
                }
                JsonMeta::Drop(drop) => match drop {
                    MetaDrop::Super { table_name } => action.apply_in_place(table_name),
                    MetaDrop::Other { table_name_list } => {
                        for name in table_name_list {
                            action.apply_in_place(name);
                        }
                    }
                },
                JsonMeta::Delete(_) => {
                    // todo: renamed table should be deleted.
                    todo!()
                }
            },
            Action::RenameChildTable(action) => match meta {
                JsonMeta::Create(create) => match create {
                    MetaCreate::Child {
                        table_name,
                        using: _,
                        tags: _,
                        tag_num: _,
                    } => {
                        // dbg!(action, &meta);
                        let s = action.apply(table_name);
                        table_name.clear();
                        table_name.extend(s.chars());
                    }
                    _ => (),
                },
                JsonMeta::Alter(_) => (),
                JsonMeta::Drop(drop) => match drop {
                    MetaDrop::Super { table_name: _ } => (),
                    MetaDrop::Other { table_name_list } => {
                        // todo(@zitsen): normal or child?
                        for name in table_name_list {
                            action.apply_in_place(name);
                        }
                    }
                },
                JsonMeta::Delete(_) => {
                    todo!()
                }
            },
            Action::RenameSuperTable(action) => Action::rename_super_meta(action, meta),
            Action::RenameReplaceWithRegex(action) => Action::rename_super_meta(action, meta),
        }
        Ok(())
    }

    fn rename_super_meta(action: &RenameOpts, meta: &mut JsonMeta) {
        match meta {
            JsonMeta::Create(create) => match create {
                MetaCreate::Super {
                    table_name,
                    columns: _,
                    tags: _,
                } => {
                    let s = action.apply(table_name);
                    table_name.clear();
                    table_name.extend(s.chars());
                }
                MetaCreate::Child {
                    table_name: _,
                    using,
                    tags: _,
                    tag_num: _,
                } => {
                    let s = action.apply(&using);
                    using.clear();
                    using.extend(s.chars());
                }
                _ => (),
            },
            JsonMeta::Alter(alter) => match alter.alter_type {
                taos::AlterType::AddTag => action.apply_in_place(&mut alter.table_name),
                taos::AlterType::DropTag => action.apply_in_place(&mut alter.table_name),
                taos::AlterType::RenameTag => action.apply_in_place(&mut alter.table_name),
                taos::AlterType::SetTagValue => action.apply_in_place(&mut alter.table_name),
                taos::AlterType::AddColumn => (),
                taos::AlterType::DropColumn => (),
                taos::AlterType::ModifyColumnLength => (),
                taos::AlterType::ModifyTagLength => {
                    action.apply_in_place(&mut alter.table_name)
                }
                taos::AlterType::ModifyTableOption => (),
                taos::AlterType::RenameColumn => (),
            },
            JsonMeta::Drop(drop) => match drop {
                MetaDrop::Super { table_name } => {
                    // todo(@zitsen): normal or child?
                    action.apply_in_place(table_name)
                }
                _ => (),
            },
            JsonMeta::Delete(_) => {
                todo!()
            }
        }
    }

}

#[derive(Hash)]
enum ActionType {
    Select,
    AddTag,
    RenameTable,
    RenameChildTable,
    RenameSuperTable,
    RenameReplaceWithRegex,
}

impl Hash for Action {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_type().hash(state);
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ActionParseError {
    #[error("Empty action option is not valid")]
    EmptyAction,
    #[error("Invalid action: {0}, use `<action>:<option>` format")]
    FormatError(String),
    #[error("Unsupported action type: {0}")]
    Unsupported(String),
    #[error("AddTag parse error: {0}")]
    AddTagError(#[from] AddTagParseError),
    #[error("Select parse error: {0}")]
    SelectError(#[from] SelectParseError),
    #[error("Rename parse error: {0}")]
    RenameError(#[from] RenameParseError),
}

impl FromStr for Action {
    type Err = ActionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ActionParseError::*;
        if s.is_empty() {
            return Err(EmptyAction);
        }
        use convert_case::{Case, Casing};
        let (action, option) = s.split_once(":").ok_or(FormatError(s.to_string()))?;
        match action.to_case(Case::Pascal).as_str() {
            "AddTag" => Ok(Action::AddTag(AddTag::from_str(option)?)),
            "Select" => Ok(Action::Select(Select::from_str(option)?)),
            "RenameTable" => Ok(Action::RenameTable(RenameOpts::from_str(option)?)),
            "RenameSuperTable" => Ok(Action::RenameSuperTable(RenameOpts::from_str(option)?)),
            "RenameChildTable" => Ok(Action::RenameChildTable(RenameOpts::from_str(option)?)),
            "RenameReplaceWithRegex" => Ok(Action::RenameReplaceWithRegex(RenameOpts::from_str(option)?)),
            _ => Err(Unsupported(action.to_string())),
        }
    }
}

#[test]
fn test_action_from_str() {
    use ActionParseError::*;
    let errors = [
        ("", EmptyAction),
        ("a", FormatError("a".to_string())),
        ("a:", Unsupported("a".to_string())),
        ("add-tag:", AddTagError(AddTagParseError::Empty)),
        ("select:", SelectError(SelectParseError::Empty)),
        ("rename-table:", RenameError(RenameParseError::Empty)),
    ];
    for (s, e) in errors {
        let ee = Action::from_str(s).unwrap_err();
        assert_eq!(ee.to_string(), e.to_string());
    }

    let actions = [
        (
            "add-tag:a=b",
            Action::AddTag(AddTag {
                name: "a".to_string(),
                len: 100,
                opts: AddTagOpts::value("b"),
            }),
        ),
        (
            "select:subset:a,b",
            Action::Select(Select::subset(vec!["a".to_string(), "b".to_string()])),
        ),
        (
            "rename-table:template:v1_v1",
            Action::RenameTable(RenameOpts::template("v1_v1")),
        ),
    ];
    for (s, a) in actions {
        let aa = Action::from_str(s).unwrap();
        assert_eq!(aa, a);
    }
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

    #[test]
    fn test_trait() {
        struct A;
        impl Transform for A {
            fn transform(&mut self, _: &Action) -> anyhow::Result<()> {
                Ok(())
            }
        }
        let action = Action::from_str("select:subset:a,b,c").unwrap();
        let mut a = A;
        a.transform(&action).unwrap();
    }
}
