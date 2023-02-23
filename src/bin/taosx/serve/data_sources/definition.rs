use serde::{Deserialize, Serialize};

use taos::{Dsn, IntoDsn};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ProtocolItem {
    pub name: String,
    #[serde(skip_serializing_if = "bool_is_false", default)]
    pub default: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub description: Option<String>,
}

/// Additionally protocol settings.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct Protocol {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub choices: Vec<ProtocolItem>,
    /// Current value of protocol, it must be one of [ProtocolItem::name].
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum DataSourceType {
    Uri,
    Path,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
#[serde(default)]
pub struct OptionDef {
    #[serde(skip_serializing_if = "bool_is_false")]
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(rename = "snake_case", untagged)]
pub enum DataSourceOptions {
    Path {
        path: OptionDef,
    },
    Uri {
        #[serde(default)]
        host: OptionDef,
        #[serde(default)]
        port: OptionDef,
        #[serde(default)]
        username: OptionDef,
        #[serde(default)]
        password: OptionDef,
        #[serde(default)]
        subject: OptionDef,
    },
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct Param {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<Hint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(untagged)]
pub enum Hint {
    Named(String),
    Flat {
        r#type: String,
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        choices: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct GroupedParams {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_order: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub params: Vec<Param>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct Authentication {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct HintDefinition {
    pub name: String,
    pub r#type: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub choices: Vec<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct Definitions {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub hints: Vec<HintDefinition>,
}

impl Definitions {
    pub fn is_none(&self) -> bool {
        self.hints.is_empty()
    }
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct DataSourceDefinition {
    /// Data source driver id
    id: String,

    /// Type for DSN parser.
    r#type: DataSourceType,

    /// Data source driver name.
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    /// Data source description in markdown format.
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    /// Allow custom parameters.
    #[serde(skip_serializing_if = "bool_is_false")]
    #[serde(default)]
    strict: bool,
    /// Options for specified type.
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<DataSourceOptions>,
    /// Protocol list.
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<Protocol>,

    /// Authentication settings.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    authentication: Vec<Authentication>,

    /// Grouped parameters.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    groups: Vec<GroupedParams>,

    /// Ungrouped parameters.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    params: Vec<Param>,

    /// Schema definitions, not used currently.
    #[serde(skip_serializing_if = "Definitions::is_none", default)]
    definitions: Definitions,
}

impl DataSourceDefinition {
    // todo: parse values from DSN.
    pub fn values_from(&mut self, _dsn: &Dsn) {}
}

#[test]
fn test() {
    let json = include_str!("tmq.json");
    let def: Vec<DataSourceDefinition> = serde_json::from_str(json).unwrap();
    let json2 = serde_json::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def[0]).unwrap();
    println!("{}", &toml);
}

const fn bool_is_false(v: &bool) -> bool {
    !*v
}
