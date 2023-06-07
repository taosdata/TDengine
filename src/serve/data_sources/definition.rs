use serde::{Deserialize, Serialize};

use taos::Dsn;
use taosx_core::Parser;
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

impl Protocol {
    fn value(mut self, value: String) -> Self {
        self.value.replace(value);
        self
    }
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

impl OptionDef {
    fn value(mut self, value: String) -> Self {
        self.value.replace(value);
        self
    }
}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(rename = "snake_case", untagged)]
pub enum DataSourceOptions {
    Path {
        path: OptionDef,
    },
    Endpoint {
        endpoint: OptionDef,
    },
    Uri {
        #[serde(default)]
        host: OptionDef,
        #[serde(default)]
        port: OptionDef,
        #[serde(default)]
        subject: OptionDef,
    },
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct Param {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<Hint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// Condition for a parameter, eg. "if: protocol.ws"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#if: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternatives: Option<Vec<ParamAlternatives>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Vec<Param>>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ParamAlternatives {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<Hint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<Param>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(untagged)]
pub enum Hint {
    Named(String),
    Flat {
        r#type: String,
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        choices: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<i64>,
    },
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct GroupedParams {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_order: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub collapsible: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub collapsed: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub params: Vec<Param>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
#[serde(default)]
pub struct AuthItem {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<OptionDef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<OptionDef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<Param>,
}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct Authentication {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Authentication item name.
    pub value: Option<String>,
    /// Authentication items.
    pub alternatives: Vec<AuthItem>,
}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct HintDefinition {
    pub name: String,
    pub r#type: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub choices: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
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

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct DatasetsDefinition {
    pub name: String,
    pub description: String,
    pub categories: Vec<DatasetParam>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct DatasetParam {
    pub category: String,
    pub display: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflilcts_with: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<Target>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<Param>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct Target {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub required: bool,
    pub multiple: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub editable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selectable: Option<bool>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct ParserDefinition {
    pub display: String,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<Param>>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode)]
pub struct DataSourceDefinition {
    /// Data source driver id
    pub id: String,

    /// Type for DSN parser.
    pub r#type: DataSourceType,

    /// Data source driver name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Data source description in markdown format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Allow custom parameters.
    #[serde(skip_serializing_if = "bool_is_false")]
    #[serde(default)]
    pub strict: bool,
    /// Options for specified type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<DataSourceOptions>,
    /// Protocol list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,

    /// Authentication settings.
    #[serde(default)]
    pub authentication: Authentication,

    /// Grouped parameters.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub groups: Vec<GroupedParams>,

    /// Ungrouped parameters.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub params: Vec<Param>,

    /// Schema definitions, not used currently.
    #[serde(skip_serializing_if = "Definitions::is_none", default)]
    pub definitions: Definitions,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<DatasetsDefinition>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parser: Option<ParserDefinition>,
}

impl DataSourceDefinition {
    // todo: parse values from DSN.
    pub fn values_from(mut self, mut dsn: Dsn) -> Self {
        debug_assert!(self.id == dsn.driver);
        let username_value = dsn.username.clone();
        let password_value = dsn.password.clone();
        if let Some(val) = dsn.protocol.as_deref() {
            if let Some(proto) = self.protocol.as_mut() {
                proto.value.replace(val.to_string());
            } else {
                self.protocol
                    .replace(Protocol::default().value(val.to_string()));
            }
        }

        match self.r#type {
            DataSourceType::Uri => match self.options.as_mut() {
                Some(options) => match options {
                    DataSourceOptions::Path { path: _ } => {
                        panic!("mixed path and uri type of DSN");
                    }
                    DataSourceOptions::Uri {
                        host,
                        port,
                        subject,
                    } => {
                        if let Some(addr) = dsn.addresses.first() {
                            if let Some(value) = addr.host.as_ref() {
                                host.value.replace(value.to_string());
                            }
                            if let Some(value) = addr.port.as_ref() {
                                port.value.replace(value.to_string());
                            }
                        }
                        if let Some(value) = dsn.subject.as_ref() {
                            subject.value.replace(value.to_string());
                        }
                    }
                    DataSourceOptions::Endpoint { endpoint } => {
                        let mut endpoint_str = String::new();
                        if let Some(addr) = dsn.addresses.first() {
                            if let Some(value) = addr.host.as_ref() {
                                endpoint_str.push_str(value.as_str());
                            }
                            if let Some(value) = addr.port.as_ref() {
                                endpoint_str.push_str(":");
                                endpoint_str.push_str(value.to_string().as_str());
                            }
                        }
                        if let Some(value) = dsn.subject.as_ref() {
                            endpoint_str.push_str("/");
                            endpoint_str.push_str(value.as_str());
                        }
                        endpoint.value.replace(endpoint_str);
                    }
                },
                None => (),
            },
            DataSourceType::Path => {
                if let Some(value) = dsn.path.as_ref() {
                    match self.options.as_mut() {
                        Some(options) => match options {
                            DataSourceOptions::Path { path } => {
                                path.value.replace(value.to_string());
                            }
                            DataSourceOptions::Uri {
                                host: _,
                                port: _,
                                subject: _,
                            } => panic!("mixed path and uri type of DSN"),
                            DataSourceOptions::Endpoint { endpoint: _ } => {
                                panic!("mixed path and uri type of DSN")
                            }
                        },
                        None => {
                            self.options.replace(DataSourceOptions::Path {
                                path: OptionDef::default().value(value.to_string()),
                            });
                        }
                    }
                }
            }
        }
        if password_value.is_some() || username_value.is_some() {
            if let Some(auth) = self
                .authentication
                .alternatives
                .iter_mut()
                .find(|auth| auth.name == "plain")
            {
                self.authentication.value.replace("plain".to_string());
                if let Some(value) = username_value.as_deref() {
                    auth.username
                        .get_or_insert(Default::default())
                        .value
                        .replace(value.to_string());
                }
                if let Some(value) = password_value {
                    auth.password
                        .get_or_insert(Default::default())
                        .value
                        .replace(value);
                }
            }
        }
        for (name, auth) in self
            .authentication
            .alternatives
            .iter_mut()
            .filter(|item| item.name != "plain")
            .flat_map(|auth| auth.params.iter_mut().map(|param| (&auth.name, param)))
        {
            if let Some(value) = dsn.remove(&auth.name) {
                self.authentication.value.replace(name.to_string());
                if !value.is_empty() {
                    auth.value.replace(value);
                }
            }
        }
        for group in self.groups.as_mut_slice() {
            if group.collapsible {
                group.collapsed.replace(false);
            }
            for param in &mut group.params {
                if let Some(value) = dsn.remove(&param.name) {
                    if group.collapsible {
                        group.collapsed.replace(true);
                    }
                    if !value.is_empty() {
                        param.value.replace(value);
                    }
                }
            }
        }

        if let Some(datasets) = self.datasets.as_mut() {
            for dataset_param in datasets.categories.as_mut_slice() {
                if let Some(target) = dataset_param.target.as_mut() {
                    if let Some(value) = dsn.remove(target.name.clone()) {
                        if !value.is_empty() {
                            if target.multiple == true {
                                target.value = Some(serde_json::Value::Array(
                                    value
                                        .split(",")
                                        .into_iter()
                                        .map(|v| serde_json::Value::String(v.to_string()))
                                        .collect(),
                                ));
                            } else {
                                target.value = Some(serde_json::Value::String(value));
                            }
                        }
                    }
                }
            }
        }

        for param in &mut self.params {
            if let Some(value) = dsn.remove(&param.name) {
                if !value.is_empty() {
                    param.value.replace(value);
                }
            }
        }

        for (name, value) in dsn.params {
            self.params.push(Param {
                name,
                hint: None,
                description: None,
                required: Some(false),
                placeholder: None,
                value: Some(value),
                display: None,
                r#if: None,
                alternatives: None,
                params: None,
            })
        }
        self
    }
}

#[test]
fn test() {
    use std::str::FromStr;
    let json = include_str!("en/tmq.yaml");
    let mut def: Vec<DataSourceDefinition> = serde_yaml::from_str(json).unwrap();
    let json2 = serde_json::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def[0]).unwrap();
    println!("{}", &toml);

    let dsn = "tmq+ws://root:taosdata@localhost:6041/database?token=abc";
    let dsn = Dsn::from_str(&dsn).unwrap();
    let tmq = &mut def[0];
    let new = tmq.clone().values_from(dsn);
    dbg!(tmq, new);
}
#[test]
fn opc() {
    use std::str::FromStr;
    let json = include_str!("cn/opc.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_yaml::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "opc+ua://localhost:123/opcua/server1?ua.nodes=a::b::c::d";
    let dsn = Dsn::from_str(&dsn).unwrap();
    let dsn = def.values_from(dsn);
    dbg!(&dsn);
}
#[test]
fn influxdb() {
    use std::str::FromStr;
    let json = include_str!("cn/influxdb.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_yaml::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "influxdb://localhost:123/opcua/server1?ua.nodes=a::b::c::d";
    let dsn = Dsn::from_str(&dsn).unwrap();
    let dsn = def.values_from(dsn);
    dbg!(&dsn);
}
#[test]
fn test_mqtt() {
    use std::str::FromStr;
    let json = include_str!("cn/mqtt.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_yaml::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "mqtt://localhost:123/opcua/server1?ca=abc&cert=abc&abc";
    let dsn = Dsn::from_str(&dsn).unwrap();
    // let tmq = &mut def[0];
    let ds = def.values_from(dsn);
    assert_eq!(ds.groups[0].collapsed, Some(true));
    dbg!(&ds);
}
#[test]
fn test_values() {
    use std::str::FromStr;
    let dsn = "tmq+ws://root:taosdata@localhost:6041/database?token=abc";
    let _dsn = Dsn::from_str(&dsn).unwrap();
}

const fn bool_is_false(v: &bool) -> bool {
    !*v
}
