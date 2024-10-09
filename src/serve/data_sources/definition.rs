use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use taos::Dsn;
use taosx_core::utils;
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(rename(serialize = "patternMsg"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern_msg: Option<String>,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<OptionDef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<OptionDef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        security_mode: Option<Param>,
        #[serde(skip_serializing_if = "Option::is_none")]
        security_policy: Option<Param>,
        #[serde(skip_serializing_if = "Option::is_none")]
        certificate: Option<Param>,
        #[serde(skip_serializing_if = "Option::is_none")]
        private_key: Option<Param>,
        #[serde(skip_serializing_if = "Option::is_none")]
        connect_timeout: Option<Param>,
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
pub struct ConflictsWith {
    pub name: String,
    pub value: Option<String>,
    pub when: Option<String>,
}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct Param {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_order: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<Hint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(rename(serialize = "patternMsg"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern_msg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiple: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub editable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden: Option<bool>,
    /// Requires a boolean param to be true, otherwise hide.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub requires: Option<String>,
    /// Condition for a parameter, eg. "if: protocol.ws"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#if: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflicts_with: Option<Vec<ConflictsWith>>,
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
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum HintType {
    Constant {
        value: String,
    },
    Integer {
        value: Option<i64>,
        min: Option<i64>,
        max: Option<i64>,
        default: Option<i64>,
    },
    Str {
        value: Option<String>,
        choices: Vec<String>,
        default: Option<String>,
    },
    Time {
        value: Option<String>,
        default: Option<String>,
    },
    Duration {
        value: Option<String>,
        default: Option<String>,
    },
    File {
        value: Option<PathBuf>,
        default: Option<PathBuf>,
    },
    Bool {
        value: Option<bool>,
        default: Option<bool>,
    },
}

impl HintType {
    pub fn parse_value(&mut self, v: &str) -> bool {
        match self {
            HintType::Constant { value } => value == v,
            HintType::Integer {
                value, min, max, ..
            } => {
                if let Ok(v) = v.parse() {
                    if let Some(min) = min {
                        if v < *min {
                            return false;
                        }
                    }
                    if let Some(max) = max {
                        if v > *max {
                            return false;
                        }
                    }
                    value.replace(v);
                    true
                } else {
                    false
                }
            }
            HintType::Str { value, choices, .. } => {
                if !choices.is_empty() {
                    if choices.contains(&v.to_string()) {
                        value.replace(v.to_string());
                        true
                    } else {
                        false
                    }
                } else {
                    value.replace(v.to_string());
                    true
                }
            }
            HintType::Time { value, .. } => {
                if let Ok(time) = chrono::DateTime::parse_from_rfc3339(v) {
                    value.replace(time.to_rfc3339());
                    true
                } else {
                    false
                }
            }
            HintType::Duration { value, .. } => {
                if let Ok(duration) = utils::parse_duration(v) {
                    value.replace(format!("{:?}", duration));
                    true
                } else {
                    false
                }
            }
            HintType::File { value, .. } => {
                value.replace(std::path::Path::new(v).to_path_buf());
                true
            }
            HintType::Bool { value, .. } => {
                if let Ok(b) = v.parse() {
                    value.replace(b);
                    true
                } else {
                    false
                }
            }
        }
    }
}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct HintItem {
    selected: bool,
    display: Option<String>,
    #[serde(flatten)]
    r#type: HintType,
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
    OneOf(Vec<HintItem>),
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct GroupedParams {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_order: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub collapsible: bool,
    // added by zachary, 2024-04-17,
    // if the grouped params is connection option, this group will display before the connection check button.
    #[serde(default)]
    pub connection_option: bool,

    /// Dropdown collapsed or not.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Authentication items.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alternatives: Vec<AuthItem>,
}

impl Authentication {
    pub fn is_none(&self) -> bool {
        self.display.is_none()
            && self.description.is_none()
            && self.value.is_none()
            && self.alternatives.is_empty()
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub categories: Vec<DatasetParam>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub params: Vec<Param>,
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
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub params: Vec<Param>,
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
pub struct ParserFixedField {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct ParserDefinition {
    pub display: String,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<ParserFixedField>>,
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
    #[serde(skip_serializing_if = "Authentication::is_none")]
    pub authentication: Authentication,

    /// Grouped parameters.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub groups: Vec<GroupedParams>,

    /// Advanced parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub advanced: Option<GroupedParams>,

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
    pub fn compute(&mut self) {
        for group in self.groups.as_mut_slice() {
            // TD-25111
            if let (None, Some(desc)) = (&group.short_description, &group.description) {
                group.short_description = desc
                    .split_terminator("\n")
                    .next()
                    .map(ToString::to_string)
                    .map(|s| s.replace("<br>", ""));
            }
            if group.collapsible {
                group.collapsed.replace(false);
            }
            for param in &mut group.params {
                if let (None, Some(desc)) = (&param.short_description, &param.description) {
                    param.short_description = desc
                        .split_terminator("\n")
                        .next()
                        .map(ToString::to_string)
                        .map(|s| s.replace("<br>", ""));
                }
            }
        }
    }
    // todo: parse values from DSN.
    pub fn values_from(mut self, mut dsn: Dsn) -> Self {
        debug_assert!(self.id == dsn.driver);
        let username_value = dsn.username.clone();
        let password_value = dsn.password.clone();
        if self.protocol.is_some() {
            if let Some(val) = dsn.protocol.as_deref() {
                if let Some(proto) = self.protocol.as_mut() {
                    proto.value.replace(val.to_string());
                } else {
                    self.protocol
                        .replace(Protocol::default().value(val.to_string()));
                }
            }
        }

        match self.r#type {
            DataSourceType::Uri => {
                if let Some(options) = self.options.as_mut() {
                    match options {
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
                        DataSourceOptions::Endpoint {
                            endpoint,
                            username,
                            password,
                            security_mode,
                            security_policy,
                            certificate,
                            private_key,
                            connect_timeout,
                        } => {
                            match dsn.driver.as_str() {
                                "tmq" | "sync" => {
                                    let mut dsn = dsn.clone();
                                    dsn.driver = "tmq".to_string();
                                    for group in self.groups.as_mut_slice() {
                                        for param in &mut group.params {
                                            dsn.remove(&param.name);
                                        }
                                    }

                                    for param in &mut self.params {
                                        dsn.remove(&param.name);
                                    }
                                    endpoint.value.replace(dsn.to_string());
                                }
                                _ => {
                                    let mut endpoint_str = String::new();
                                    if let Some(scheme) = dsn.protocol.as_deref() {
                                        endpoint_str.push_str(scheme);
                                        endpoint_str.push_str("://");
                                    }
                                    if let Some(addr) = dsn.addresses.first() {
                                        endpoint_str.push_str(addr.to_string().as_str());
                                    }
                                    if let Some(value) = dsn.subject.as_ref() {
                                        endpoint_str.push('/');
                                        endpoint_str.push_str(value.as_str());
                                    }
                                    if dsn.driver == "opcua" {
                                        if let Some(value) = dsn.remove("security_mode") {
                                            security_mode
                                                .get_or_insert(Default::default())
                                                .value
                                                .replace(value.to_string());
                                        }
                                        if let Some(value) = dsn.remove("security_policy") {
                                            security_policy
                                                .get_or_insert(Default::default())
                                                .value
                                                .replace(value.to_string());
                                        }
                                        if let Some(value) = dsn.remove("certificate") {
                                            certificate
                                                .get_or_insert(Default::default())
                                                .value
                                                .replace(value.to_string());
                                        }
                                        if let Some(value) = dsn.remove("private_key") {
                                            private_key
                                                .get_or_insert(Default::default())
                                                .value
                                                .replace(value.to_string());
                                        }
                                        if let Some(value) = dsn.remove("connect_timeout") {
                                            connect_timeout
                                                .get_or_insert(Default::default())
                                                .value
                                                .replace(value.to_string());
                                        }
                                    }
                                    endpoint.value.replace(endpoint_str);
                                }
                            }

                            // user/pass
                            if let Some(value) = username_value.as_deref() {
                                username
                                    .get_or_insert(Default::default())
                                    .value
                                    .replace(value.to_string());
                            }
                            if let Some(value) = password_value.as_deref() {
                                password
                                    .get_or_insert(Default::default())
                                    .value
                                    .replace(value.to_string());
                            }
                        }
                    }
                }
            }
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
                            DataSourceOptions::Endpoint {
                                endpoint: _,
                                username: _,
                                password: _,
                                security_mode: _,
                                security_policy: _,
                                certificate: _,
                                private_key: _,
                                connect_timeout: _,
                            } => {
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

        for auth_item in self
            .authentication
            .alternatives
            .iter_mut()
            .filter(|item| item.name != "plain")
        {
            let mut is_current_auth = true;
            for param in &auth_item.params {
                if !dsn.params.contains_key(&param.name) {
                    is_current_auth = false;
                    break;
                }
            }
            if is_current_auth {
                self.authentication.value.replace(auth_item.name.clone());
                for param in auth_item.params.iter_mut() {
                    if let Some(value) = dsn.remove(&param.name) {
                        if !value.is_empty() {
                            param.value.replace(value);
                        }
                    }
                }
            }
        }

        // for (name, auth) in self
        //     .authentication
        //     .alternatives
        //     .iter_mut()
        //     .filter(|item| item.name != "plain")
        //     .flat_map(|auth| auth.params.iter_mut().map(|param| (&auth.name, param)))
        // {
        //     if let Some(value) = dsn.remove(&auth.name) {
        //         self.authentication.value.replace(name.to_string());
        //         if !value.is_empty() {
        //             auth.value.replace(value);
        //         }
        //     }
        // }
        if let Some(datasets) = self.datasets.as_mut() {
            for dataset_param in datasets.categories.as_mut_slice() {
                if let Some(target) = dataset_param.target.as_mut() {
                    if let Some(value) = dsn.remove(&target.name) {
                        if !value.is_empty() {
                            if target.multiple {
                                target.value = Some(serde_json::Value::Array(
                                    value
                                        .split(",")
                                        .map(|v| serde_json::Value::String(v.to_string()))
                                        .collect(),
                                ));
                            } else {
                                target.value = Some(serde_json::Value::String(value));
                            }
                            datasets.value.replace(target.name.clone());
                        }
                    }
                }
                for param in &mut dataset_param.params {
                    if let Some(value) = dsn.remove(&param.name) {
                        if !value.is_empty() {
                            param.value.replace(value);
                            // datasets.value.replace(param.name.clone());
                        }
                    }
                }
            }
            datasets.params.iter_mut().for_each(|param| {
                if let Some(value) = dsn.remove(&param.name) {
                    if !value.is_empty() {
                        param.value.replace(value);
                        datasets.value.replace(param.name.clone());
                    }
                }
            });
        }

        for group in self.groups.as_mut_slice() {
            // TD-25111
            if let (None, Some(desc)) = (&group.short_description, &group.description) {
                group.short_description =
                    desc.split_terminator("\n").next().map(ToString::to_string);
            }
            if group.collapsible {
                group.collapsed.replace(false);
            }
            for param in &mut group.params {
                if let (None, Some(desc)) = (&param.short_description, &param.description) {
                    param.short_description =
                        desc.split_terminator("\n").next().map(ToString::to_string);
                }
                if let Some(v) = dsn.remove(&param.name) {
                    if group.collapsible {
                        group.collapsed.replace(true);
                    }
                    if !v.is_empty() {
                        // for hint type recognition.
                        if let Some(hint) = &mut param.hint {
                            match hint {
                                Hint::Named(_) => (),
                                Hint::Flat { .. } => (),
                                Hint::OneOf(items) => {
                                    let has_selected = false;
                                    for item in items {
                                        if has_selected {
                                            // if has selected item, then unselect all.
                                            item.selected = false;
                                        }
                                        if item.r#type.parse_value(&v) {
                                            // if value is valid, then select it.
                                            item.selected = true;
                                        } else {
                                            item.selected = false;
                                        }
                                    }
                                }
                            }
                        }

                        param.value.replace(v);
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

        if let Some(group) = &mut self.advanced {
            if let (None, Some(desc)) = (&group.short_description, &group.description) {
                group.short_description =
                    desc.split_terminator("\n").next().map(ToString::to_string);
            }
            if group.collapsible && group.collapsed.is_none() {
                group.collapsed.replace(false);
            }
            for param in &mut group.params {
                if let (None, Some(desc)) = (&param.short_description, &param.description) {
                    param.short_description =
                        desc.split_terminator("\n").next().map(ToString::to_string);
                }
                if let Some(v) = dsn.remove(&param.name) {
                    if group.collapsible {
                        group.collapsed.replace(true);
                    }
                    if !v.is_empty() {
                        // for hint type recognition.
                        if let Some(hint) = &mut param.hint {
                            match hint {
                                Hint::Named(_) => (),
                                Hint::Flat { .. } => (),
                                Hint::OneOf(items) => {
                                    let has_selected = false;
                                    for item in items {
                                        if has_selected {
                                            // if has selected item, then unselect all.
                                            item.selected = false;
                                        }
                                        if item.r#type.parse_value(&v) {
                                            // if value is valid, then select it.
                                            item.selected = true;
                                        } else {
                                            item.selected = false;
                                        }
                                    }
                                }
                            }
                        }

                        param.value.replace(v);
                    }
                }
            }
        }

        for (name, value) in dsn.params {
            self.params.push(Param {
                name,
                display_order: None,
                hint: None,
                short_description: None,
                description: None,
                required: Some(false),
                multiple: Some(false),
                editable: Some(false),
                placeholder: None,
                value: Some(value),
                pattern: None,
                pattern_msg: None,
                display: None,
                requires: None,
                hidden: None,
                r#if: None,
                alternatives: None,
                params: None,
                conflicts_with: None,
            })
        }
        self
    }
}

#[cfg(test)]
use std::str::FromStr;

#[test]
fn test() {
    use std::str::FromStr;
    let json = include_str!("en/tmq.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_json::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "tmq+ws://root:taosdata@localhost:6041/database?token=abc";
    let dsn = Dsn::from_str(dsn).unwrap();
    let new = def.clone().values_from(dsn);
    dbg!(def, new);
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
    let dsn = Dsn::from_str(dsn).unwrap();
    let dsn = def.values_from(dsn);
    dbg!(&dsn);
}
#[test]
fn test_mqtt() {
    use std::str::FromStr;
    let json = include_str!("en/mqtt.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_yaml::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "mqtt://localhost:123/opcua/server1?ca=abc&cert=abc&abc";
    let dsn = Dsn::from_str(dsn).unwrap();
    // let tmq = &mut def[0];
    let ds = def.values_from(dsn);
    assert_eq!(ds.groups[0].collapsed, Some(true));
    dbg!(&ds);

    dbg!(&ds.advanced);
}
#[test]
fn test_csv() {
    use std::str::FromStr;
    let json = include_str!("en/csv.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_yaml::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "csv:abc.csv?quote=\"";
    let dsn = Dsn::from_str(dsn).unwrap();
    // let tmq = &mut def[0];
    let ds = def.values_from(dsn);
    // assert_eq!(ds.groups[0].collapsed, Some(true));
    let options = ds.options.as_ref().unwrap();
    matches!(options, DataSourceOptions::Path { path: _ });
    dbg!(&ds);
}
#[test]
fn test_kafka() {
    use std::str::FromStr;
    let json = include_str!("en/kafka.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_yaml::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "kafka://a.k/?topics=a,b";
    let dsn = Dsn::from_str(dsn).unwrap();
    // let tmq = &mut def[0];
    let _ds = def.values_from(dsn);
}
#[test]
fn test_legacy() {
    use std::str::FromStr;
    let json = include_str!("en/taos.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_yaml::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "taos:///test?libraryPath=a.so";
    let dsn = Dsn::from_str(dsn).unwrap();
    // let tmq = &mut def[0];
    let ds = def.values_from(dsn);
    dbg!(ds);
}

#[test]
fn test_historian() {
    let json = include_str!("en/historian.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    dbg!(&def);
    let json = include_str!("cn/historian.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    dbg!(&def);
}

#[test]
fn test_pi() {
    let json = include_str!("en/pi.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();

    let dsn = "pi://PI?MaxBackfillRangeDays=1&point_file=*&system_configuration=PI Data Archive Only&batch_size=1000&batch_timeout=1";
    let dsn = Dsn::from_str(dsn).unwrap();
    let def = def.values_from(dsn);
    dbg!(&def);
    assert_eq!(
        def.datasets.as_ref().unwrap().value,
        Some("point_file".to_string())
    );
}

#[test]
fn test_opc_ua() {
    let json = include_str!("cn/opcua.yaml");
    let def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    // dbg!(&def);

    let dsn = "opcua://localhost:53530/OPCUA/SimulationServer?connect_timeout=10&security_mode=Sign&security_policy=Basic128Rsa15&connect_timeout=20";
    let dsn = Dsn::from_str(dsn).unwrap();
    let def = def.values_from(dsn);
    dbg!(&def);

    let ds_options = def.options.unwrap();
    match ds_options {
        DataSourceOptions::Endpoint {
            endpoint,
            username: _,
            password: _,
            security_mode,
            security_policy,
            certificate: _,
            private_key: _,
            connect_timeout,
        } => {
            assert_eq!(
                endpoint.value,
                Some("localhost:53530/OPCUA/SimulationServer".to_string())
            );
            assert_eq!(security_mode.unwrap().value, Some("Sign".to_string()));
            assert_eq!(
                security_policy.unwrap().value,
                Some("Basic128Rsa15".to_string())
            );
            assert_eq!(connect_timeout.unwrap().value, Some("20".to_string()));
        }
        _ => panic!("invalid options"),
    }

    // assert_eq!(
    //     def.datasets.as_ref().unwrap().value,
    //     Some("point_file".to_string())
    // );
}

#[test]
fn test_pi_backfill() {
    use std::str::FromStr;

    let hint_item = HintItem {
        selected: false,
        display: None,
        r#type: HintType::Constant {
            value: "auto".to_string(),
        },
    };
    let s = serde_json::to_string_pretty(&hint_item).unwrap();
    println!("hint: {}", &s);

    let json = include_str!("en/pi-backfill.yaml");
    let mut def: DataSourceDefinition = serde_yaml::from_str(json).unwrap();
    let json2 = serde_json::to_string(&def).unwrap();
    dbg!(&json2);
    let toml = toml::to_string_pretty(&def).unwrap();
    println!("{}", &toml);

    let dsn = "pibackfill://PIserver?BackfillStartTime=auto";
    let dsn = Dsn::from_str(dsn).unwrap();
    let tmq = &mut def;
    let new = tmq.clone().values_from(dsn);
    dbg!(&tmq, &new);
    assert_eq!(new.groups[0].params[0].value, Some("auto".to_string()));
    let dsn = "pibackfill://PIserver?BackfillStartTime=2023-01-01T00:00:00Z";
    let dsn = Dsn::from_str(dsn).unwrap();
    let tmq = &mut def;
    let new = tmq.clone().values_from(dsn);
    dbg!(&tmq, &new);
    assert_eq!(
        new.groups[0].params[0].value,
        Some("2023-01-01T00:00:00Z".to_string())
    );

    //todo!
    // assert_eq!(
    //     new.groups[0].params[2].hint,
    //     Some("2023-01-01T00:00:00Z".to_string())
    // );
}
#[test]
fn test_values() {
    use std::str::FromStr;
    let dsn = "tmq+ws://root:taosdata@localhost:6041/database?token=abc";
    let _dsn = Dsn::from_str(dsn).unwrap();
}

#[test]
fn test_short_desc() {}

const fn bool_is_false(v: &bool) -> bool {
    !*v
}
