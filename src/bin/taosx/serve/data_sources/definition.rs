use serde::{Deserialize, Serialize};

use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ProtocolItem {
    name: String,
    display: Option<String>,
    description: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct Protocol {
    display: Option<String>,
    description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
		#[serde(default)]
		choices: Vec<ProtocolItem>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(rename = "snake_case")]
pub enum DataSourceType {
		Uri,
		Path,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct OptionDef {
    display: Option<String>,
    description: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(rename = "snake_case")]
pub enum DataSourceOptions {
		Path { path: OptionDef },
		Uri { host: OptionDef, port: Option<u16>, username: Option<String>, password: Option<String>,  }
}



#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct Param {
		name: String,
		hint: Option<String>,
		description: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct GroupedParams {
		name: String,
		display_order: Option<u8>,
		description: Option<String>,
		params: Vec<Param>,

}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct DataSourceDefinition {
		/// Data source driver id
    id: String,
		/// Data source driver name.
    name: String,
		/// Data source description in markdown format.
    description: Option<String>,
		/// Type for DSN parser.
		r#type: DataSourceType,
		/// Allow custom parameters.
		#[serde(default)]
		strict: bool,
		/// Options for specified type.
		options: DataSourceOptions,
		/// Protocol list.
    protocol: Option<Protocol>,
		/// Grouped parameters.
    #[serde(skip_serializing_if = "Vec::is_empty")]
		#[serde(default)]
		groups: Vec<GroupedParams>,
		/// Ungrouped parameters.
    #[serde(skip_serializing_if = "Vec::is_empty")]
		#[serde(default)]
		params: Vec<Param>,
		/// Schema definitions, not used currently.
		#[serde(skip_serializing_if = "Option::is_none")]
		definitions: Option<()>,
}
