use serde::{Deserialize, Serialize};

use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct Protocol {
    name: String,
    display: Option<String>,
    description: Option<String>,
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
pub struct DataSource {
    id: String,
    name: String,
    description: Option<String>,
		r#type: DataSourceType,
		strict: bool,
		options: DataSourceOptions,
    #[serde(skip_serializing_if = "Vec::is_empty")]
		#[serde(default)]
    protocol: Vec<Protocol>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
		#[serde(default)]
		groups: Vec<GroupedParams>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
		#[serde(default)]
		params: Vec<Param>,
}
