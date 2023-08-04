// use std::collections::HashMap;

// use taos::Dsn;

// use serde_json::Value as JsonValue;

// mod plugins;

// pub struct DataSet {
//     id: String,
//     parser: Option<JsonValue>,
// }
// pub struct DataSourceConfig {
//     /// Connection url.
//     connection: Dsn,
//     /// A list of data set.
//     sets: Vec<DataSet>,
//     options: HashMap<String, Option<String>>,
// }

// #[cfg(test)]

// mod tests {
//     use super::*;

//     #[test]
//     fn config() {
//         let config_str = r#"
// 		{ id: }
// 		"#;
//     }
// }
