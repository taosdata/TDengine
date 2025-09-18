use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use anyhow::{anyhow, Context, Result};
use taos::{Address, Dsn, Itertools};

pub trait DsnParamGetter {
    fn get_bool(&self, key: &str) -> Result<Option<bool>>;
}

impl DsnParamGetter for Dsn {
    fn get_bool(&self, key: &str) -> Result<Option<bool>> {
        self.params
            .get(key)
            .map(|v| {
                v.parse::<bool>()
                    .map_err(|err| anyhow!("invalid param {}, cause: {}", key, err.to_string()))
            })
            .transpose()
    }
}

pub fn dsn_to_json(dsn: &Dsn) -> serde_json::Value {
    let mut map_l1 = serde_json::Map::new();
    let mut map_l2 = serde_json::Map::new();
    map_l1.insert("type".to_string(), serde_json::json!(dsn.driver));
    if let Some(protocol) = &dsn.protocol {
        map_l1.insert("protocol".to_string(), serde_json::json!(protocol));
    }
    if let Some(username) = &dsn.username {
        map_l2.insert("username".to_string(), serde_json::json!(username));
    }
    if let Some(password) = &dsn.password {
        map_l2.insert("password".to_string(), serde_json::json!(password));
    }
    if let Some(address) = dsn.addresses.first() {
        if let Some(host) = &address.host {
            map_l2.insert("host".to_string(), serde_json::json!(host));
        }
        if let Some(port) = &address.port {
            map_l2.insert("port".to_string(), serde_json::json!(port));
        }
    }
    if let Some(path) = &dsn.path {
        map_l2.insert("path".to_string(), serde_json::json!(path));
    }
    if let Some(subject) = &dsn.subject {
        map_l2.insert("subject".to_string(), serde_json::json!(subject));
    }
    // custom parameters for different drivers
    match dsn.driver.to_lowercase().as_str() {
        "mqtt" | "kafka" | "sparkplugb" => {
            // 192.168.1.45:1883,192.168.1.46:1883,...
            let endpoint = dsn
                .addresses
                .iter()
                .map(|address| match (&address.host, &address.port) {
                    (Some(host), Some(port)) => format!("{}:{}", host, port),
                    (Some(host), None) => host.clone(),
                    _ => String::new(),
                })
                .collect_vec()
                .join(",");
            map_l2.insert("endpoint".to_string(), serde_json::json!(endpoint));
        }
        "opcua" | "opcda" => {
            // 192.168.1.45:53530/OPCUA/SimulationServer
            let endpoint = if let Some(address) = dsn.addresses.first() {
                match (&address.host, &address.port) {
                    (Some(host), Some(port)) => {
                        format!("{}://{}:{}", dsn.driver.to_lowercase(), host, port)
                    }
                    (Some(host), None) => format!("{}://{}", dsn.driver.to_lowercase(), host),
                    _ => format!("{}://", dsn.driver.to_lowercase()),
                }
            } else {
                format!("{}://", dsn.driver.to_lowercase())
            };
            let endpoint = if let Some(subject) = &dsn.subject {
                format!("{}/{}", endpoint, subject)
            } else {
                endpoint
            };
            map_l2.insert("endpoint".to_string(), serde_json::json!(endpoint));
        }
        "tmq" => {
            // taos+ws://root:taosdata@192.168.1.45:6041/db1
            let endpoint = if let Some(address) = dsn.addresses.first() {
                match (&address.host, &address.port) {
                    (Some(host), Some(port)) => format!("{}:{}", host, port),
                    (Some(host), None) => host.clone(),
                    _ => String::new(),
                }
            } else {
                String::new()
            };
            let endpoint = match (&dsn.username, &dsn.password) {
                (Some(username), Some(password)) => {
                    format!("{}:{}@{}", username, password, endpoint)
                }
                (Some(username), None) => format!("{}@{}", username, endpoint),
                _ => endpoint,
            };
            let endpoint = if dsn.protocol.is_some() {
                format!(
                    "{}+{}://{}",
                    dsn.driver.to_lowercase(),
                    dsn.protocol.clone().unwrap(),
                    endpoint
                )
            } else {
                format!("{}://{}", dsn.driver.to_lowercase(), endpoint)
            };
            let endpoint = if let Some(subject) = &dsn.subject {
                format!("{}/{}", endpoint, subject)
            } else {
                endpoint
            };
            map_l2.insert("endpoint".to_string(), serde_json::json!(endpoint));
        }
        _ => {}
    }
    // other dsn params
    for (k, v) in &dsn.params {
        map_l2.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    map_l1.insert("data".to_string(), serde_json::Value::Object(map_l2));
    serde_json::Value::Object(map_l1)
}

pub fn json_to_dsn(json: &serde_json::Value) -> anyhow::Result<Dsn> {
    let mut dsn = Dsn::default();
    // json to hashmap
    let mut params_map = HashMap::<String, String>::new();
    match json {
        serde_json::Value::String(str) => {
            let str = if str.starts_with('"') {
                str.trim_matches('"').replace(r#"\""#, r#"""#)
            } else {
                str.to_string()
            };
            if str.is_empty() {
                return Ok(dsn);
            }
            // try to parse as json string
            let json_value: serde_json::Value = match serde_json::from_str(&str) {
                Ok(value) => value,
                Err(_) => {
                    tracing::info!("parse by json failed, use default dsn: {}", str);
                    return str
                        .parse()
                        .with_context(|| format!("Invalid data source: {}", json));
                }
            };
            // json to hashmap
            flatten_json(&String::new(), &json_value, &mut params_map);
        }
        serde_json::Value::Object(map) => {
            map.iter().for_each(|(k, v)| {
                flatten_json(k, v, &mut params_map);
            });
        }
        _ => anyhow::bail!("Invalid data source: {}", json),
    };
    // dsn driver
    dsn.driver = params_map
        .remove("type")
        .map(|s| s.to_string())
        .context("Invalid data source: 'type' not found")?;
    // dsn protocol
    dsn.protocol = params_map.remove("protocol").map(|s| s.to_string());
    // dsn username
    dsn.username = params_map.remove("username").map(|s| s.to_string());
    // dsn password
    dsn.password = params_map.remove("password").map(|s| s.to_string());
    // dsn addresses
    let host = params_map.remove("host").map(|s| s.to_string());
    let port = params_map.remove("port").map(|s| s.to_string());
    if host.is_some() && port.is_some() {
        // unnecessary to check unwrap result, because they are not None
        let host = host.unwrap();
        let port = port.unwrap();
        let port: u16 = port
            .parse()
            .context("Invalid data source: 'port' is not a number")?;
        let address = Address::new(host, port);
        dsn.addresses.push(address);
    } else if host.is_some() {
        let host = host.unwrap();
        let address = Address::from_host(host);
        dsn.addresses.push(address);
    }
    // dsn path
    dsn.path = params_map.remove("path").map(|s| s.to_string());
    // dsn subject
    dsn.subject = params_map.remove("subject").map(|s| s.to_string());
    // custom parameters for different drivers
    match dsn.driver.to_lowercase().as_str() {
        "mqtt" | "kafka" | "sparkplugb" => {
            // 192.168.1.45:1883,192.168.1.46:1883,...
            let endpoint = params_map.remove("endpoint").map(|s| s.to_string());
            if let Some(endpoint) = endpoint {
                let endpoints = endpoint.split(",").collect::<Vec<&str>>();
                for endpoint in endpoints {
                    let parts = endpoint.split(":").collect::<Vec<&str>>();
                    if parts.len() == 2 {
                        let host = parts[0].to_string();
                        let port = parts[1].parse::<u16>().with_context(|| {
                            format!(
                                "Invalid data source: 'port' is not a number, endpoint: {}",
                                endpoint
                            )
                        })?;
                        let address = Address::new(host, port);
                        dsn.addresses.push(address);
                    }
                }
            }
        }
        "opcua" | "opcda" => {
            // 192.168.1.45:53530/OPCUA/SimulationServer
            let endpoint = params_map.remove("endpoint").map(|s| s.to_string());
            if let Some(endpoint) = endpoint {
                let ep = format!(
                    "opcua://{}",
                    endpoint
                        .trim_start_matches("opcua://")
                        .trim_start_matches("opcda://")
                );
                let d = Dsn::from_str(&ep).with_context(|| {
                    format!(
                        "Invalid data source: parse endpoint error, endpoint: {}",
                        endpoint
                    )
                })?;
                dsn.addresses = d.addresses;
                dsn.path = d.path;
                dsn.subject = d.subject;
            }
        }
        "tmq" => {
            // taos+ws://root:taosdata@192.168.1.45:6041/db1
            let endpoint = params_map.remove("endpoint").map(|s| s.to_string());
            if let Some(endpoint) = endpoint {
                let d = Dsn::from_str(&endpoint).with_context(|| {
                    format!(
                        "Invalid data source: parse endpoint error, endpoint: {}",
                        endpoint
                    )
                })?;
                match (d.driver.as_str(), d.protocol.as_ref()) {
                    ("ws" | "wss" | "http" | "https", None) => {
                        // TD-34891 支持 ws://... 写法
                        dsn.protocol = Some(d.driver);
                    }
                    _ => {
                        dsn.protocol = d.protocol;
                    }
                }
                dsn.username = d.username;
                dsn.password = d.password;
                dsn.addresses = d.addresses;
                dsn.path = d.path;
                dsn.subject = d.subject;
                params_map.extend(d.params);
            }
        }
        _ => {}
    }
    // other dsn params
    dsn.params = BTreeMap::from_iter(params_map);
    Ok(dsn)
}

fn flatten_json(key: &String, value: &serde_json::Value, map: &mut HashMap<String, String>) {
    match value {
        serde_json::Value::Null => {
            // ignore
        }
        serde_json::Value::Bool(b) => {
            map.insert(key.clone(), b.to_string());
        }
        serde_json::Value::Number(n) => {
            map.insert(key.clone(), n.to_string());
        }
        serde_json::Value::String(s) => {
            if !s.is_empty() {
                map.insert(key.clone(), s.clone());
            }
        }
        serde_json::Value::Array(arr) => {
            // maybe lost data with duplicate key
            arr.iter().for_each(|val| {
                flatten_json(key, val, map);
            });
        }
        serde_json::Value::Object(obj) => {
            obj.iter().for_each(|(k, v)| {
                flatten_json(k, v, map);
            });
        }
    }
}

pub fn option_param<'a>(dsn: &'a Dsn, key: &'a str) -> Option<&'a str> {
    dsn.get(key).map(|v| v.trim()).filter(|v| !v.is_empty())
}

pub fn parse_option_param<T>(dsn: &Dsn, key: &str) -> std::result::Result<Option<T>, T::Err>
where
    T: std::str::FromStr,
{
    dsn.get(key)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|value| value.parse::<T>())
        .transpose()
}

pub fn parse_simple_params<T>(dsn: &Dsn, key: &str) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    dsn.get(key)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse::<T>()
                .with_context(|| format!("invalid {key}: `{v}`"))
        })
        .transpose()
}

pub fn parse_multiple_value<T>(dsn: &Dsn, key: &str) -> anyhow::Result<Option<Vec<T>>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    dsn.get(key)
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.split(',')
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(|v| {
                    v.parse::<T>()
                        .with_context(|| format!("invalid {key}: `{v}`"))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use crate::utils::dsn::dsn_to_json;
    use crate::utils::dsn::json_to_dsn;

    /// test Value::String(json_string)
    #[test]
    fn test_json_to_dsn_use_json_string() {
        let from = r#"
{
    "name": "",
    "type": "mqtt",
    "targetDB": "",
    "agent": "",
    "data": {
        "connection_options": {
            "host": "0.0.0.0",
            "port": "9099"
        },
        "authentication": {
            "plain": {
                "username": "uname",
                "password": "upass"
            },
            "currentTab": "plain"
        },
        "groups_before": {
            "ssl": {
                "ca": "",
                "cert": "",
                "cert_key": "",
                "isEnable": false
            },
            "collect": {
                "version": "3.1",
                "client_id": "",
                "keep_alive": 60,
                "clean_session": true,
                "topics": "",
                "compression": "none",
                "char_encoding": "UTF_8"
            }
        },
        "checkConnectivity": "",
        "groups_after": {
            "mode":{
                "collect_mode": "subscribe",
                "interval": 10,
                "request_timeout": 10,
                "update_interval": 600
             }
        },
        "datasets": {
            "csv_config_file": "",
            "select_all_points": {
                "root": "",
                "namespaces": "",
                "node_id_pattern": "",
                "browse_name_pattern": "",
                "super_table_expression": "opc_{type}",
                "child_table_expression": "t_{ns}_{id}",
                "table_primary_key": "original_ts",
                "table_primary_key_alias": "ts"
            },
            "currentTab": "csv_config_file"
        },
        "advanced_options": {
            "unprocessed_messages_buffer_size": 50000,
            "maximum_processing_batch": 100,
            "batch_size": 1000,
            "batch_timeout": 500,
            "keep_raw_data": false,
            "keep_raw_data_days": 1,
            "keep_raw_data_dir": "",
            "health_check_window_in_second_type": "s",
            "busy_threshold": 100,
            "busy_threshold_type": "%",
            "max_queue_length": 1000,
            "max_errors_in_window": 10
        },
        "write_config": {
            "primary_timestamp_overflow": "archive",
            "primary_timestamp_null": "archive",
            "table_name_length_overflow": "archive",
            "table_name_contains_illegal_char": "",
            "table_name_contains_illegal_char_type": "replace_to",
            "variable_not_exist_in_table_name_template": "",
            "variable_not_exist_in_table_name_template_type": "replace_to",
            "field_name_length_overflow": "archive"
        }
    }
}"#;
        let from = serde_json::Value::String(from.to_string());
        let dsn = json_to_dsn(&from).unwrap();
        dbg!(&dsn);
    }

    /// test Value::Object(json_object)
    #[test]
    fn test_json_to_dsn_use_json_object() {
        let from = r#"
{
  "name": "eee",
  "type": "mqtt",
  "targetDB": "guxiang",
  "agent": "",
  "data": {
    "connection_options": {
      "host": "192.168.1.45",
      "port": "1883"
    },
    "authentication": {
      "plain": {
        "username": "",
        "password": ""
      },
      "currentTab": "plain"
    },
    "groups_before": {
      "ssl": {
        "ca": "",
        "cert": "",
        "cert_key": "",
        "isEnable": false
      },
      "collect": {
        "version": "3.1",
        "client_id": "eee",
        "keep_alive": 60,
        "clean_session": true,
        "topics": "test::0",
        "topic_pattern": "",
        "compression": "none",
        "char_encoding": "UTF_8"
      }
    },
    "checkConnectivity": "",
    "groups_after": "",
    "parser": {
      "parse": {
        "payload": {
          "json": [],
          "keep": true
        }
      },
      "model": {
        "name": "",
        "using": "",
        "columns": [
          "ts"
        ],
        "tags": []
      }
    },
    "advanced_options": {
      "unprocessed_messages_buffer_size": 50000,
      "maximum_processing_batch": 100,
      "batch_size": 1000,
      "batch_timeout": 500,
      "keep_raw_data": false,
      "keep_raw_data_days": 1,
      "keep_raw_data_dir": "",
      "health_check_window_in_second_type": "s",
      "busy_threshold": 100,
      "busy_threshold_type": "%",
      "max_queue_length": 1000,
      "max_errors_in_window": 10
    },
    "write_config": {
      "primary_timestamp_overflow": "archive",
      "primary_timestamp_null": "archive",
      "table_name_length_overflow": "archive",
      "table_name_contains_illegal_char": "",
      "table_name_contains_illegal_char_type": "replace_to",
      "variable_not_exist_in_table_name_template": "",
      "variable_not_exist_in_table_name_template_type": "replace_to",
      "field_name_length_overflow": "archive"
    }
  }
}"#;
        let from = serde_json::from_str(from).unwrap();
        let dsn = json_to_dsn(&from).unwrap();
        dbg!(&dsn);
    }

    /// test Value::String(dsn_string)
    #[test]
    fn test_json_to_dsn_use_dsn_string() {
        let from = "tmq+ws://0.0.0.0:1883";
        let from = serde_json::Value::String(from.to_string());
        let dsn = json_to_dsn(&from).unwrap();
        dbg!(&dsn);
    }

    #[test]
    fn test_dsn_to_json() {
        let dsn = r#"mongodb://admin:tbase125!@127.0.0.1:27017?database=test_db5_${y}&collection=tb_${J}&sql={"createtime":{"$gte":${start_datetime},"$lt":${end_datetime}}}&start=2020-01-02T00:00:00+08:00&end=2020-03-01T00:00:00+08:00&read_concurrency=0&tls=False"#;
        let dsn = json_to_dsn(&serde_json::Value::String(dsn.to_string())).unwrap();
        dbg!(&dsn);
        let json = dsn_to_json(&dsn);
        dbg!(&json);
        println!("{}", serde_json::to_string(&json).unwrap());
    }

    #[test]
    fn tmq_json_to_dsn_test() -> anyhow::Result<()> {
        let dsn = json_to_dsn(&serde_json::json!({
            "agent": "",
            "type": "tmq",
            "data": {
                "endpoint": "ws://root:taosdata@192.168.0.201:6041/astro_test",
                "auto.offset.reset": "earliest",
                "group.id": "",
                "client.id": "",
                "timeout": "0s",
                "experimental.snapshot.enable": true,
                "with.meta.drop": true,
                "with.meta.delete": true,
                "compression": false,
                "health_check_window_in_second": "0s",
                "busy_threshold": "100%",
                "max_queue_length": 1000,
                "max_errors_in_window": 10,
                "num.of.consumers": 0,
                "num.of.writers": 0,
                "prefer": "auto",
                "commit.chunk.size": 0,
                "commit.interval.ms": 0
            }
        }))
        .unwrap();
        assert_eq!(dsn.driver, "tmq");
        assert!(dsn.protocol.is_some_and(|s| s == "ws"));

        let dsn = json_to_dsn(&serde_json::json!({
            "agent": "",
            "type": "tmq",
            "data": {
                "endpoint": "tmq+ws://root:taosdata@192.168.0.201:6041/astro_test",
                "auto.offset.reset": "earliest",
                "group.id": "",
                "client.id": "",
                "timeout": "0s",
                "experimental.snapshot.enable": true,
                "with.meta.drop": true,
                "with.meta.delete": true,
                "compression": false,
                "health_check_window_in_second": "0s",
                "busy_threshold": "100%",
                "max_queue_length": 1000,
                "max_errors_in_window": 10,
                "num.of.consumers": 0,
                "num.of.writers": 0,
                "prefer": "auto",
                "commit.chunk.size": 0,
                "commit.interval.ms": 0
            }
        }))
        .unwrap();
        assert_eq!(dsn.driver, "tmq");
        assert!(dsn.protocol.is_some_and(|s| s == "ws"));
        Ok(())
    }
}
