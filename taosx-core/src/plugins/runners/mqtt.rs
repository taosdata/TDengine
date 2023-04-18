use std::collections::HashMap;

use serde_json::to_string;

#[derive(Debug, serde::Serialize)]
struct MqttConfig {
    log_level: Option<String>,
    remote: String,
    mqtt: MqttConnectConfig,
    topics: HashMap<String, u8>,
}

#[derive(Debug, serde::Serialize)]
struct MqttConnectConfig {
    address: String,
    client_id: Option<String>,
    username: Option<String>,
    password: Option<String>,
    keep_alive: Option<usize>,
    clean_session: Option<bool>,
    ca: Option<String>,
    cert: Option<String>,
    cert_key: Option<String>,
}

#[test]
fn test_mqtt_config() {
    let log_level = Some("debug".to_string());
    let remote = "127.0.0.1:62307".to_string();
    let address = "tcp://127.0.0.1:1883".to_string();
    let client_id = Some("12123".to_string());
    let username = Some("mqtt_test".to_string());
    let password = Some("123456".to_string());
    let keep_alive = Some(60 as usize);
    let clean_session = Some(true);
    let ca = Some(r#"-----BEGIN CERTIFICATE-----
MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt
-----END CERTIFICATE-----"#.to_string());
    let cert = Some(r#"-----BEGIN CERTIFICATE-----
MIIDEzCCAfugAwIBAgIBATANBgkqhkiG9w0BAQsFADA
-----END CERTIFICATE-----"#.to_string());
    let cert_key = Some(r#"-----BEGIN CERTIFICATE-----
MIIEpAIBAAKCAQEAzLiGiSwpxkENtjrzS7pNLblTnWe4HUUFwYyUX0H
-----END RSA PRIVATE KEY-----"#.to_string());
    let mut topics = HashMap::new();
    topics.insert("topic-1".to_string(), 1);
    let mqtt_config = MqttConfig {
        log_level,
        remote,
        mqtt: MqttConnectConfig { 
            address, 
            client_id, 
            username, 
            password, 
            keep_alive, 
            clean_session, 
            ca, 
            cert, 
            cert_key 
        },
        topics,
    };
    let toml = toml::to_string(&mqtt_config).unwrap();
    println!("{}", toml);
}