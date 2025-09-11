use std::time::Duration;

use anyhow::Context;
use taos::Dsn;

use source_mqtt::config::{
    Certificates, parse_clean_session, parse_client_id, parse_host_port, parse_keep_alive,
    parse_tls_certificates, parse_version,
};
use taosx_core::utils::dsn::{option_param, parse_option_param, parse_simple_params};

#[derive(Clone)]
pub struct TmqConfig {
    pub dsn: Dsn,
    pub with_meta: bool,
}

impl TryFrom<&Dsn> for TmqConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        Ok(Self {
            dsn: dsn.clone(),
            with_meta: parse_simple_params(dsn, "with.meta")?.unwrap_or_default(),
        })
    }
}

#[derive(Clone)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub version: source_mqtt::client::Version,
    pub topic: String,
    pub meta_topic: Option<String>,
    pub qos: u8,
    pub client_id: String,
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: Option<Certificates>,
    pub concurrency: usize,
}

impl TryFrom<&Dsn> for MqttConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        let (host, port) = parse_host_port(dsn)?;

        Ok(Self {
            host,
            port,
            version: parse_version(dsn)?,
            topic: option_param(dsn, "topic")
                .context("topic not found")?
                .replace("${", "{"),
            meta_topic: option_param(dsn, "meta_topic").map(|s| s.replace("${", "{")),
            qos: parse_simple_params(dsn, "qos")?.unwrap_or(0),
            client_id: parse_client_id(dsn)?,
            keep_alive: parse_keep_alive(dsn)?,
            clean_session: parse_clean_session(dsn)?,
            username: dsn.username.clone(),
            password: dsn.password.clone(),
            tls: parse_tls_certificates(dsn)?,
            concurrency: parse_option_param::<usize>(dsn, "read_concurrency")
                .context("parse read_concurrency param error")?
                .unwrap_or_else(default_concurrency),
        })
    }
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|s| s.get())
        .unwrap_or(5)
}
