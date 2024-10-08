use anyhow::bail;
use std::fs;

use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::runners::opc::config::AuthMethod;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UaConnectConfig {
    pub endpoint: String,
    pub connect_timeout: i64,
    pub request_timeout: i64,
    pub security_policy: String,
    pub security_mode: String,
    pub certificate: Option<String>,
    pub private_key: Option<String>,
    pub auth_method: AuthMethod,
    pub username: Option<String>,
    pub password: Option<String>,
    pub auth_certificate: Option<String>,
    pub auth_private_key: Option<String>,
}

impl UaConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let endpoint = Self::parse_endpoint(dsn)?;
        let connect_timeout = Self::parse_connect_timeout(dsn)?;
        let request_timeout = Self::parse_request_timeout(dsn)?;
        let security_policy = Self::parse_security_policy(dsn);
        let security_mode = Self::parse_security_mode(dsn);
        let certificate = Self::parse_file_path(dsn, "certificate")?;
        let private_key = Self::parse_file_path(dsn, "private_key")?;
        let auth_certificate = Self::parse_file_path(dsn, "auth_certificate")?;
        let auth_private_key = Self::parse_file_path(dsn, "auth_private_key")?;
        let username = dsn.username.clone();
        let password = dsn.password.clone();
        let auth_method = if username.is_some() || password.is_some() {
            match username.as_ref().zip(password.as_ref()) {
                Some(_) => AuthMethod::UserName,
                None => Err(anyhow::anyhow!("Username and password are both required for UserName authentication method in {}", dsn.clone().to_string()))?,
            }
        } else if auth_certificate.is_some() || auth_private_key.is_some() {
            AuthMethod::Certificate
        } else {
            AuthMethod::Anonymous
        };

        Ok(Self {
            endpoint,
            connect_timeout,
            request_timeout,
            security_policy,
            security_mode,
            certificate,
            private_key,
            auth_method,
            username,
            password,
            auth_certificate,
            auth_private_key,
        })
    }

    pub fn set_temp_filepath(&mut self, key: &str, filepath: &str) -> anyhow::Result<()> {
        match key {
            "certificate" => self.certificate = Some(filepath.to_string()),
            "private_key" => self.private_key = Some(filepath.to_string()),
            "auth_certificate" => self.auth_certificate = Some(filepath.to_string()),
            "auth_private_key" => self.auth_private_key = Some(filepath.to_string()),
            _ => bail!("invalid temp filepath key: {}", key),
        }
        Ok(())
    }

    fn parse_endpoint(dsn: &Dsn) -> anyhow::Result<String> {
        let addr = dsn
            .addresses
            .first()
            .ok_or_else(|| anyhow::anyhow!("endpoint is required"))?;
        let host = addr
            .host
            .clone()
            .ok_or(anyhow::anyhow!("host is required"))?;
        let port = addr.port.ok_or(anyhow::anyhow!("port is required"))?;
        let subject = dsn.subject.clone().unwrap_or("".to_string());
        let endpoint = format!("opc.tcp://{}:{}/{}", host, port, subject);
        Ok(endpoint)
    }

    fn parse_connect_timeout(dsn: &Dsn) -> anyhow::Result<i64> {
        Ok(dsn
            .params
            .get("connect_timeout")
            .map(|v| {
                v.parse::<i64>().map_err(|err| {
                    anyhow::anyhow!(
                        "parse connection_timeout failed, cause: {}",
                        err.to_string()
                    )
                })
            })
            .transpose()?
            .unwrap_or(10))
    }

    fn parse_request_timeout(dsn: &Dsn) -> anyhow::Result<i64> {
        Ok(dsn
            .params
            .get("request_timeout")
            .map(|v| {
                v.parse::<i64>().map_err(|err| {
                    anyhow::anyhow!("parse request_timeout failed, cause: {}", err.to_string())
                })
            })
            .transpose()?
            .unwrap_or(10))
    }

    fn parse_security_policy(dsn: &Dsn) -> String {
        dsn.params
            .get("security_policy")
            .map(|v| v.to_string())
            .unwrap_or("None".to_string())
    }

    fn parse_security_mode(dsn: &Dsn) -> String {
        dsn.params
            .get("security_mode")
            .map(|v| v.to_string())
            .unwrap_or("None".to_string())
    }

    /// 从 dsn 中解析 key
    /// 如果参数在 dsn 中不存在或参数值为空，返回 None
    /// 如果参数值以 @ 开头，则认为是文件路径，返回文件的绝对路径
    /// 如果参数值不以 @ 开头，则认为是文件内容，返回 Err
    fn parse_file_path(dsn: &Dsn, key: &str) -> anyhow::Result<Option<String>> {
        let value = dsn.params.get(key).map(|v| v.to_string());
        match value {
            None => Ok(None),
            Some(v) => {
                if v.is_empty() {
                    return Ok(None);
                }

                if let Some(file_path) = v.strip_prefix('@') {
                    let path = fs::canonicalize(file_path)
                        .map_err(|err| {
                            anyhow::anyhow!("{}: {} not found, cause: {}", key, file_path, err)
                        })?
                        .display()
                        .to_string();
                    Ok(Some(path))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[cfg(test)]
mod ua_connect_config_tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_file_path() {
        let dsn = Dsn::from_str("opcua://").unwrap();
        let param = UaConnectConfig::parse_file_path(&dsn, "certificate").unwrap();
        assert!(param.is_none());

        let dsn = Dsn::from_str("opcua://?certificate=").unwrap();
        let param = UaConnectConfig::parse_file_path(&dsn, "certificate").unwrap();
        assert!(param.is_none());
        // 以@开头，存在的文件
        let file_path = std::env::current_dir()
            .unwrap()
            .parent()
            .unwrap()
            .join("tests")
            .join("opc")
            .join("certificate.crt")
            .display()
            .to_string();
        let dsn = Dsn::from_str("opcua://?certificate=@../tests/opc/certificate.crt").unwrap();
        let absolute_path = UaConnectConfig::parse_file_path(&dsn, "certificate")
            .unwrap()
            .unwrap();
        assert_eq!(file_path, absolute_path);

        // 以@开头，不存在的文件
        let dsn = Dsn::from_str("opcua://?certificate=@abc").unwrap();
        let content = UaConnectConfig::parse_file_path(&dsn, "certificate");
        assert!(content.is_err());
        assert_eq!(
            "certificate: abc not found, cause: No such file or directory (os error 2)",
            content.unwrap_err().to_string()
        );

        // 不以@开头，文件内容
        let dsn = Dsn::from_str("opcua://?certificate=abc").unwrap();
        let content = UaConnectConfig::parse_file_path(&dsn, "certificate").unwrap();
        assert!(content.is_none());
    }

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("opc://localhost:1234/?").unwrap();
        let config = UaConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("opc.tcp://localhost:1234/", config.endpoint);
        assert_eq!(10, config.connect_timeout);
        assert_eq!(10, config.request_timeout);
        assert_eq!("None", config.security_policy);
        assert_eq!("None", config.security_mode);
        assert_eq!(None, config.auth_certificate);
        assert_eq!(None, config.auth_private_key);
        assert_eq!(None, config.username);
        assert_eq!(None, config.password);
        assert_eq!(AuthMethod::Anonymous, config.auth_method);

        let dsn = Dsn::from_str("opc://root:taosdata@localhost:1234").unwrap();
        let config = UaConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("opc.tcp://localhost:1234/", config.endpoint);
        assert_eq!(10, config.connect_timeout);
        assert_eq!(10, config.request_timeout);
        assert_eq!("None", config.security_policy);
        assert_eq!("None", config.security_mode);
        assert_eq!(None, config.auth_certificate);
        assert_eq!(None, config.auth_private_key);
        assert_eq!("root", config.username.unwrap());
        assert_eq!("taosdata", config.password.unwrap());
        assert_eq!(AuthMethod::UserName, config.auth_method);
    }

    #[test]
    fn test_parse_endpoint() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let endpoint = UaConnectConfig::parse_endpoint(&dsn);
        assert!(endpoint.is_err());
        assert_eq!("endpoint is required", endpoint.unwrap_err().to_string());

        let dsn = Dsn::from_str("opc://localhost").unwrap();
        let endpoint = UaConnectConfig::parse_endpoint(&dsn);
        assert!(endpoint.is_err());
        assert_eq!("port is required", endpoint.unwrap_err().to_string());

        let dsn = Dsn::from_str("opc://:1234/").unwrap();
        let endpoint = UaConnectConfig::parse_endpoint(&dsn);
        assert!(endpoint.is_err());
        assert_eq!("host is required", endpoint.unwrap_err().to_string());

        let dsn = Dsn::from_str("opc://localhost:1234/").unwrap();
        let endpoint = UaConnectConfig::parse_endpoint(&dsn).unwrap();
        assert_eq!("opc.tcp://localhost:1234/", endpoint);

        let dsn = Dsn::from_str("opc://localhost:1234/subject").unwrap();
        let endpoint = UaConnectConfig::parse_endpoint(&dsn).unwrap();
        assert_eq!("opc.tcp://localhost:1234/subject", endpoint);
    }

    #[test]
    fn test_parse_connect_timeout() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let timeout = UaConnectConfig::parse_connect_timeout(&dsn).unwrap();
        assert_eq!(10, timeout);

        let dsn = Dsn::from_str("opc://?connect_timeout=123").unwrap();
        let timeout = UaConnectConfig::parse_connect_timeout(&dsn).unwrap();
        assert_eq!(123, timeout);

        let dsn = Dsn::from_str("opc://?connect_timeout=abc").unwrap();
        let timeout = UaConnectConfig::parse_connect_timeout(&dsn);
        assert!(timeout.is_err());
        assert_eq!(
            "parse connection_timeout failed, cause: invalid digit found in string",
            timeout.unwrap_err().to_string()
        );
    }

    #[test]
    #[ignore]
    fn test_certificate_file() {
        let dsn = Dsn::from_str("opc://localhost:7080?certificate=@/tmp/cert").unwrap();
        let config = UaConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("/tmp/cert", config.certificate.unwrap());

        let dsn = Dsn::from_str("opc://localhost:7080?certificate=abc").unwrap();
        let config = UaConnectConfig::from_dsn(&dsn).unwrap();
        println!("{:?}", config.certificate);
        assert!(config.certificate.is_none());
    }

    #[test]
    fn test_parse_request_timeout() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let timeout = UaConnectConfig::parse_request_timeout(&dsn).unwrap();
        assert_eq!(10, timeout);

        let dsn = Dsn::from_str("opc://?request_timeout=123").unwrap();
        let timeout = UaConnectConfig::parse_request_timeout(&dsn).unwrap();
        assert_eq!(123, timeout);

        let dsn = Dsn::from_str("opc://?request_timeout=abc").unwrap();
        let timeout = UaConnectConfig::parse_request_timeout(&dsn);
        assert!(timeout.is_err());
        assert_eq!(
            "parse request_timeout failed, cause: invalid digit found in string",
            timeout.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_security_policy() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let policy = UaConnectConfig::parse_security_policy(&dsn);
        assert_eq!("None", policy);

        let dsn = Dsn::from_str("opc://?security_policy=abc").unwrap();
        let policy = UaConnectConfig::parse_security_policy(&dsn);
        assert_eq!("abc", policy);
    }

    #[test]
    fn test_parse_security_mode() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let mode = UaConnectConfig::parse_security_mode(&dsn);
        assert_eq!("None", mode);

        let dsn = Dsn::from_str("opc://?security_mode=abc").unwrap();
        let mode = UaConnectConfig::parse_security_mode(&dsn);
        assert_eq!("abc", mode);
    }
}
