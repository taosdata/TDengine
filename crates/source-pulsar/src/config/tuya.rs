use async_trait::async_trait;
use pulsar::{authentication::Authentication, error::AuthenticationError};

pub const ENCRYPT_MODEL: &str = "em";
pub const KEY_START: usize = 8;
pub const KEY_END: usize = 24;

#[derive(Debug, Clone)]
pub struct EnvInner {
    pub key: String,
    pub value: String,
    pub description: String,
}
#[derive(Debug, Clone)]
pub enum TuyaEnv {
    Prod(EnvInner),
    Test(EnvInner),
}

impl TuyaEnv {
    pub fn inner(&self) -> &EnvInner {
        match self {
            Self::Prod(config) => config,
            Self::Test(config) => config,
        }
    }

    pub fn get_key(&self) -> &str {
        &self.inner().key
    }

    pub fn get_value(&self) -> &str {
        &self.inner().value
    }

    pub fn get_description(&self) -> &str {
        &self.inner().description
    }
}

impl TryFrom<&str> for TuyaEnv {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> anyhow::Result<Self, Self::Error> {
        match value {
            "prod" => Ok(Self::Prod(EnvInner {
                key: "prod".into(),
                value: "event".into(),
                description: "online environment".into(),
            })),
            "test" => Ok(Self::Test(EnvInner {
                key: "test".into(),
                value: "event-test".into(),
                description: "test environment".into(),
            })),
            _ => anyhow::bail!("tuya env must be prod or test"),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TuyaMessage {
    pub data: String,
    pub protocol: i32,
    pub pv: String,
    pub sign: String,
    pub t: i64,
}

#[derive(Debug)]
pub struct TuyaAuthentication {
    pub access_id: String,
    pub access_key: String,
}

impl TuyaAuthentication {
    pub fn new(access_id: String, access_key: String) -> Box<Self> {
        Box::new(Self {
            access_id,
            access_key,
        })
    }
}

#[async_trait]
impl Authentication for TuyaAuthentication {
    fn auth_method_name(&self) -> String {
        "auth1".to_string()
    }

    async fn initialize(&mut self) -> Result<(), AuthenticationError> {
        Ok(())
    }

    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
        let data = format!(
            "{}",
            serde_json::json!({
                "username": self.access_id,
                "password": gen_password(&self.access_id, &self.access_key),
            })
        );
        Ok(data.as_bytes().to_vec())
    }
}

pub fn gen_password(access_id: &str, access_key: &str) -> String {
    let key_hash = format!("{:x}", md5::compute(access_key.as_bytes()));

    // md5(accessId + key_hash), required by tuya api
    let concat = format!("{}{}", access_id, key_hash);
    let concat_hash = format!("{:x}", md5::compute(concat.as_bytes()));

    // 取 [8..24] 共 16 个字符
    let password = concat_hash
        .get(8..24)
        .expect("md5 hex always 32 chars")
        .to_string();

    tracing::debug!(
        "pulsar tuya gen password, access_id: {:?}, access_key: {:?}, password: {:?}",
        access_id,
        access_key,
        password
    );

    password
}
