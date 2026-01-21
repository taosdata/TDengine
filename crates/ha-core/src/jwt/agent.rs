use super::Result;

static SECRET: &[u8] = &[
    126, 222, 130, 137, 43, 122, 41, 173, 144, 146, 116, 138, 153, 244, 251, 99, 50, 55, 140, 238,
    218, 232, 15, 161, 226, 54, 130, 40, 211, 234, 111, 171,
];

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AgentClaims {
    /// The agent id
    pub sub: i64,
    /// Unix epoch in seconds for created time.
    pub iat: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AgentToken(pub String);

impl AgentToken {
    pub fn jwt_decode(&self) -> Result<AgentClaims> {
        jwt_decode(&self.0)
    }
}

pub fn jwt_decode(token: &str) -> Result<AgentClaims> {
    super::jwt_decode(token, SECRET)
}

pub fn jwt_encode(claims: &AgentClaims) -> Result<String> {
    super::jwt_encode(claims, SECRET)
}

impl std::ops::Deref for AgentToken {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: AsRef<[u8]>> From<T> for AgentToken {
    fn from(value: T) -> Self {
        Self(String::from_utf8_lossy(value.as_ref()).to_string())
    }
}

impl std::fmt::Display for AgentToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
