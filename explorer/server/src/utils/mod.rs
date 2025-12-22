pub mod aes;
pub mod cbc;

pub fn deserialize_non_empty_string<'de, D>(
    deserializer: D,
) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: String = <String as serde::Deserialize>::deserialize(deserializer)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(serde::de::Error::custom("must not be empty"));
    }
    Ok(trimmed.to_string())
}
