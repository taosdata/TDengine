//! TaosX's backup file format
//!
//!
use chrono::DateTime;
use chrono::Local;
use chrono::TimeZone;
use std::fmt::Display;
use std::io::prelude::*;
use taos::*;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

bitflags::bitflags! {
    pub struct DataType: u8 {
        const IS_DATA = 0b00000001;
        const IS_META = 0b00000010;
    }
}

const Z_CURRENT_VERSION: Version = Version(0, 0);

/// A version repr: `(compatible_version, patch_version)`.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Version(u8, u8);

impl Version {
    // pub const CURRENT: Version = Z_CURRENT_VERSION;
}
impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.0, self.1))
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.0.partial_cmp(&other.0) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.1.partial_cmp(&other.1)
    }
}

#[derive(Debug)]
pub struct Header {
    version: Version,
    created: DateTime<Local>,
    database: Option<String>,
}

impl Header {
    pub fn new(database: impl Into<Option<String>>) -> Self {
        Self {
            version: Z_CURRENT_VERSION,
            created: Local::now(),
            database: database.into(),
        }
    }
}

impl Inlinable for Header {
    fn read_inlined<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let _ = reader.read_u32()?;
        let version = Version(reader.read_u8()?, reader.read_u8()?);
        if version.0 != Z_CURRENT_VERSION.0 {
            panic!(
                "We're so sorry that we cant read in-compatible version {} at {} app",
                version, Z_CURRENT_VERSION
            );
        }
        let ts = reader.read_u64()?;
        let created = Local.timestamp_millis_opt(ts as _).unwrap();
        let database = reader.read_inlined_str::<1>()?;
        Ok(Self {
            version,
            created,
            database: if database.len() > 0 {
                Some(database)
            } else {
                None
            },
        })
    }

    fn write_inlined<W: Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        let mut l = wtr.write("TAOZ".as_bytes())?;
        l += wtr.write(&[self.version.0, self.version.1])?;
        l += wtr.write_i64_le(self.created.naive_utc().timestamp_millis())?;
        if let Some(database) = &self.database {
            l += wtr.write_inlined_str::<1>(&database)?;
        } else {
            wtr.write_inlined_str::<1>(&"")?;
        }
        Ok(l)
    }
}

#[async_trait::async_trait]
impl taos::AsyncInlinable for Header {
    async fn read_inlined<R: AsyncRead + Send + Unpin>(reader: &mut R) -> std::io::Result<Self> {
        let _ = reader.read_u32().await?;
        let version = Version(reader.read_u8().await?, reader.read_u8().await?);
        if version.0 != Z_CURRENT_VERSION.0 {
            panic!(
                "We're so sorry that we cant read in-compatible version {} at {} app",
                version, Z_CURRENT_VERSION
            );
        }
        let ts = reader.read_u64().await?;
        let created = Local.timestamp_millis_opt(ts as _).unwrap();
        let database = reader.read_inlined_str::<1>().await?;
        Ok(Self {
            version,
            created,
            database: if database.len() > 0 {
                Some(database)
            } else {
                None
            },
        })
    }

    async fn write_inlined<W: AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        let mut l = wtr.write("TAOZ".as_bytes()).await?;
        l += wtr.write(&[self.version.0, self.version.1]).await?;
        wtr.write_i64(self.created.naive_utc().timestamp_millis())
            .await?;
        l += std::mem::size_of::<i64>();
        if let Some(database) = &self.database {
            l += wtr.write_inlined_str::<1>(&database).await?;
        } else {
            wtr.write_inlined_str::<1>(&"").await?;
        }
        Ok(l)
    }
}

#[cfg(test)]
mod tests {
    use super::Header;
    #[test]
    fn test_inline() {
        use taos::InlinableWrite;

        let header = Header::new("abc".to_string());
        let mut bytes = Vec::new();
        bytes.resize(18, 0);

        let len = bytes.as_mut_slice().write_inlinable(&header).unwrap();
        assert!(len > 0);
        assert_eq!(bytes.len(), len);
    }
}
