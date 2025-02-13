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
    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
    pub struct DataType: u8 {
        const IS_DATA = 0b00000001;
        const IS_META = 0b00000010;
        const IS_RAW = 0b00000100;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RawType {
    Meta = 1,
    Data = 2,
    Both = 3,
    Raw = 4,
}

impl From<u8> for RawType {
    fn from(v: u8) -> Self {
        match v {
            1 => RawType::Meta,
            2 => RawType::Data,
            3 => RawType::Both,
            4 => RawType::Raw,
            _ => panic!("Invalid RawType: {}", v),
        }
    }
}

const Z_ZERO_VERSION: Version = Version(0, 0);

const Z_CURRENT_VERSION: Version = Version(1, 0);

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
    // The backup file format version.
    version: Version,
    /// The taosx api version that the backup was created from.
    api_version: Option<String>,
    /// The server version that the backup was created from.
    server_version: Option<String>,
    /// Created time of the backup file.
    created: DateTime<Local>,
    /// The database name that the backup was created from.
    database: Option<String>,
}

impl Header {
    pub fn new(
        taosx_version: impl Into<String>,
        taosd_version: impl Into<String>,
        database: impl Into<Option<String>>,
    ) -> Self {
        Self {
            version: Z_CURRENT_VERSION,
            api_version: Some(taosx_version.into()),
            server_version: Some(taosd_version.into()),
            created: Local::now(),
            database: database.into(),
        }
    }

    pub fn created(&self) -> DateTime<Local> {
        self.created
    }

    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn api_version(&self) -> Option<&str> {
        self.api_version.as_deref()
    }

    pub fn server_version(&self) -> Option<&str> {
        self.server_version.as_deref()
    }
}

impl Inlinable for Header {
    fn read_inlined<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let _ = reader.read_u32()?;
        let version = Version(reader.read_u8()?, reader.read_u8()?);
        if version > Z_CURRENT_VERSION {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!(
                    "We're so sorry that we can't read in-compatible version {} with {} app",
                    version, Z_CURRENT_VERSION
                ),
            );
        }
        let mut api_version = None;
        let mut server_version = None;
        if version > Z_ZERO_VERSION {
            api_version.replace(reader.read_inlined_str::<1>()?);
            server_version.replace(reader.read_inlined_str::<1>()?);
        }
        let ts = reader.read_u64()?;
        let created = Local.timestamp_millis_opt(ts as _).unwrap();
        let database = reader.read_inlined_str::<1>()?;
        Ok(Self {
            version,
            api_version,
            server_version,
            created,
            database: if !database.is_empty() {
                Some(database)
            } else {
                None
            },
        })
    }

    fn write_inlined<W: Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        assert!(
            self.api_version.is_some() && self.server_version.is_some(),
            "api_version and server_version must be set"
        );
        let mut l = wtr.write("TAOZ".as_bytes())?;
        l += wtr.write(&[self.version.0, self.version.1])?;
        l += wtr.write_inlined_str::<1>(self.api_version.as_ref().unwrap())?;
        l += wtr.write_inlined_str::<1>(self.server_version.as_ref().unwrap())?;
        l += wtr.write_i64_le(self.created.timestamp_millis())?;
        if let Some(database) = &self.database {
            l += wtr.write_inlined_str::<1>(database)?;
        } else {
            wtr.write_inlined_str::<1>("")?;
        }
        Ok(l)
    }
}

#[async_trait::async_trait]
impl taos::AsyncInlinable for Header {
    async fn read_inlined<R: AsyncRead + Send + Unpin>(reader: &mut R) -> std::io::Result<Self> {
        let _ = reader.read_u32().await?;
        let version = Version(reader.read_u8().await?, reader.read_u8().await?);
        // For backward compatibility, we can read the old version.
        if version > Z_CURRENT_VERSION {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!(
                    "We're so sorry that we can't read in-compatible version {} with {} app",
                    version, Z_CURRENT_VERSION
                ),
            );
        }
        let mut api_version = None;
        let mut server_version = None;
        if version > Z_ZERO_VERSION {
            api_version.replace(reader.read_inlined_str::<1>().await?);
            server_version.replace(reader.read_inlined_str::<1>().await?);
        }
        let ts = reader.read_u64().await?;
        let created = Local.timestamp_millis_opt(ts as _).unwrap();
        let database = reader.read_inlined_str::<1>().await?;
        Ok(Self {
            version,
            api_version,
            server_version,
            created,
            database: if !database.is_empty() {
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
        assert!(
            self.api_version.is_some() && self.server_version.is_some(),
            "api_version and server_version must be set"
        );
        let mut l = wtr.write("TAOZ".as_bytes()).await?;
        l += wtr.write(&[self.version.0, self.version.1]).await?;
        l += wtr
            .write_inlined_str::<1>(self.api_version.as_ref().unwrap())
            .await?;
        l += wtr
            .write_inlined_str::<1>(self.server_version.as_ref().unwrap())
            .await?;
        wtr.write_i64(self.created.timestamp_millis()).await?;
        l += std::mem::size_of::<i64>();
        if let Some(database) = &self.database {
            l += wtr.write_inlined_str::<1>(database).await?;
        } else {
            wtr.write_inlined_str::<1>("").await?;
        }
        Ok(l)
    }
}

#[cfg(test)]
mod tests {
    use super::Header;
    use taos::{Inlinable, InlinableWrite};

    #[test]
    fn test_inline() {
        let header = Header::new("1.6.0", "3.3.0.0", "abc".to_string());
        let mut bytes = vec![0; 32];

        let len = bytes.as_mut_slice().write_inlinable(&header).unwrap();
        assert!(len > 0);
        assert_eq!(bytes.len(), len);

        let mut reader = std::io::Cursor::new(bytes);
        let header2 = Header::read_inlined(&mut reader).unwrap();
        dbg!(header, header2);
    }
}
