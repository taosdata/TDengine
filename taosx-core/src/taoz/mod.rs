//! TaosX's backup file format
//!
//!

use std::io::Result as IoResult;
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::Path;
use std::path::PathBuf;

use chrono::Local;
use taos::taos_query::common::RawData;
use taos::*;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

mod header;

use crate::dsv::DataSourceValidation;
use async_compression::{tokio::write::ZstdEncoder, Level};
pub use header::*;
use tokio::io::BufReader;

type ZFileInner = ZCodec<ZstdEncoder<BufReader<tokio::fs::File>>>;

/// Construct a backup file with name pattern `{prefix}-{timestamp}.z`.
///
/// Automatically create new file when file reach the max_file_size
pub struct ZFile {
    path: PathBuf,
    file: ZCodec<ZstdEncoder<BufReader<tokio::fs::File>>>,
    prefix: PathBuf,
    level: Level,
    current_size: usize,
    max_file_size: u64,
    api_version: String,
    server_version: String,
    move_to: Option<PathBuf>,
}

async fn new_z_file(
    prefix: impl AsRef<Path>,
    compression_level: Level,
    api_version: &str,
    server_version: &str,
) -> IoResult<(PathBuf, ZFileInner)> {
    let prefix = prefix.as_ref().to_path_buf();
    let now = Local::now();
    let timestamp = now.timestamp();
    let path = PathBuf::from(format!("{}-{timestamp}.z", prefix.display()));
    let file = File::create(&path).await?;
    let wtr = BufReader::new(file);
    let wtr = ZstdEncoder::with_quality(wtr, compression_level);
    let mut file = ZCodec::new(wtr);
    file.write_head_async(&Header::new(api_version, server_version, None))
        .await?;
    Ok((path, file))
}

impl ZFile {
    pub async fn new(
        prefix: impl AsRef<Path>,
        compression_level: Level,
        api_version: &str,
        server_version: &str,
    ) -> IoResult<Self> {
        let prefix = prefix.as_ref().to_path_buf();
        let (file_name, file) =
            new_z_file(&prefix, compression_level, api_version, server_version).await?;
        let max_file_size = 1024 * 1024 * 1024;
        Ok(Self {
            path: file_name,
            file,
            prefix,
            level: compression_level,
            current_size: 0,
            max_file_size,
            api_version: api_version.to_string(),
            server_version: server_version.to_string(),
            move_to: None,
        })
    }

    pub fn set_max_file_size(&mut self, max_file_size: u64) {
        self.max_file_size = max_file_size;
    }

    pub fn set_move_to(&mut self, move_to: Option<PathBuf>) {
        self.move_to = move_to;
    }

    pub async fn check_or_next(&mut self) -> IoResult<()> {
        if self.current_size as u64 >= self.max_file_size {
            self.file.flush().await?;
            self.file.shutdown().await?;

            match &self.move_to {
                Some(new_dir) => {
                    // move the current file to a new path
                    let file_path = &self.path;
                    if let Some(file_name) = file_path.file_name() {
                        let new_path = new_dir.clone().join(file_name);
                        tokio::fs::rename(file_path, new_path).await?;
                    }
                }
                None => {
                    // nothing
                }
            }

            (self.path, self.file) = new_z_file(
                &self.prefix,
                self.level,
                &self.api_version,
                &self.server_version,
            )
            .await?;
            self.current_size = 0;
        }
        Ok(())
    }

    pub async fn write_meta(&mut self, meta: &RawMeta) -> IoResult<()> {
        self.current_size += self.file.write_meta_async(meta).await?;
        self.check_or_next().await?;
        Ok(())
    }

    pub async fn write_raw(&mut self, raw: &RawData, raw_type: RawType) -> IoResult<()> {
        self.current_size += self.file.write_raw_async(raw, raw_type).await?;
        self.check_or_next().await?;
        Ok(())
    }

    pub async fn start_raw_block(&mut self) -> IoResult<()> {
        self.current_size += self.file.start_data_async().await?;
        Ok(())
    }

    pub async fn write_raw_block(&mut self, block: &RawBlock) -> IoResult<()> {
        self.current_size += self.file.write_data_async(block).await?;
        Ok(())
    }

    pub async fn finish_raw_block(&mut self) -> IoResult<()> {
        self.current_size += self.file.finish_data_async().await?;
        self.check_or_next().await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> IoResult<()> {
        self.file.flush().await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> IoResult<()> {
        tracing::debug!("shutdown file {}", self.prefix.display());
        self.file.shutdown().await?;
        Ok(())
    }
}

pub struct ZCodec<W>(W);

impl<W> Deref for ZCodec<W> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<W> DerefMut for ZCodec<W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<W> ZCodec<W> {
    pub fn new(wtr: W) -> Self {
        Self(wtr)
    }
}

impl<W> ZCodec<W>
where
    W: AsyncWrite + Unpin + Send,
{
    pub async fn write_head_async(&mut self, header: &Header) -> std::io::Result<usize> {
        self.0.write_inlinable(header).await
    }

    pub async fn write_raw_async(
        &mut self,
        raw: &RawData,
        raw_type: RawType,
    ) -> std::io::Result<usize> {
        self.0.write_all(&[DataType::IS_RAW.bits()]).await?;
        self.0.write_u8(raw_type as u8).await?;
        Ok(self.0.write_inlinable(raw).await? + std::mem::size_of::<DataType>() + 1)
    }

    pub async fn write_meta_async(&mut self, meta: &RawMeta) -> std::io::Result<usize> {
        self.0.write_all(&[DataType::IS_META.bits()]).await?;
        Ok(self.0.write_inlinable(meta).await? + std::mem::size_of::<DataType>())
    }

    pub async fn start_data_async(&mut self) -> IoResult<usize> {
        self.0.write_all(&[DataType::IS_DATA.bits()]).await?;
        Ok(std::mem::size_of::<DataType>())
    }
    pub async fn write_data_async(&mut self, data: &RawBlock) -> IoResult<usize> {
        self.0.write_inlinable(data).await
    }
    pub async fn finish_data_async(&mut self) -> IoResult<usize> {
        self.0.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).await?;
        Ok(4)
    }
}

pub enum ZMessage {
    Meta(RawMeta),
    Data(Vec<RawBlock>),
    Raw(RawType, RawData),
}

impl<R> ZCodec<R>
where
    R: AsyncRead + Unpin + Send,
{
    pub async fn header_async(&mut self) -> IoResult<Header> {
        AsyncInlinable::read_inlined(&mut self.0).await
    }

    pub async fn read_message_async(&mut self) -> IoResult<ZMessage> {
        let msg_type = self.0.read_u8().await?;
        let data_type = DataType::from_bits(msg_type).ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid data type or broken backup file",
        ))?;

        if data_type == DataType::IS_META {
            let meta = <taos::RawMeta as taos::AsyncInlinable>::read_inlined(&mut self.0).await?;
            Ok(ZMessage::Meta(meta))
        } else if data_type == DataType::IS_DATA {
            let mut data = Vec::new();
            while let Some(raw) =
                <taos::RawBlock as taos::AsyncInlinable>::read_optional_inlined(&mut self.0).await?
            {
                data.push(raw);
            }
            Ok(ZMessage::Data(data))
        } else if data_type == DataType::IS_RAW {
            let raw_type: RawType = self.0.read_u8().await?.into();
            let raw = <taos::taos_query::common::RawData as taos::AsyncInlinable>::read_inlined(
                &mut self.0,
            )
            .await?;
            Ok(ZMessage::Raw(raw_type, raw))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid data type or broken backup file",
            ))
        }
    }
}

pub async fn is_taos_valid(dsn: &Dsn) -> DataSourceValidation {
    if dsn.subject.is_none() {
        return DataSourceValidation::invalid(
            "taos".to_string(),
            "Database is required.".to_string(),
        );
    }
    let builder = TaosBuilder::from_dsn(dsn);
    match builder {
        Err(err) => DataSourceValidation::invalid(
            "taos".to_string(),
            format!("invalid dsn: {}, cause: {}", dsn, err),
        ),
        Ok(b) => {
            let conn = b.build().await;
            match conn {
                Err(err) => DataSourceValidation::invalid(
                    "taos".to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                ),
                Ok(c) => {
                    let version = c.server_version().await;
                    match version {
                        Err(err) => DataSourceValidation::invalid(
                            "taos".to_string(),
                            format!(
                                "failed to get server version from dsn: {}, cause: {}",
                                dsn, err
                            ),
                        ),
                        Ok(v) => DataSourceValidation {
                            valid: true,
                            support: true,
                            data_source: "taos".to_string(),
                            version: Some(v.to_string()),
                            message: None,
                            namespaces: None,
                        },
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_is_taos_valid_timeout() {
        let dsn = Dsn::from_str("taos+ws://unknown_user:unknown_pass@ec2-35-86-78-3.us-west-2.compute.amazonaws.com:6041/test").unwrap();
        // let dsv = is_taos_valid(&dsn).await;
        let timeout = std::time::Duration::from_secs(5);
        let timeout = tokio::time::timeout(timeout, is_taos_valid(&dsn)).await;

        match timeout {
            Err(err) => {
                println!("timeout: {}", err);
            }
            Ok(_) => {
                unreachable!("should not reach here");
            }
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_is_taos_valid() {
        // taos
        let dsn = Dsn::from_str("taos+ws://root:taosdata@192.168.1.40:6041").unwrap();
        let dsv = is_taos_valid(&dsn).await;
        assert!(dsv.valid);
        assert!(dsv.support);
        assert_eq!("taos", dsv.data_source);
        assert_eq!("2.6.0.27", dsv.version.unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn write() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        pretty_env_logger::formatted_builder().filter_level(log::LevelFilter::Debug);
        taos.exec_many([
            "drop topic if exists abc1",
            "create topic abc1 with meta as database abc1",
            "use abc1",
        ])
        .await?;

        // let writer = std::fs::File::create("abc1.test.z")?;
        let writer = tokio::fs::File::create("abc1.test.bin").await?;

        let writer = async_compression::tokio::write::ZstdEncoder::new(writer);
        let mut writer = ZCodec::new(writer);
        // let writer =
        let db = "abc1";
        writer
            .write_head_async(&Header::new("1.6.0", "3.3.0.0", db.to_string()))
            .await?;

        let mut tmq = TmqBuilder::from_dsn("taos:///?group.id=c")?.build().await?;
        tmq.subscribe([db]).await?;
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        let rows = tmq
            .stream_with_timeout(Timeout::from_millis(500))
            .map_err(anyhow::Error::from)
            .map_ok(|(offset, message)| async {
                let mut rows = 0;
                let mut writer = writer.lock().await;
                match message {
                    MessageSet::Meta(meta) => {
                        // dbg!(meta.as_json_meta().await?);
                        writer
                            .write_meta_async(&meta.as_raw_meta().await?)
                            .await
                            .unwrap();
                    }
                    MessageSet::Data(data) => {
                        writer.start_data_async().await.unwrap();
                        while let Some(block) = data.fetch_raw_block().await.unwrap() {
                            // dbg!(&block);
                            let _len = writer.write_data_async(&block).await.unwrap();
                            rows += block.nrows();
                            // dbg!(len);
                            // tracing::info!("");
                            tracing::info!(
                                "table {} rows: {}",
                                block.table_name().unwrap(),
                                block.nrows()
                            );
                        }
                        writer.finish_data_async().await.unwrap();
                    }
                    _ => unreachable!(),
                }
                writer.flush().await.unwrap();
                tmq.commit(offset).await?;
                anyhow::Result::<usize>::Ok(rows)
            })
            .try_fold(0, |sum, n| async move { Ok(n.await? + sum) })
            .await?;
        let mut writer = writer.lock().await;
        writer.flush().await?;
        writer.shutdown().await?;
        // let mut bytes = Vec::with_capacity(10000);
        // bytes.resize(10000, 0xffu8);
        // writer.write_all(&bytes).await?;
        // writer.deref_mut().shutdown().await?;
        println!("backup {} rows in database {}", rows, db);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn read() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop database if exists abc3",
            "create database if not exists abc3",
            "use abc3",
        ])
        .await?;

        let reader = tokio::fs::File::open("abc1.test.bin").await?;
        let reader = tokio::io::BufReader::new(reader);

        let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);

        let mut reader = ZCodec::new(reader);

        let header = reader.header_async().await?;
        dbg!(header);

        // let mut rows = AtomicU64::new(0);
        let mut rows = 0;

        loop {
            let res = reader.read_message_async().await;
            match res {
                Ok(message) => match message {
                    ZMessage::Meta(meta) => {
                        dbg!(&meta);
                        taos.write_raw_meta(&meta).await?
                    }
                    ZMessage::Data(data) => {
                        // dbg!(&data);
                        for raw in data {
                            rows += raw.nrows();
                            taos.write_raw_block(&raw).await?;
                        }
                        println!("rows: {}", rows);
                        // taos.write_raw_data(data[0]).await?
                    }
                    ZMessage::Raw(raw_type, raw) => {
                        dbg!(&raw_type, &raw);
                        let meta = raw.into();
                        taos.write_raw_meta(&meta).await?
                    }
                },
                Err(err) => {
                    dbg!(&err);
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    dbg!(&err);
                    break;
                }
            }
        }
        println!("total {} rows", rows);
        Ok(())
    }
}
