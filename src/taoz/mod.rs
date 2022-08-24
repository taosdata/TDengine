//! TaosX's backup file format
//!
//!

use std::io::prelude::*;
use std::io::Result as IoResult;
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::Path;
use std::path::PathBuf;


use chrono::Local;
use futures::FutureExt;
use taos::*;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;



mod header;

use async_compression::{tokio::write::ZstdEncoder, Level};
pub use header::*;
use tokio::io::BufReader;


type ZFileInner = ZCodec<ZstdEncoder<BufReader<tokio::fs::File>>>;

/// Construct a backup file with name pattern `{prefix}-{timestamp}.z`.
///
/// Automatically create new file when file reach the max_file_size
pub struct ZFile {
    file: ZCodec<ZstdEncoder<BufReader<tokio::fs::File>>>,
    prefix: PathBuf,
    level: Level,
    current_size: usize,
    max_file_size: u64,
    version: Version,
}

async fn new_z_file(
    prefix: impl AsRef<Path>,
    compression_level: async_compression::Level,
) -> IoResult<ZFileInner> {
    let prefix = prefix.as_ref().to_path_buf();
    let now = Local::now();
    let timestamp = now.timestamp();
    let file = File::create(format!("{}-{timestamp}.z", prefix.display())).await?;
    let wtr = BufReader::new(file);
    let wtr = async_compression::tokio::write::ZstdEncoder::with_quality(wtr, compression_level);
    let mut file = ZCodec::new(wtr);
    file.write_head_async(&Header::new(None)).await?;
    Ok(file)
}

impl ZFile {
    pub async fn new(
        prefix: impl AsRef<Path>,
        compression_level: async_compression::Level,
    ) -> IoResult<Self> {
        let prefix = prefix.as_ref().to_path_buf();
        let file = new_z_file(&prefix, compression_level).await?;
        let max_file_size = 1 * 1024 * 1024 * 1024;
        Ok(Self {
            file,
            prefix,
            level: compression_level,
            current_size: 0,
            max_file_size,
            version: Version::CURRENT,
        })
    }

    pub async fn check_or_next(&mut self) -> IoResult<()> {
        if self.current_size as u64 >= self.max_file_size {
            self.file.flush().await?;
            self.file.shutdown().await?;
            self.file = new_z_file(&self.prefix, self.level).await?;
            self.current_size = 0;
        }
        Ok(())
    }

    pub async fn write_meta(&mut self, meta: &RawMeta) -> IoResult<()> {
        self.current_size += self.file.write_meta_async(meta).await?;
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

    pub async fn write_meta_async(&mut self, meta: &RawMeta) -> std::io::Result<usize> {
        self.0.write(&[DataType::IS_META.bits()]).await?;
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

impl<R> ZCodec<R>
where
    R: AsyncRead + Unpin + Send,
{
    pub async fn header_async(&mut self) -> IoResult<Header> {
        AsyncInlinable::read_inlined(&mut self.0).await
    }

    pub async fn read_message_async(&mut self) -> IoResult<MessageSet<RawMeta, Vec<RawBlock>>> {
        let msg_type = self.0.read_u8().await?;
        let data_type = DataType::from_bits(msg_type).ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid data type or broken backup file",
        ))?;
        if data_type == DataType::IS_META {
            return <RawMeta as AsyncInlinable>::read_inlined(&mut self.0)
                .await
                .map(MessageSet::Meta);
        } else {
            let mut data = Vec::new();
            loop {
                if let Some(raw) =
                    <RawBlock as AsyncInlinable>::read_optional_inlined(&mut self.0).await?
                {
                    data.push(raw);
                } else {
                    break;
                }
            }

            Ok(MessageSet::Data(data))
        }
    }
}
#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
    };

    use futures::TryFutureExt;
    use taos::TBuilder;

    use super::*;

    #[tokio::test]
    async fn write() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
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
            .write_head_async(&Header::new(db.to_string()))
            .await?;

        let mut tmq = TmqBuilder::from_dsn("taos:///?group.id=c")?.build()?;
        tmq.subscribe([db]).await?;
        let writer = Arc::new(Mutex::new(writer));

        let rows = tmq
            .stream_with_timeout(Timeout::from_millis(500))
            .map_err(anyhow::Error::from)
            .map_ok(|(offset, message)| async {
                let mut rows = 0;
                let mut writer = writer.lock().unwrap();
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
                            // log::info!("");
                            log::info!(
                                "table {} rows: {}",
                                block.table_name().unwrap(),
                                block.nrows()
                            );
                        }
                        writer.finish_data_async().await.unwrap();
                    }
                }
                writer.flush().await.unwrap();
                tmq.commit(offset).await?;
                anyhow::Result::<usize>::Ok(rows)
            })
            .try_fold(0, |sum, n| async move { Ok(n.await? + sum) })
            .await?;
        let mut writer = writer.lock().unwrap();
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
    async fn read() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
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
                    MessageSet::Meta(meta) => {
                        dbg!(&meta);
                        taos.write_raw_meta(meta).await?
                    }
                    MessageSet::Data(data) => {
                        // dbg!(&data);
                        for raw in data {
                            rows += raw.nrows();
                            taos.write_raw_block(&raw).await?;
                        }
                        println!("rows: {}", rows);
                        // taos.write_raw_data(data[0]).await?
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
