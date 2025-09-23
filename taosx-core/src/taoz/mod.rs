use anyhow::{bail, Context};
use async_compression::tokio::write::ZstdEncoder;
use async_compression::zstd::CParameter;
use chrono::{DateTime, Utc};
use std::io::Result as IoResult;
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use taos::taos_query::common::RawData;
use taos::*;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::time::Instant;

use crate::dsv::DataSourceValidation;

pub use header::*;

mod header;

#[derive(Debug, Clone)]
pub struct ZFileName {
    /// zfile 的原始路径
    pub raw_path: Option<PathBuf>,
    /// zfile 对应的 topic 名称
    pub topic: String,
    /// zfile 对应的备份时间戳
    pub timestamp: Option<DateTime<Utc>>,
    /// vgroup id
    pub vg_id: i32,
    /// zfile 的 index
    pub index: u64,
}

impl FromStr for ZFileName {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ZFile::parse_file_name(s)
            .map(|(tp, ts, vg, seq)| ZFileName {
                raw_path: None,
                topic: tp,
                timestamp: Some(ts),
                vg_id: vg,
                index: seq,
            })
            .map_err(|_| ())
    }
}

impl std::fmt::Display for ZFileName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            ZFile::file_name((self.topic.as_str(), self.timestamp, self.vg_id, self.index))
        )
    }
}

impl ZFileName {
    pub fn from_path(path: impl AsRef<Path>) -> Option<Self> {
        path.as_ref()
            .file_name()
            .and_then(|f| f.to_str())
            .and_then(|f| f.parse::<ZFileName>().ok())
            .map(|mut f| {
                f.raw_path = Some(path.as_ref().to_path_buf());
                f
            })
    }

    pub fn compare(a: &ZFileName, b: &ZFileName) -> std::cmp::Ordering {
        a.topic
            .cmp(&b.topic)
            .then_with(|| match a.timestamp.cmp(&b.timestamp) {
                std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                std::cmp::Ordering::Equal => match a.vg_id.cmp(&b.vg_id) {
                    std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                    std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                    std::cmp::Ordering::Equal => a.index.cmp(&b.index),
                },
            })
    }
}

// type ZFileInner = ZCodec<ZstdEncoder<BufReader<File>>>;
type ZFileInner = ZCodec<ZstdEncoder<BufWriter<File>>>;

/// Construct a ZFile with name pattern `{prefix}-{timestamp}.z`.
///
/// Automatically create new file when file reach the max_file_size
pub struct ZFile {
    /// taosx version
    api_version: String,
    /// taosd version
    server_version: String,
    /// 文件所在的目录
    dir: PathBuf,
    /// 文件名：$TOPIC-$TIMESTAMP-$VG_ID-$INDEX.z
    name: (String, Option<DateTime<Utc>>, i32, u64),
    /// 压缩级别
    compression_level: async_compression::Level,
    /// 最大文件大小
    max_file_size: u64,
    /// 文件写完后，移动到的目录
    move_to: Option<PathBuf>,
    /// 写入超时，如果文件不为空，且超过 timeout 没有写入新数据，则关闭当前文件
    timeout: Duration,

    /// 当前文件路径
    current_file: PathBuf,
    /// writer
    writer: ZFileInner,
    /// 当前文件大小
    current_size: usize,
    /// 最后一次写入
    last_modified: Option<Instant>,
}

impl ZFile {
    pub async fn new(
        api_version: &str,
        server_version: &str,
        file_dir: impl AsRef<Path>,
        file_name: (&str, Option<DateTime<Utc>>, i32, u64),
        compression_level: async_compression::Level,
        max_file_size: u64,
        move_to: Option<PathBuf>,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let (current_file, writer) = ZFile::new_writer(
            api_version,
            server_version,
            &file_dir,
            file_name,
            compression_level,
        )
        .await?;
        tracing::info!("created new file {}", current_file.display());

        Ok(Self {
            api_version: api_version.to_string(),
            server_version: server_version.to_string(),
            dir: file_dir.as_ref().to_path_buf(),
            name: (
                file_name.0.to_string(),
                file_name.1,
                file_name.2,
                file_name.3,
            ),
            compression_level,
            max_file_size,
            move_to,
            timeout,
            current_file,
            writer,
            current_size: 0,
            last_modified: None,
        })
    }

    fn get_file_name(&self) -> String {
        Self::file_name((self.name.0.as_str(), self.name.1, self.name.2, self.name.3))
    }

    /// 根据 topic, timestamp, vg_id, index 生成文件名。注意：timestamp 的秒和纳秒部分都会被忽略
    fn file_name(name: (&str, Option<DateTime<Utc>>, i32, u64)) -> String {
        let ts = match name.1 {
            None => Utc::now().timestamp_millis(),
            Some(t) => t.timestamp_millis(),
        };
        format!("{}-{}-{}-{}.z", name.0, ts, name.2, name.3)
    }

    pub fn parse_file_name(file_name: &str) -> anyhow::Result<(String, DateTime<Utc>, i32, u64)> {
        if !file_name.ends_with(".z") {
            bail!("invalid ZFile name: {}", file_name);
        }
        let splits = file_name
            .trim_end_matches(".z")
            .split('-')
            .collect::<Vec<_>>();
        if splits.len() != 4 {
            bail!("invalid ZFile name: {}", file_name);
        }

        let topic = splits[0].to_string();
        let ts = splits[1]
            .parse::<i64>()
            .with_context(|| format!("invalid timestamp in ZFile name: {}", file_name))?;

        let ts = DateTime::from_timestamp_millis(ts).ok_or(anyhow::anyhow!(
            "invalid timestamp in ZFile name: {}",
            file_name
        ))?;
        let vg_id = splits[2]
            .parse::<i32>()
            .with_context(|| format!("invalid vgroup id in ZFile name: {}", file_name))?;
        let index = splits[3]
            .parse::<u64>()
            .with_context(|| format!("invalid index in ZFile name: {}", file_name))?;

        Ok((topic, ts, vg_id, index))
    }

    async fn new_writer(
        api_version: &str,
        server_version: &str,
        dir: impl AsRef<Path>,
        name: (&str, Option<DateTime<Utc>>, i32, u64),
        compression_level: async_compression::Level,
    ) -> anyhow::Result<(PathBuf, ZFileInner)> {
        let file_name = Self::file_name(name);
        let path = dir.as_ref().to_path_buf().join(&file_name);

        if let Some(parent) = path.parent() {
            let exists = parent.exists();
            if !exists {
                tokio::fs::create_dir_all(parent).await.map_err(|err| {
                    std::io::Error::new(
                        err.kind(),
                        format!("Can't create dir {}: {err:#}", parent.display()),
                    )
                })?;
            } else if !parent.is_dir() {
                tracing::error!("parent path is not a directory: {}", parent.display());
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    parent.display().to_string(),
                );
            }
        }

        let file = File::create(&path).await.map_err(|err| {
            tracing::error!("Can't create file {}: {err:#}", path.display());
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Can't create file {}: {err:#}", path.display()),
            )
        })?;
        let wtr = BufWriter::new(file);
        // let wtr = BufReader::new(file);
        // let wtr = ZstdEncoder::with_quality(wtr, compression_level);
        let wtr = ZstdEncoder::with_quality_and_params(
            wtr,
            compression_level,
            &[CParameter::checksum_flag(true)],
        );
        let mut file = ZCodec::new(wtr);
        file.write_head_async(&Header::new(api_version, server_version, None))
            .await?;

        Ok((path, file))
    }

    /// 检查当前文件是否存在，防止备份文件被误删
    pub async fn check(&self) -> anyhow::Result<()> {
        if !self.current_file.exists() {
            bail!("file not exists: {}", self.current_file.display());
        }
        Ok(())
    }

    /// 如果当前文件的大小超过 max_file_size，或者(now - last_modify) > timeout，关闭当前文件，并创建新的文件
    pub async fn check_or_next(&mut self) -> anyhow::Result<()> {
        let timeout = if let Some(last_modified) = self.last_modified {
            self.current_size > 0 && (last_modified.elapsed() > self.timeout)
        } else {
            false
        };

        if self.current_size as u64 >= self.max_file_size || timeout {
            // 关闭当前文件
            self.writer
                .flush()
                .await
                .context("failed to flush ZFile writer")?;
            self.writer
                .shutdown()
                .await
                .context("failed to shutdown ZFile writer")?;
            tracing::info!("closed file {}", self.get_file_name());

            // 如果 name.1 为空，即：没有指定备份点的时间戳，则使用当前时间作为备份点的时间戳，并更新文件名
            if self.name.1.is_none() {
                let now = Utc::now();
                let new_name =
                    Self::file_name((self.name.0.as_str(), Some(now), self.name.2, self.name.3));

                let old_file = self.current_file.clone();
                let new_file = self.dir.clone().join(&new_name);
                tracing::debug!(
                    "rename file from {} to {}",
                    old_file.display(),
                    new_file.display()
                );
                tokio::fs::rename(old_file.as_path(), new_file.as_path())
                    .await
                    .with_context(|| {
                        format!(
                            "failed to rename file, old: {:?}, new: {:?}",
                            old_file, new_file,
                        )
                    })?;
                // keep metadata in sync
                self.name.1 = Some(now);
                self.current_file = new_file; // IMPORTANT: update current_file after rename
            }

            // 如果 move_to 不为空，则将当前备份文件移动到 move_to 目录
            self.move_to().await?;

            // create a new ZFile
            self.name.3 += 1;
            let (current_file, writer) = ZFile::new_writer(
                &self.api_version,
                &self.server_version,
                &self.dir.as_path(),
                (self.name.0.as_str(), self.name.1, self.name.2, self.name.3),
                self.compression_level,
            )
            .await
            .context("failed to create new ZFile")?;
            self.current_file = current_file;
            self.writer = writer;
            self.current_size = 0;
            self.last_modified = None;
            tracing::info!("created new file {}", self.get_file_name());
        }
        Ok(())
    }

    pub async fn write_meta(&mut self, meta: &RawMeta) -> anyhow::Result<()> {
        self.current_size += self.writer.write_meta_async(meta).await?;
        self.last_modified = Some(Instant::now());
        self.check_or_next()
            .await
            .context("failed to check or next")?;
        Ok(())
    }

    pub async fn write_raw(&mut self, raw: &RawData, raw_type: RawType) -> anyhow::Result<()> {
        self.current_size += self.writer.write_raw_async(raw, raw_type).await?;
        self.last_modified = Some(Instant::now());
        self.check_or_next()
            .await
            .context("failed to check or next")?;
        Ok(())
    }

    pub async fn start_raw_block(&mut self) -> anyhow::Result<()> {
        self.current_size += self.writer.start_data_async().await?;
        self.last_modified = Some(Instant::now());
        Ok(())
    }

    pub async fn write_raw_block(&mut self, block: &RawBlock) -> IoResult<()> {
        self.current_size += self.writer.write_data_async(block).await?;
        self.last_modified = Some(Instant::now());
        Ok(())
    }

    pub async fn finish_raw_block(&mut self) -> anyhow::Result<()> {
        self.current_size += self.writer.finish_data_async().await?;
        self.last_modified = Some(Instant::now());
        self.check_or_next()
            .await
            .context("failed to check or next")?;
        Ok(())
    }

    /// 如果 move_to 不为空，则将当前备份文件移动到 move_to 目录
    pub async fn move_to(&self) -> anyhow::Result<()> {
        if let Some(new_dir) = &self.move_to {
            // Ensure destination directory exists
            if !new_dir.exists() {
                tokio::fs::create_dir_all(new_dir).await.with_context(|| {
                    format!("failed to create move_to dir: {}", new_dir.display())
                })?;
            }
            let src = self.current_file.clone();
            let file_name = src
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| anyhow::anyhow!("invalid current_file name: {}", src.display()))?;
            let dst = new_dir.clone().join(file_name);
            if src != dst {
                tracing::info!("move file from {} to {}", src.display(), dst.display());
                tokio::fs::rename(&src, &dst)
                    .await
                    .context("failed to move ZFile")?;
            } else {
                tracing::info!("move skipped, src equals dst: {}", src.display());
            }
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> IoResult<()> {
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> IoResult<()> {
        tracing::debug!(
            "shutdown file {}",
            self.dir.clone().join(self.get_file_name()).display()
        );
        self.writer.shutdown().await?;
        Ok(())
    }

    /// 从目录中读取所有后缀为 .z 的备份文件
    pub async fn list_in_dir(dir: impl AsRef<Path>) -> anyhow::Result<Vec<ZFileName>> {
        let mut files = vec![];

        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                let file_name = ZFileName::from_path(&path);
                if let Some(zfile) = file_name {
                    files.push(zfile);
                }
            }
        }
        files.sort_by(ZFileName::compare);
        Ok(files)
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
            "invalid data type or broken ZFile",
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
                "invalid data type or broken ZFile",
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
    use super::*;
    use chrono::TimeZone;
    use std::str::FromStr;
    use std::sync::Arc;

    /// 两次备份任务的间隔时间必须大于 1 分钟，否则会导致文件名相同
    #[test]
    fn test_file_name() {
        let ts = Utc.with_ymd_and_hms(2021, 8, 27, 12, 0, 0).unwrap();
        let name = ZFile::file_name(("abc", Some(ts), 1, 1));
        assert_eq!("abc-1630065600000-1-1.z", name);

        let ts = Utc.with_ymd_and_hms(2021, 8, 27, 12, 0, 33).unwrap();
        let name = ZFile::file_name(("abc", Some(ts), 1, 1));
        assert_eq!("abc-1630065633000-1-1.z", name);

        let ts = Utc::now();
        let expect = format!("abc-{}-1-1.z", ts.timestamp_millis());
        let name = ZFile::file_name(("abc", Some(ts), 1, 1));
        assert_eq!(expect, name);
    }

    #[tokio::test]
    async fn test_list_zfile_in_dir() {
        // given
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut raw_files = vec![];
        for i in 1..=10 {
            let ts = Utc::now().timestamp() + i;
            let file = tmp_dir.as_ref().join(format!("abc-{}-{}-{}.z", ts, i, i));
            std::fs::write(file.as_path(), b"hello world").unwrap();
            raw_files.push(file);
        }

        // when
        let files = ZFile::list_in_dir(tmp_dir.as_ref()).await.unwrap();

        // then
        assert_eq!(files.len(), 10);
        assert_eq!(files[0].raw_path.as_ref().unwrap(), &raw_files[0]);
        assert_eq!(files[4].raw_path.as_ref().unwrap(), &raw_files[4]);
        assert_eq!(files[9].raw_path.as_ref().unwrap(), &raw_files[9]);
    }

    #[tokio::test]
    async fn test_parse_file_name() {
        let file_name = "abc-1630065633001-22-1.z";
        let (topic, ts, vg_id, index) = ZFile::parse_file_name(file_name).unwrap();
        assert_eq!("abc", topic);
        assert_eq!(1630065633001, ts.timestamp_millis());
        assert_eq!(22, vg_id);
        assert_eq!(1, index);
    }

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
                    ZMessage::Meta(meta) => taos.write_raw_meta(&meta).await?,
                    ZMessage::Data(data) => {
                        // dbg!(&data);
                        for raw in data {
                            rows += raw.nrows();
                            taos.write_raw_block(&raw).await?;
                        }
                        println!("rows: {}", rows);
                        // taos.write_raw_data(data[0]).await?
                    }
                    ZMessage::Raw(_raw_type, raw) => taos.write_raw_meta(&raw).await?,
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
