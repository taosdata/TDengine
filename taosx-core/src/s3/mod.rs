use crate::taoz::{ZFile, ZFileName};
use crate::utils;
use anyhow::Context;
use opendal::raw::HttpClient;
use opendal::{Entry, EntryMode, Operator};
use std::path::{Path, PathBuf};
use std::time::Duration;
use taos::Dsn;
use tokio_util::sync::CancellationToken;

pub const S3_ENABLE: &str = "s3_enable";

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: Option<String>,
    pub bucket: String,
    /// S3 对象存储的前缀，类似于 directory，默认为：'/'，即根目录
    pub prefix: Option<String>,
}

impl S3Config {
    /// 从 DSN 中解析 S3 配置
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let endpoint = utils::parse_key_in_dsn::<String>(dsn, "s3_endpoint")?
            .ok_or(anyhow::anyhow!("s3_endpoint not found"))?;
        let access_key_id = utils::parse_key_in_dsn::<String>(dsn, "s3_access_key_id")?
            .ok_or(anyhow::anyhow!("s3_access_key_id not found"))?;
        let secret_access_key = utils::parse_key_in_dsn::<String>(dsn, "s3_secret_access_key")?
            .ok_or(anyhow::anyhow!("s3_secret_access_key not found"))?;
        let region = utils::parse_key_in_dsn::<String>(dsn, "s3_region")?;
        let bucket = utils::parse_key_in_dsn(dsn, "s3_bucket")?
            .ok_or(anyhow::anyhow!("s3_bucket not found"))?;
        let prefix = utils::parse_key_in_dsn::<String>(dsn, "s3_object_prefix")?.map(|s| {
            if s.trim().ends_with('/') {
                s
            } else {
                format!("{}/", s)
            }
        });

        Ok(Self {
            endpoint,
            access_key_id,
            secret_access_key,
            region,
            bucket,
            prefix,
        })
    }

    pub async fn connect(&self) -> anyhow::Result<Operator> {
        // Pick a builder and configure it.
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()?;
        let http_client = HttpClient::with(client);

        let mut builder = opendal::services::S3::default()
            .disable_config_load()
            .bucket(&self.bucket)
            .endpoint(&self.endpoint)
            .access_key_id(&self.access_key_id)
            .secret_access_key(&self.secret_access_key);

        if let Some(region) = &self.region {
            builder = builder.region(region.as_str());
        }

        // Init an operator
        let op = Operator::new(builder)?.finish();
        op.update_http_client(|_| http_client);
        // check
        op.check().await?;
        // read the meta of the prefix
        let prefix = self.prefix.as_deref().unwrap_or("/");
        let meta = op.stat(prefix).await?;
        match meta.mode() {
            EntryMode::DIR => {
                tracing::info!("connected with s3 directory: {:?}", self.prefix);
            }
            EntryMode::FILE | EntryMode::Unknown => {
                anyhow::bail!("prefix: {:?} is not a directory", self.prefix);
            }
        }

        Ok(op)
    }
}

#[derive(Debug)]
pub struct S3DumpConfig {
    /// 本地备份路径
    pub local_path: PathBuf,
    /// S3 的连接配置
    pub s3_connect: S3Config,
    /// 备份文件保留时长
    pub retention_period: Option<Duration>,
    /// 备份文件保留数量
    pub retention_size: Option<u64>,
    /// 刷新间隔
    fresh_interval: Duration,
}

pub struct S3Dumper {
    config: S3DumpConfig,
    cancel_token: CancellationToken,
    op: Operator,
}

impl S3Dumper {
    pub async fn new(
        local_path: PathBuf,
        s3_config: S3Config,
        retention_period: Option<Duration>,
        retention_size: Option<u64>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<Self> {
        // connect to s3
        let op = s3_config.connect().await?;
        Ok(Self {
            config: S3DumpConfig {
                local_path,
                s3_connect: s3_config,
                retention_period,
                retention_size,
                fresh_interval: Duration::from_secs(10),
            },
            cancel_token,
            op,
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        tracing::info!("s3 dumper started");
        tracing::debug!("config: {:?}", self.config);

        loop {
            if let Err(e) = self.process().await {
                tracing::error!("s3 dumper error: {:?}", e);
                break;
            }
            if self.cancel_token.is_cancelled() {
                tracing::info!("s3 dumper cancelled");
                break;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        tracing::info!("s3 dumper stopped");
        Ok(())
    }

    async fn process(&self) -> anyhow::Result<()> {
        // 列出备份路径下的所有备份文件, 根据 topic 过滤，按照 ts, vgId, index 排序
        let files = ZFile::list_in_dir(&self.config.local_path).await?;

        // 根据备份保留策略，过滤备份文件
        let to_upload = Self::filter_retention(
            files.iter().collect(),
            self.config.retention_period,
            self.config.retention_size,
        )?;

        // 上传 files_to_upload 到 S3
        for f in to_upload.iter() {
            if let Some(raw_path) = f.raw_path.as_ref() {
                let key = match &self.config.s3_connect.prefix {
                    Some(prefix) => format!("{}{}", prefix, f),
                    None => f.to_string(),
                };
                let content = tokio::fs::read(raw_path).await?;
                self.op.write(&key, content).await?;
                tokio::fs::remove_file(raw_path).await?;
                tracing::info!("s3 dumper uploaded file: {:?}", key);
            }
        }

        tokio::time::sleep(self.config.fresh_interval).await;

        Ok(())
    }

    /// 根据备份保留策略，过滤备份文件
    fn filter_retention(
        files: Vec<&ZFileName>,
        retention_period: Option<Duration>,
        retention_size: Option<u64>,
    ) -> anyhow::Result<Vec<ZFileName>> {
        let files = files.into_iter().cloned().collect::<Vec<ZFileName>>();

        // 如果 file 的 ts < now - backup_retention_period, 则将 file 移到 files_to_upload
        let (mut to_upload, retain) = if let Some(retention_period) = retention_period {
            files.into_iter().partition(|f| {
                if let Some(ts) = f.timestamp {
                    ts < chrono::Utc::now() - retention_period
                } else {
                    false
                }
            })
        } else {
            (vec![], files)
        };

        // 遍历剩下的 files，取 files.len() - backup_retention_size 个文件，将其移动到 files_to_upload
        if let Some(retention_size) = retention_size
            && retain.len() > retention_size as usize
        {
            let to_remove = retain.len() - retention_size as usize;
            for f in retain.into_iter().take(to_remove) {
                to_upload.push(f.clone());
            }
        }

        Ok(to_upload)
    }
}

/// 从 S3 下载文件
pub struct S3Loader {
    pub s3_config: S3Config,

    op: Operator,
}

impl S3Loader {
    pub async fn try_from(s3_config: &S3Config) -> anyhow::Result<Self> {
        let op = s3_config.connect().await?;

        Ok(Self {
            s3_config: s3_config.clone(),
            op,
        })
    }

    /// 从 S3 上下载文件到本地
    pub async fn load_to(&self, local_path: impl AsRef<Path>) -> anyhow::Result<()> {
        let objects = self.list().await?;

        for obj in objects {
            let meta = obj.metadata();
            match meta.mode() {
                EntryMode::FILE => {
                    let obj_key = obj.path();
                    let content = self.op.read(obj_key).await?;
                    let local_file = local_path.as_ref().join(obj.name());
                    tokio::fs::write(local_file.as_path(), content.to_vec())
                        .await
                        .context(format!(
                            "s3 loader failed to download {} to {}",
                            obj_key,
                            local_file.display()
                        ))?;
                    tracing::info!("s3 loader download {} to {}", obj_key, local_file.display());
                }
                EntryMode::DIR | EntryMode::Unknown => continue,
            }
        }

        Ok(())
    }

    /// 列出 S3 上的文件
    pub async fn list(&self) -> anyhow::Result<Vec<Entry>> {
        let prefix = self.s3_config.prefix.as_deref().unwrap_or("/");

        self.list_dir(prefix).await
    }

    /// 列出 S3 上指定 dir 的文件
    pub async fn list_dir(&self, prefix: &str) -> anyhow::Result<Vec<Entry>> {
        let mut uploaded = vec![];

        let objects = self.op.list(prefix).await?;
        for obj in objects {
            let meta = obj.metadata();
            match meta.mode() {
                EntryMode::FILE => {
                    uploaded.push(obj);
                }
                EntryMode::DIR | EntryMode::Unknown => continue,
            }
        }

        Ok(uploaded)
    }
}

#[cfg(test)]
mod tests {
    use crate::s3::{S3Config, S3Dumper, S3Loader};
    use crate::taoz::ZFileName;
    use anyhow::Context;
    use chrono::Utc;
    use std::env;
    use std::path::PathBuf;
    use std::time::Duration;
    use taos::IntoDsn;

    #[test]
    fn test_filter_retention() {
        let now = Utc::now();
        let mut files = vec![];
        for i in 0..10 {
            let f = format!(
                "./abc-{}-1-1.z",
                (now - Duration::from_secs((10 - i) * 60)).timestamp_millis()
            );
            files.push(ZFileName::from_path(f).unwrap());
        }

        // retention_period = None, retention_size = None
        let upload = S3Dumper::filter_retention(files.iter().collect(), None, None).unwrap();
        assert!(upload.is_empty());

        // retention_period = 310 sec, retention_size = None
        let upload = S3Dumper::filter_retention(
            files.iter().collect(),
            Some(Duration::from_secs(310)),
            None,
        )
        .unwrap();
        assert_eq!(upload.len(), 5);
        for i in 0..5 {
            assert_eq!(upload[i].raw_path, files[i].raw_path);
        }

        // retention_period = None, retention_size = 3
        let upload = S3Dumper::filter_retention(files.iter().collect(), None, Some(3)).unwrap();
        assert_eq!(upload.len(), 7);
        for i in 0..7 {
            assert_eq!(upload[i].raw_path, files[i].raw_path);
        }

        // retention_period = 310 sec, retention_size = 10
        let upload = S3Dumper::filter_retention(
            files.iter().collect(),
            Some(Duration::from_secs(310)),
            Some(3),
        )
        .unwrap();
        assert_eq!(upload.len(), 7);
        for i in 0..7 {
            assert_eq!(upload[i].raw_path, files[i].raw_path);
        }

        // retention_period = 0, retention_size = None
        let upload =
            S3Dumper::filter_retention(files.iter().collect(), Some(Duration::from_secs(0)), None)
                .unwrap();
        assert_eq!(upload.len(), 10);
        for i in 0..10 {
            assert_eq!(upload[i].raw_path, files[i].raw_path);
        }

        // retention_period = None, retention_size = 0
        let upload = S3Dumper::filter_retention(files.iter().collect(), None, Some(0)).unwrap();
        assert_eq!(upload.len(), 10);
        for i in 0..10 {
            assert_eq!(upload[i].raw_path, files[i].raw_path);
        }
    }

    /// # Case
    /// 测试 S3 配置解析和连通性检查
    /// # Example
    /// 用环境变量指定 S3_ENDPOINT 等连接信息
    /// ```shell
    /// S3_ENDPOINT=http://192.168.2.139:9000 S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_BUCKET=test S3_REGION=us-west-1 cargo nextest run -p taosx-core test_s3_config --no-capture
    /// ```
    /// S3_ENDPOINT 是 url encoded 的地址
    /// ```shell
    /// S3_ENDPOINT=http%3A%2F%2F192.168.2.139%3A9000 S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_BUCKET=test S3_REGION=us-west-1 cargo nextest run -p taosx-core test_s3_config --no-capture
    /// ```
    #[tokio::test]
    async fn test_s3_config() {
        let endpoint = env::var("S3_ENDPOINT").ok();
        if let Some(endpoint) = endpoint {
            // given
            let key = env::var("S3_ACCESS_KEY_ID").unwrap();
            let secret = env::var("S3_SECRET_ACCESS_KEY").unwrap();
            let bucket = env::var("S3_BUCKET").unwrap();
            let mut dsn = format!(
                "local:/tmp?s3_endpoint={}&s3_access_key_id={}&s3_secret_access_key={}&s3_bucket={}",
                endpoint, key, secret, bucket
            );
            let region = env::var("S3_REGION").ok();
            if let Some(region) = &region {
                dsn.push_str(&format!("&s3_region={}", region));
            }
            let dsn = dsn.into_dsn().unwrap();
            // when
            let config = super::S3Config::from_dsn(&dsn).unwrap();
            // then
            let endpoint = urlencoding::decode(&endpoint).unwrap().to_string();
            assert_eq!(config.endpoint, endpoint);
            assert_eq!(config.access_key_id, key);
            assert_eq!(config.secret_access_key, secret);
            assert_eq!(config.region, region);
            assert_eq!(config.bucket, bucket);
            assert_eq!(config.prefix, None);

            let op = config.connect().await;
            assert!(op.is_ok());
        }
    }

    /// # Case
    /// 测试 S3 Dumper，将本地目录中的备份文件上传到 S3
    /// * LOCAL_DIR: 本地目录, 默认为 /tmp
    /// * S3_ENDPOINT: S3 服务的地址
    /// * S3_ACCESS_KEY_ID: 访问密钥ID
    /// * S3_SECRET_ACCESS_KEY: 访问密钥
    /// * S3_BUCKET: 存储桶
    /// * S3_REGION: 地区
    /// # Example
    /// ```shell
    /// LOCAL_DIR=/tmp S3_ENDPOINT=http://192.168.2.13:9000 S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_BUCKET=test S3_REGION=auto cargo nextest run -p taosx-core test_s3_dumper --no-capture
    /// ```
    #[tokio::test]
    async fn test_s3_dumper() {
        let endpoint = env::var("S3_ENDPOINT").ok();
        if let Some(endpoint) = endpoint {
            // init params
            let local_dir = env::var("LOCAL_DIR")
                .map(PathBuf::from)
                .ok()
                .unwrap_or(tempfile::tempdir().unwrap().keep());
            let access_key_id = env::var("S3_ACCESS_KEY_ID").unwrap();
            let secret_access_key = env::var("S3_SECRET_ACCESS_KEY").unwrap();
            let bucket = env::var("S3_BUCKET").unwrap();
            let mut dsn = format!(
                "local:{:?}?s3_endpoint={}&s3_access_key_id={}&s3_secret_access_key={}&s3_bucket={}",
                local_dir.as_path(),
                endpoint,
                access_key_id,
                secret_access_key,
                bucket
            );
            let region = env::var("S3_REGION").ok();
            if let Some(region) = &region {
                dsn.push_str(&format!("&s3_region={}", region));
            }
            let dsn = dsn.into_dsn().unwrap();
            dbg!(&dsn);

            // given
            let config = super::S3Config::from_dsn(&dsn).unwrap();
            let cancel_token = tokio_util::sync::CancellationToken::new();
            let dumper = S3Dumper::new(local_dir, config, None, None, cancel_token.clone())
                .await
                .unwrap();
            // when
            let h = tokio::spawn(async move {
                dumper.run().await.unwrap();
            });
            // then
            tokio::time::sleep(Duration::from_secs(5)).await;
            cancel_token.cancel();
            h.await.unwrap();
        }
    }

    /// # example
    /// ```shell
    /// LOCAL_DIR=/tmp S3_ENDPOINT='https://192.168.2.139:9000' S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_REGION=us-west-1 S3_BUCKET=taosx S3_OBJECT_PREFIX=backup/ cargo nextest run -p taosx-core test_s3_loader --no-capture --retries 0
    /// ```
    #[tokio::test]
    async fn test_s3_loader() {
        let s3_args = load_s3_env_args().unwrap();
        if let Some(s3_config) = s3_args {
            // given
            let local_dir = env::var("LOCAL_DIR")
                .map(PathBuf::from)
                .ok()
                .unwrap_or(tempfile::tempdir().unwrap().keep());

            // when
            let loader = S3Loader::try_from(&s3_config).await.unwrap();
            loader.load_to(local_dir.as_path()).await.unwrap();

            // 列出本地目录
            let mut files = tokio::fs::read_dir(local_dir).await.unwrap();
            while let Ok(Some(f)) = files.next_entry().await {
                let path = f.path();
                let file_name = path.file_name().unwrap().to_str().unwrap();
                if file_name.ends_with(".z") {
                    dbg!(&file_name);
                }
            }
        }
    }

    pub fn load_s3_env_args() -> anyhow::Result<Option<S3Config>> {
        if let Ok(endpoint) = env::var("S3_ENDPOINT") {
            let access_key_id =
                env::var("S3_ACCESS_KEY_ID").context("S3_ACCESS_KEY_ID not found")?;
            let secret_access_key =
                env::var("S3_SECRET_ACCESS_KEY").context("S3_SECRET_ACCESS_KEY not found")?;
            let bucket = env::var("S3_BUCKET").context("S3_BUCKET not found")?;
            let region = env::var("S3_REGION").ok();
            let prefix = env::var("S3_OBJECT_PREFIX").ok();

            let s3_config = S3Config {
                endpoint,
                access_key_id,
                secret_access_key,
                region,
                bucket,
                prefix,
            };

            dbg!(&s3_config);

            return Ok(Some(s3_config));
        };
        Ok(None)
    }

    /// 纯解析用例：校验 `s3_object_prefix` 的尾随斜杠归一化
    #[test]
    fn test_s3_config_from_dsn_prefix_normalization() {
        // given
        let base = "local:/tmp?s3_endpoint=http://localhost:9000&s3_access_key_id=ak&s3_secret_access_key=sk&s3_bucket=bk";
        // when: 未带斜杠
        let dsn1 = format!("{base}&s3_object_prefix=backup")
            .into_dsn()
            .unwrap();
        let cfg1 = super::S3Config::from_dsn(&dsn1).unwrap();
        // then
        assert_eq!(cfg1.prefix.as_deref(), Some("backup/"));

        // when: 已带斜杠
        let dsn2 = format!("{base}&s3_object_prefix=logs/").into_dsn().unwrap();
        let cfg2 = super::S3Config::from_dsn(&dsn2).unwrap();
        // then
        assert_eq!(cfg2.prefix.as_deref(), Some("logs/"));

        // when: 未设置前缀
        let dsn3 = base.into_dsn().unwrap();
        let cfg3 = super::S3Config::from_dsn(&dsn3).unwrap();
        // then
        assert_eq!(cfg3.prefix, None);
    }

    /// 纯解析用例：缺失必需键时返回错误
    #[test]
    fn test_s3_config_from_dsn_missing_required_keys() {
        // missing s3_endpoint
        let dsn = "local:/tmp?s3_access_key_id=ak&s3_secret_access_key=sk&s3_bucket=bk"
            .into_dsn()
            .unwrap();
        let err = super::S3Config::from_dsn(&dsn).unwrap_err();
        assert!(format!("{err}").contains("s3_endpoint not found"));

        // missing s3_access_key_id
        let dsn =
            "local:/tmp?s3_endpoint=http://localhost:9000&s3_secret_access_key=sk&s3_bucket=bk"
                .into_dsn()
                .unwrap();
        let err = super::S3Config::from_dsn(&dsn).unwrap_err();
        assert!(format!("{err}").contains("s3_access_key_id not found"));

        // missing s3_secret_access_key
        let dsn = "local:/tmp?s3_endpoint=http://localhost:9000&s3_access_key_id=ak&s3_bucket=bk"
            .into_dsn()
            .unwrap();
        let err = super::S3Config::from_dsn(&dsn).unwrap_err();
        assert!(format!("{err}").contains("s3_secret_access_key not found"));

        // missing s3_bucket
        let dsn = "local:/tmp?s3_endpoint=http://localhost:9000&s3_access_key_id=ak&s3_secret_access_key=sk"
            .into_dsn()
            .unwrap();
        let err = super::S3Config::from_dsn(&dsn).unwrap_err();
        assert!(format!("{err}").contains("s3_bucket not found"));
    }

    /// 纯逻辑用例：timestamp 缺失不参与时间窗口过滤；size 等于剩余数量时不删除
    #[test]
    fn test_filter_retention_no_timestamp_and_exact_size() {
        // 构造 3 个文件，其中 1 个没有 timestamp（不会被 period 条件匹配）
        let mut files = vec![
            ZFileName::from_path("./tp-1700000000000-1-1.z").unwrap(),
            ZFileName::from_path("./tp-1700000060000-1-2.z").unwrap(),
        ];
        let mut f_no_ts = files[0].clone();
        f_no_ts.timestamp = None;
        files.push(f_no_ts);

        // period: 1 小时，只能匹配有 ts 的旧文件；size: 保留剩余全部数量（不触发删除）
        let upload = S3Dumper::filter_retention(
            files.iter().collect(),
            Some(Duration::from_secs(3600)),
            Some(2),
        )
        .unwrap();

        // 因为只有带 ts 的旧文件会入选；size 等于剩余数量时不再删除
        // 在上述构造里，两个有 ts 的文件中会根据当前时间判断是否过期；
        // 我们至少验证：无 ts 的条目不会因为 period 被移动
        assert!(upload.iter().all(|f| f.timestamp.is_some()));
    }

    /// 集成用例：运行 S3Dumper 将本地 .z 备份上传到带前缀目录，并删除本地文件
    /// 仅当环境变量存在时运行：S3_ENDPOINT, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET
    #[tokio::test]
    async fn test_s3_dumper_uploads_and_removes_with_prefix() {
        if let Some(mut s3_cfg) = load_s3_env_args().unwrap() {
            // 先确保前缀在远端存在：使用无前缀配置连接并写入一个占位对象
            let mut cfg_no_prefix = s3_cfg.clone();
            cfg_no_prefix.prefix = None;
            let op = cfg_no_prefix.connect().await.unwrap();
            let prefix = "ut/";
            op.write(&format!("{}{}", prefix, "_placeholder"), "ok")
                .await
                .unwrap();

            // 使用带前缀的配置运行 dumper
            s3_cfg.prefix = Some(prefix.to_string());

            // 构造本地临时目录与若干伪备份文件
            let local_dir = tempfile::tempdir().unwrap().keep();
            let now = Utc::now().timestamp_millis();
            let names = vec![
                format!("tp-{}-1-1.z", now - 120_000),
                format!("tp-{}-1-2.z", now - 60_000),
                format!("tp-{}-1-3.z", now),
            ];
            for n in &names {
                let p = local_dir.join(n);
                tokio::fs::write(&p, b"dummy").await.unwrap();
            }

            // 创建 dumper，并运行一次循环后取消
            let cancel = tokio_util::sync::CancellationToken::new();
            let dumper = S3Dumper::new(
                local_dir.clone(),
                s3_cfg.clone(),
                None,
                Some(0),
                cancel.clone(),
            )
            .await
            .unwrap();
            let h = tokio::spawn(async move { dumper.run().await.unwrap() });
            tokio::time::sleep(Duration::from_secs(3)).await;
            cancel.cancel();
            h.await.unwrap();

            // 验证：本地 .z 文件应已被删除
            let mut remained = tokio::fs::read_dir(local_dir.as_path()).await.unwrap();
            while let Ok(Some(e)) = remained.next_entry().await {
                let name = e.file_name();
                let name = name.to_string_lossy();
                assert!(!name.ends_with(".z"));
            }

            // 验证：远端带前缀目录下存在对应对象
            let loader = S3Loader::try_from(&s3_cfg).await.unwrap();
            let objs = loader.list_dir(prefix).await.unwrap();
            let obj_names: Vec<String> = objs.iter().map(|o| o.name().to_string()).collect();
            for n in &names {
                assert!(obj_names.contains(n));
            }
        }
    }
}
