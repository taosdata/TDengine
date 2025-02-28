use crate::taoz::{ZFile, ZFileName};
use crate::utils;
use opendal::raw::HttpClient;
use opendal::{EntryMode, Operator};
use std::path::PathBuf;
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
            .bucket(&self.bucket)
            .endpoint(&self.endpoint)
            .access_key_id(&self.access_key_id)
            .secret_access_key(&self.secret_access_key)
            .http_client(http_client);

        if let Some(region) = &self.region {
            builder = builder.region(region.as_str());
        }

        // Init an operator
        let op = Operator::new(builder)?.finish();
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
pub struct S3Dumper {
    path: PathBuf,
    config: S3Config,
    retention_period: Option<Duration>,
    retention_size: Option<u64>,

    cancel_token: CancellationToken,
    op: Operator,
}

impl S3Dumper {
    pub async fn new(
        path: PathBuf,
        config: S3Config,
        retention_period: Option<Duration>,
        retention_size: Option<u64>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<Self> {
        // connect to s3
        let op = config.connect().await?;
        Ok(Self {
            path,
            config,
            retention_period,
            retention_size,
            cancel_token,
            op,
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        tracing::info!("s3 dumper started");
        tracing::info!(
            "s3 dumper config: {:?}, path: {:?}, retention_period: {:?}, retention_size: {:?}",
            self.config,
            self.path,
            self.retention_period,
            self.retention_size
        );

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
        let files = ZFile::list_in_dir(&self.path).await?;

        // 根据备份保留策略，过滤备份文件
        let to_upload = Self::filter_retention(
            files.iter().collect(),
            self.retention_period,
            self.retention_size,
        )?;

        // 上传 files_to_upload 到 S3
        for f in to_upload.iter() {
            if let Some(raw_path) = f.raw_path.as_ref() {
                let key = match &self.config.prefix {
                    Some(prefix) => format!("{}{}", prefix, f),
                    None => f.to_string(),
                };
                let content = tokio::fs::read(raw_path).await?;
                self.op.write(&key, content).await?;
                tracing::info!("s3 dumper upload file: {:?}", key);
            }
        }

        Ok(())
    }

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
        if let Some(retention_size) = retention_size {
            if retain.len() > retention_size as usize {
                let to_remove = retain.len() - retention_size as usize;
                for f in retain.into_iter().take(to_remove) {
                    to_upload.push(f.clone());
                }
            }
        }

        Ok(to_upload)
    }
}

#[cfg(test)]
mod tests {
    use crate::s3::S3Dumper;
    use crate::taoz::ZFileName;
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
                (now - Duration::from_secs((10 - i) * 60)).timestamp()
            );
            files.push(ZFileName::from_path(f).unwrap());
        }
        // dbg!(files
        //     .iter()
        //     .map(|f| f.raw_path.clone().unwrap())
        //     .collect_vec());

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
    /// 测试 S3 配置
    /// # Example
    /// ```shell
    /// S3_ENDPOINT=http://192.168.2.139:9000 S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_BUCKET=test S3_REGION=us-west-1 cargo nextest run -p taosx-core test_s3_config --no-capture
    ///
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
    /// 测试 S3 Dumper，将本地目录中的文件上传到 S3
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
                .unwrap_or(tempfile::tempdir().unwrap().into_path());
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
            let dumper = super::S3Dumper::new(local_dir, config, None, None, cancel_token.clone())
                .await
                .unwrap();
            // when
            let h = tokio::spawn(async move {
                dumper.run().await.unwrap();
            });
            // then
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            cancel_token.cancel();
            h.await.unwrap();
        }
    }
}
