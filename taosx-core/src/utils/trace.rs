use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::{atomic::Ordering, OnceLock};

use anyhow::Context;
use bitfield::bitfield;
use taoslog::utils::QidMetadataSetter;
use taoslog::QidManager;
use tokio::sync::RwLock;

use crate::get_data_dir;

pub static INSTANCE_ID: OnceLock<u8> = OnceLock::new();
pub const DEFAULT_INSTANCE_ID: u8 = 16;

static QID_DB: OnceLock<sled::Db> = OnceLock::new();

pub fn qid_db_init() -> anyhow::Result<()> {
    let db_path = get_data_dir().join("tasks").join("qid");
    if !db_path.is_dir() {
        std::fs::create_dir_all(&db_path).context("create qid database path error")?;
    }

    // TS-5631: Use the qid file lock to ensure that multiple taosx services do not access the same data directory.
    // 这里只抛出数据库文件锁定的错误，其他错误忽略，使用内存的计数器
    let db = match sled::open(&db_path) {
        Ok(db) => db,
        Err(sled::Error::Io(e))
            if e.kind() == std::io::ErrorKind::Other
                && e.to_string().starts_with("could not acquire lock on") =>
        {
            anyhow::bail!("qid db file has been locked")
        }
        Err(e) => {
            tracing::error!("open qid database {} error: {e:#}", db_path.display());
            return Ok(());
        }
    };
    QID_DB.get_or_init(|| db);
    Ok(())
}

bitfield! {
    pub struct Qid(u64);
    impl Debug;

    pub u8, sub_batch_id, inner_set_sub_batch_id: 7,0;
    pub u32, batch_id, inner_set_batch_id: 39,8;
    pub u16, task_id, inner_set_task_id: 55, 40;
    pub u8, instance_id, inner_set_instance_id: 63, 56;
}

impl Qid {
    pub fn set_task_id(&mut self, task_id: u16) {
        self.inner_set_task_id(task_id);
        taoslog::utils::Span.set_qid(self);
    }
    pub fn set_batch_id(&mut self, batch_id: u32) {
        self.inner_set_batch_id(batch_id);
        taoslog::utils::Span.set_qid(self);
    }
    pub fn add_sub_batch_id(&mut self) {
        let (num, _overflow) = self.sub_batch_id().overflowing_add(1);
        self.inner_set_sub_batch_id(num);
        taoslog::utils::Span.set_qid(self);
    }
}

#[derive(Clone)]
enum IdCounter {
    Sled(Arc<sled::Tree>),
    Atomic(Arc<AtomicU32>),
}

impl IdCounter {
    async fn fetch_batch_id(&self, step: u32) -> anyhow::Result<std::ops::Range<u32>> {
        match self {
            IdCounter::Sled(db) => Self::sled_fetch_batch_id(db.clone(), step).await,
            IdCounter::Atomic(atomic_u32) => {
                let start = atomic_u32.fetch_add(step, Ordering::Relaxed);
                if start.overflowing_add(step).1 {
                    Ok(0..step)
                } else {
                    Ok(start..start + step)
                }
            }
        }
    }

    async fn sled_fetch_batch_id(
        db: Arc<sled::Tree>,
        step: u32,
    ) -> anyhow::Result<std::ops::Range<u32>> {
        let increment = move |old: Option<&[u8]>| -> Option<Vec<u8>> {
            let number = match old {
                Some(bytes) => {
                    let array: [u8; 4] = bytes.try_into().expect("valid number");
                    let number = u32::from_be_bytes(array);
                    let (number, overflow) = number.overflowing_add(step);
                    if overflow {
                        step
                    } else {
                        number
                    }
                }
                None => step,
            };
            Some(number.to_be_bytes().to_vec())
        };
        let number = tokio::task::spawn_blocking(move || -> anyhow::Result<u32> {
            let v = db
                .update_and_fetch("batch_id", increment)
                .context("update and fetch error")?
                .unwrap();
            let array: [u8; 4] = v.to_vec().try_into().unwrap();
            let number = u32::from_be_bytes(array);
            Ok(number)
        })
        .await
        .context("fetch batch id error")??;
        Ok(number - step..number)
    }
}

#[derive(Clone)]
pub struct BatchCounter {
    counter: IdCounter,
    step: u32,
    range: Arc<RwLock<std::ops::Range<u32>>>,
    current: Arc<AtomicU32>,
}

impl BatchCounter {
    pub async fn new(task_id: u16) -> anyhow::Result<Self> {
        let counter = match QID_DB.get().and_then(|db| {
            db.open_tree(task_id.to_string())
                .inspect_err(|e| tracing::warn!("open batch counter db tree error: {e}"))
                .ok()
        }) {
            Some(db) => IdCounter::Sled(Arc::new(db)),
            None => IdCounter::Atomic(Arc::new(AtomicU32::new(0))),
        };

        let step = 1000;
        let range = counter.fetch_batch_id(step).await?;
        let start = std::cmp::max(range.start, 1);

        Ok(Self {
            counter,
            step,
            range: Arc::new(RwLock::new(range)),
            current: Arc::new(AtomicU32::new(start)),
        })
    }

    pub async fn next(&self) -> anyhow::Result<u32> {
        {
            let current = self.current.fetch_add(1, Ordering::SeqCst);
            let range = self.range.read().await;
            if range.contains(&current) {
                return Ok(current);
            }
        }

        let mut range = self.range.write().await;
        // confirm again
        let current = self.current.fetch_add(1, Ordering::SeqCst);
        if range.contains(&current) {
            return Ok(current);
        }
        // fetch a new range
        let new_range = self.counter.fetch_batch_id(self.step).await?;
        *range = new_range;
        self.current.store(range.start, Ordering::SeqCst);
        Ok(range.start)
    }
}

impl Clone for Qid {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl QidManager for Qid {
    fn init() -> Self {
        let mut this = Self(0);
        if let Some(instance_id) = INSTANCE_ID.get() {
            this.inner_set_instance_id(*instance_id);
        } else {
            this.inner_set_instance_id(DEFAULT_INSTANCE_ID);
        }
        this
    }

    fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicU32, Arc};

    use crate::{
        set_env_data_dir,
        utils::trace::{BatchCounter, IdCounter},
    };

    #[tokio::test]
    async fn test_add_batch_id() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = sled::open(tmp.path().join("batch_counter_tasks")).unwrap();
        let db = IdCounter::Sled(Arc::new(db.open_tree(1.to_string()).unwrap()));
        test_batch_id(db).await;

        let db = IdCounter::Atomic(Arc::new(AtomicU32::new(0)));
        test_batch_id(db).await;
    }

    #[tokio::test]
    async fn multi_qid_db_test() {
        let tmp = tempfile::TempDir::new().unwrap();
        set_env_data_dir(tmp.path().to_str().unwrap().to_string());
        let counter = BatchCounter::new(1).await.unwrap();
        counter.next().await.unwrap();
        let counter2 = BatchCounter::new(1).await.unwrap();
        counter2.next().await.unwrap();
    }

    async fn test_batch_id(db: IdCounter) {
        assert_eq!(db.fetch_batch_id(1000).await.unwrap(), (0..1000));
        assert_eq!(db.fetch_batch_id(990).await.unwrap(), (1000..1990));
        assert_eq!(
            db.fetch_batch_id(u32::MAX / 2).await.unwrap(),
            (1990..u32::MAX / 2 + 1990)
        );
        assert_eq!(
            db.fetch_batch_id(u32::MAX / 2).await.unwrap(),
            (0..u32::MAX / 2)
        );
    }
}
