use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::{atomic::Ordering, OnceLock};
use std::time::Duration;

use anyhow::Context;
use bitfield::bitfield;
use parking_lot::RwLock;
use taoslog::utils::QidMetadataSetter;
use taoslog::QidManager;
use tokio_util::sync::{CancellationToken, DropGuard};

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
    Sled(sled::Tree),
    Atomic(Arc<AtomicU32>),
}

impl IdCounter {
    async fn fetch_batch_id(&self, step: u32) -> anyhow::Result<std::ops::Range<u32>> {
        match self {
            IdCounter::Sled(db) => Self::sled_fetch_batch_id(db, step).await,
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
        db: &sled::Tree,
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
        let db = db.clone();
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

    fn blocking_set_batch_id(&self, current: u32) -> anyhow::Result<()> {
        match self {
            IdCounter::Sled(db) => {
                db.insert("batch_id", current.to_be_bytes().to_vec())
                    .context("set batch id error")?;
            }
            IdCounter::Atomic(counter) => {
                counter.store(current, Ordering::SeqCst);
            }
        }

        Ok(())
    }
}

pub struct BatchCounter {
    counter: IdCounter,
    range: Arc<RwLock<std::ops::Range<u32>>>,
    current: Arc<AtomicU32>,
    _cancel_guard: DropGuard,
}

impl BatchCounter {
    pub async fn new(task_id: u16) -> anyhow::Result<Arc<Self>> {
        let counter = match QID_DB.get().and_then(|db| {
            db.open_tree(task_id.to_string())
                .inspect_err(|e| tracing::warn!("open batch counter db tree error: {e}"))
                .ok()
        }) {
            Some(db) => IdCounter::Sled(db),
            None => IdCounter::Atomic(Arc::new(AtomicU32::new(0))),
        };

        const STEP: u32 = 1000;
        let range = counter.fetch_batch_id(STEP).await?;
        let start = std::cmp::max(range.start, 1);
        let range = Arc::new(RwLock::new(range));
        let current = Arc::new(AtomicU32::new(start));
        let cancel = CancellationToken::new();
        tokio::spawn({
            let cancel = cancel.child_token();
            let range = range.clone();
            let current = current.clone();
            let counter = counter.clone();
            async move {
                while cancel
                    .run_until_cancelled(tokio::time::sleep(Duration::from_secs(30)))
                    .await
                    .is_some()
                {
                    if let Err(e) = step(&counter, &range, &current, STEP).await {
                        tracing::error!("batch counter step error: {e:#}");
                    }
                }
            }
        });

        Ok(Arc::new(Self {
            counter,
            range,
            current,
            _cancel_guard: cancel.drop_guard(),
        }))
    }

    pub fn next(&self) -> u32 {
        self.current.fetch_add(1, Ordering::SeqCst)
    }
}

async fn step(
    counter: &IdCounter,
    range: &RwLock<std::ops::Range<u32>>,
    current: &AtomicU32,
    step: u32,
) -> anyhow::Result<()> {
    loop {
        let current_value = current.load(Ordering::SeqCst);
        let range_value = {
            let range = range.read();
            if range.contains(&current_value) {
                return anyhow::Ok(());
            }
            range.clone()
        };
        let step = step.max(current_value - range_value.end);
        let new_range = counter.fetch_batch_id(step).await?;
        {
            let mut range = range.write();
            *range = new_range;
            current.fetch_max(range.start, Ordering::SeqCst);
        }
    }
}

impl Drop for BatchCounter {
    fn drop(&mut self) {
        let range = self.range.read();
        let current = self.current.load(Ordering::SeqCst);
        if range.contains(&current) {
            return;
        }
        self.counter.blocking_set_batch_id(current).ok();
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
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };

    use parquet::data_type::AsBytes;

    use super::*;
    use crate::set_env_data_dir;

    #[tokio::test]
    async fn test_add_batch_id() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = sled::open(tmp.path().join("batch_counter_tasks")).unwrap();
        let db = IdCounter::Sled(db.open_tree(1.to_string()).unwrap());
        test_batch_id(db).await;

        let db = IdCounter::Atomic(Arc::new(AtomicU32::new(0)));
        test_batch_id(db).await;
    }

    #[tokio::test]
    async fn multi_qid_db_test() {
        let tmp = tempfile::TempDir::new().unwrap();
        set_env_data_dir(tmp.path().to_str().unwrap().to_string());
        let counter = BatchCounter::new(1).await.unwrap();
        assert_eq!(1, counter.next());
        let counter2 = BatchCounter::new(1).await.unwrap();
        assert_eq!(1, counter2.next());
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

    #[tokio::test]
    async fn step_test() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = sled::open(tmp.path().join("batch_counter_tasks")).unwrap();
        let db = db.open_tree(1.to_string()).unwrap();
        let counter = IdCounter::Sled(db.clone());

        let range = counter.fetch_batch_id(1000).await.unwrap();

        let get_number = async || {
            tokio::task::spawn_blocking({
                let db = db.clone();
                move || {
                    let res = db.get("batch_id").unwrap().unwrap();
                    let bytes = res.as_bytes();
                    let array: [u8; 4] = bytes.try_into().expect("valid number");
                    u32::from_be_bytes(array)
                }
            })
            .await
            .unwrap()
        };
        assert_eq!(1000, get_number().await);
        let start = std::cmp::max(range.start, 1);
        let range = Arc::new(RwLock::new(range));
        let current = Arc::new(AtomicU32::new(start));

        assert_eq!(1, current.fetch_add(1, Ordering::SeqCst));
        step(&counter, &range, &current, 1000).await.unwrap();
        assert_eq!(1000, get_number().await);

        assert_eq!(2, current.fetch_add(1200, Ordering::SeqCst));
        step(&counter, &range, &current, 1000).await.unwrap();
        assert_eq!(2000, get_number().await);
    }
}
