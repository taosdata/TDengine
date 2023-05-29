use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use flume::Sender;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use tokio_util::sync::CancellationToken;

pub type ConnectorTransferred = taosx_core::Transferred;
type Cache = Arc<DashMap<(i64, String), Arc<ConnectorTransferred>>>;

#[derive(Debug, Default, PartialEq, Eq)]
struct TransferredCache {
    pub stables: u32,
    pub tables: u32,
    pub records: u64,
    pub points: u64,
}

type CacheMap = DashMap<(i64, String), TransferredCache>;
#[derive(Debug)]
pub struct Transferred {
    cache: Cache,
    persist: SqlitePool,
    interval: Duration,
    stop: CancellationToken,
    // sender: Sender<()>,
}

impl From<&ConnectorTransferred> for TransferredCache {
    fn from(value: &ConnectorTransferred) -> Self {
        let order = Ordering::SeqCst;
        Self {
            stables: value.stables.load(order),
            tables: value.tables.load(order),
            records: value.records.load(order),
            points: value.points.load(order),
        }
    }
}

async fn tracking_transferred(
    cache: Cache,
    persist: SqlitePool,
    interval: Duration,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(interval);
    // let mut last =
    async fn call_persist(
        last: &mut CacheMap,
        cache: &Cache,
        persist: &SqlitePool,
    ) -> Result<(), sqlx::Error> {
        if cache.is_empty() {
            return Ok(());
        }
        let cache = cache.clone();
        // let conn = persist.acquire().await?;
        for item in cache.iter() {
            let (cluster_id, connector) = item.key();
            let transferred = item.value();
            let cache_value: TransferredCache = transferred.as_ref().into();
            if let Some(mut cached) = last.get_mut(item.key()) {
                if *cached.value() == cache_value {
                    continue;
                } else {
                    *cached.value_mut() = cache_value;
                }
            } else {
                last.insert((*cluster_id, connector.clone()), cache_value);
            }
            // let cache
            let res = sqlx::query("insert into connector_transferred values(?, ?, ?, ?, ?) on conflict(cluster_id, connector) DO update set tables = excluded.tables, records = excluded.records, points = excluded.points").bind(cluster_id).bind(connector).bind(transferred.tables.load(Ordering::SeqCst) as i32).bind(transferred.records.load(Ordering::SeqCst) as i64).bind(transferred.points.load(Ordering::SeqCst) as i64).execute(persist).await;
            if let Err(err) = res {
                log::error!("Persist connector transferred metrics to database error: {err}");
            }
        }
        Ok::<_, sqlx::Error>(())
    };
    async fn init_persist(cache: &Cache, persist: &SqlitePool) -> Result<CacheMap, sqlx::Error> {
        let cache_last = CacheMap::default();
        if cache.is_empty() {
            return Ok(cache_last);
        }
        let cache = cache.clone();
        // let conn = persist.acquire().await?;
        for item in cache.iter() {
            let (cluster_id, connector) = item.key();
            let transferred = item.value();
            let cache_value: TransferredCache = transferred.as_ref().into();
            sqlx::query("insert into connector_transferred values(?, ?, ?, ?, ?) on conflict(cluster_id, connector) DO update set tables = excluded.tables, records = excluded.records, points = excluded.points").bind(cluster_id).bind(connector).bind(transferred.tables.load(Ordering::SeqCst) as i32).bind(transferred.records.load(Ordering::SeqCst) as i64).bind(transferred.points.load(Ordering::SeqCst) as i64).execute(persist).await?;
            cache_last.insert((*cluster_id, connector.clone()), cache_value);
        }
        Ok::<_, sqlx::Error>(cache_last)
    };
    // init persist
    let mut last = init_persist(&cache, &persist).await?;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                call_persist(&mut last, &cache, &persist).await?;
            },
            _ = stop.cancelled() => {
                call_persist(&mut last,&cache, &persist).await?;
                break;
            }
        }
    }
    Ok(())
}

impl Transferred {
    pub fn new(persist: SqlitePool, interval: Duration) -> Self {
        // let (sender, receiver) = flume::bounded(100);
        let stop = CancellationToken::new();
        let cache = Cache::default();
        tokio::spawn(tracking_transferred(
            cache.clone(),
            persist.clone(),
            interval,
            stop.clone(),
        ));
        Self {
            cache,
            persist,
            interval,
            stop,
        }
    }
    pub async fn get(&self, key: &(i64, String)) -> Option<Arc<ConnectorTransferred>> {
        if let Some(t) = self.cache.get(key) {
            Some(t.value().clone())
        } else {
            #[derive(FromRow, Default)]
            struct Used {
                tables: i32,
                records: i64,
                points: i64,
            }
            let used = sqlx::query_as::<_, Used>("select tables, records, points from connector_transferred where cluster_id = ? and connector = ?").bind(key.0).bind(&key.1).fetch_optional(&self.persist).await.unwrap_or_default().unwrap_or_default();

            let used = ConnectorTransferred {
                stables: AtomicU32::new(0),
                tables: AtomicU32::new(used.tables as _),
                records: AtomicU64::new(used.records as _),
                points: AtomicU64::new(used.points as _),
            };
            self.cache.insert(key.clone(), Arc::new(used));
            Some(self.cache.get(key).unwrap().value().clone())
        }
    }
}
