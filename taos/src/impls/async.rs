use std::ffi::c_void;
use std::os::raw::c_int;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use taos_query::common::*;
use taos_query::{AsyncQueryable, AsyncResultSet};
use taos_sys::DroppableRawRes;

use super::SyncResultSet;
use crate::util::IntoCStr;
use crate::Taos;
// A result should not be clone-able.
// Result set live shorter than query lifetime.
#[derive(Debug)]
pub struct AsyncRs<'q> {
    raw: DroppableRawRes<'q>,
    records: Arc<RwLock<Vec<i32>>>,
}

impl<'q> From<SyncResultSet<'q>> for AsyncRs<'q> {
    fn from(rs: SyncResultSet<'q>) -> Self {
        Self {
            raw: rs.raw,
            records: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<'q> AsyncRs<'q> {
    #[inline]
    fn new(raw: DroppableRawRes<'q>) -> Self {
        Self {
            raw,
            records: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<'q> AsyncResultSet for AsyncRs<'q> {
    type BlockStream = crate::block::BlockStream<'q>;

    #[inline]
    fn affected_rows(&self) -> i32 {
        self.raw.affected_rows()
    }

    #[inline]
    fn precision(&self) -> Precision {
        self.raw.precision()
    }

    #[inline]
    fn fields(&self) -> &[Field] {
        &self.raw.fields()
    }

    #[inline]
    fn summary(&self) -> (usize, usize) {
        let records = self.records.read().unwrap();
        (
            records.len(),
            records.iter().fold(0, |mut acc, v| {
                acc += *v as usize;
                acc
            }),
        )
    }

    fn block_stream(&self) -> Self::BlockStream {
        crate::block::BlockStream::from_raw(self.raw.raw(), self.records.clone())
    }
}

#[async_trait]
impl<'q> AsyncQueryable<'q> for Taos {
    type Error = super::Error;

    type AsyncResultSet = AsyncRs<'q>;
    async fn query<T: AsRef<str> + Send>(
        &'q self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        use tokio::sync::oneshot::{channel, Sender};
        let (sender, rx) = channel::<Result<Self::AsyncResultSet, taos_error::Error>>();

        pub unsafe extern "C" fn async_query_callback(
            param: *mut c_void,
            ptr: *mut c_void,
            code: c_int,
        ) {
            let sender = param as *mut Sender<_>;
            let sender = Box::from_raw(sender);
            let code = if code > 0 { 0 } else { code };
            let res = DroppableRawRes::from_ptr_with_code(ptr, code.into()).map(AsyncRs::new);

            log::trace!(
                "in async query callback, got TAOS_RES: {res:?}, will be send to:{sender:?}"
            );

            sender.send(res).unwrap();
            log::trace!("ptr: {ptr:#?}, code: {code}");
        }
        self.0.query_a(
            sql.as_ref().into_c_str().as_ptr(),
            async_query_callback as _,
            Box::into_raw(Box::new(sender)) as *mut _,
        );
        Ok(rx.await.unwrap()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use taos_macros::test;

    #[test(crate)]
    async fn async_query_de(taos: &Taos, _database: &str) -> Result<()> {
        use taos_query::AsyncQueryable;
        let rs: AsyncRs =
            <Taos as AsyncQueryable>::query(taos, "select * from log.logs limit 10000")
                .await?
                .into();

        assert!(rs.fields().len() == 5);
        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct Record {
            ts: String,
            level: i8,
            content: String,
            dnode_id: i32,
            dnode_ep: String,
        }

        use futures::prelude::stream::*;
        use taos_query::BlockExt;
        while let Some(block) = rs.block_stream().next().await {
            // let _: Record = record?;
            let des =
                itertools::Itertools::collect_vec(block.deserialize::<(i64, i32, &str)>().take(1));
            log::info!("first row in block: {:?}", des);
        }
        let (blocks, records) = rs.summary();
        println!("total blocks: {}, total rows: {}", blocks, records);
        assert_eq!(records, 10000);
        Ok(())
    }
}
