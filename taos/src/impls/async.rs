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
    precision: Precision,
    records: Arc<RwLock<Vec<i32>>>,
}

impl<'q> From<SyncResultSet<'q>> for AsyncRs<'q> {
    fn from(rs: SyncResultSet<'q>) -> Self {
        Self {
            raw: rs.raw,
            precision: rs.precision,
            records: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<'q> AsyncRs<'q> {
    fn new(raw: DroppableRawRes<'q>) -> Self {
        let precision = raw.precision();
        Self {
            raw,
            precision,
            records: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<'q> AsyncResultSet for AsyncRs<'q> {
    type BlockStream = crate::block::BlockStream<'q>;

    fn fields(&self) -> &[Field] {
        &self.raw.fields()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

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
    ) -> Result<Result<Self::AsyncResultSet, usize>, Self::Error> {
        use tokio::sync::oneshot::{channel, Sender};
        let (sender, rx) =
            channel::<Result<Result<Self::AsyncResultSet, usize>, taos_error::Error>>();

        pub unsafe extern "C" fn async_query_callback(
            param: *mut c_void,
            ptr: *mut c_void,
            code: c_int,
        ) {
            let sender = param as *mut Sender<_>;
            let sender = Box::from_raw(sender);

            let res = match code {
                code if code > 0 => Ok(Err(code as usize)),
                _ => DroppableRawRes::from_ptr_with_code(ptr, code.into())
                    .map(AsyncRs::new)
                    .map(Ok),
            };
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
                .unwrap()
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
        Ok(())
    }
}
