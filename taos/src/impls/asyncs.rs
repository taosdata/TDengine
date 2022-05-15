use std::ffi::c_void;
use std::os::raw::c_int;

use async_trait::async_trait;

use taos_query::common::*;
use taos_query::{AsyncQueryable, AsyncFetchable};
use taos_sys::DroppableRawRes;

use super::ResultSet;
use crate::util::IntoCStr;
use crate::Taos;

impl AsyncFetchable for ResultSet {
    type BlockStream = crate::block::BlockStream;

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
        use std::sync::atomic::Ordering::SeqCst;
        (
            self.summary.0.load(SeqCst) as _,
            self.summary.1.load(SeqCst) as _,
        )
    }

    #[inline]
    fn block_stream(&mut self) -> Self::BlockStream {
        crate::block::BlockStream::from_raw(self.raw.raw(), self.summary.clone())
    }
}

#[async_trait]
impl<'q> AsyncQueryable<'q> for Taos {
    type Error = super::Error;

    type AsyncResultSet = ResultSet;

    /// Query use taosc query_a API.
    async fn query<T: AsRef<str> + Send>(
        &'q self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        // todo(3.0): remove these line to use taos_query_a in async/await impl.
        if crate::client_info().starts_with("3") {
            let raw = self.0.query(sql.as_ref().into_c_str().as_ptr())?;
            return Ok(ResultSet::new(raw));
        }
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
            let res = DroppableRawRes::from_ptr_with_code(ptr, code.into()).map(ResultSet::new);

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

    #[test(log_level = "info")]
    async fn async_query_de(taos: &Taos, _database: &str) -> Result<()> {
        use taos_query::{AsyncQueryable, AsyncFetchable};
        taos.exec("create table tb1 (ts timestamp, level tinyint, content varchar(100), dnode_id int, dnode_ep varchar(100))")
            .await?;
        taos.exec("insert into tb1 values(now, 1, '', 1, 'abc')")
            .await?;
        let mut rs = <Taos as AsyncQueryable>::query(taos, "select * from tb1").await?;

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
            let des = itertools::Itertools::collect_vec(
                block.deserialize::<(i64, i32, &str, i32, String)>().take(1),
            );
            log::info!("first row in block: {:?}", des);
        }
        let (blocks, records) = rs.summary();
        println!("total blocks: {}, total rows: {}", blocks, records);
        assert!(records <= 10000);
        Ok(())
    }
}
