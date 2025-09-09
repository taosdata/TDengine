use std::time::Duration;

use snafu::ResultExt;
use taos::{
    AsAsyncConsumer, Consumer, Data, IsAsyncMeta, MessageSet, Meta, Offset, Timeout,
    taos_query::tmq::IsAsyncData,
};

use crate::message::{Message, MessageOffset};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("fetch new message error"))]
    FetchMessage { source: taos::Error },
    #[snafu(display("fetch raw block error"))]
    FetchRawBlock { source: taos::Error },
}

type Result<T> = std::result::Result<T, Error>;

pub struct Subscriber {
    consumer: Consumer,
    with_meta: bool,
    with_meta_delete: bool,
    with_meta_drop: bool,
}

impl Subscriber {
    pub fn new(
        consumer: Consumer,
        with_meta: bool,
        with_meta_delete: bool,
        with_meta_drop: bool,
    ) -> Self {
        Self {
            consumer,
            with_meta,
            with_meta_delete,
            with_meta_drop,
        }
    }

    pub async fn next(&self) -> Result<Option<(Offset, Vec<Message>)>> {
        let res = self
            .consumer
            .recv_timeout(Timeout::Duration(Duration::from_secs(5)))
            .await
            .context(FetchMessageSnafu)?;
        let Some((offset, message)) = res else {
            return Ok(None);
        };
        let messages = match message {
            MessageSet::Meta(meta) if self.with_meta => {
                process_meta(meta, &offset, self.with_meta_delete, self.with_meta_drop).await
            }
            MessageSet::Data(data) => process_data(data, &offset).await?,
            MessageSet::MetaData(meta, data) => {
                let mut messages = Vec::new();
                if self.with_meta {
                    messages.extend(
                        process_meta(meta, &offset, self.with_meta_delete, self.with_meta_drop)
                            .await,
                    );
                }
                messages.extend(process_data(data, &offset).await?);
                messages
            }
            _ => vec![],
        };
        Ok(Some((offset, messages)))
    }
}

async fn process_meta(
    meta: Meta,
    offset: &Offset,
    with_meta_delete: bool,
    with_meta_drop: bool,
) -> Vec<Message> {
    let Ok(meta) = meta.as_json_meta().await else {
        return vec![];
    };

    let offset: MessageOffset = offset.into();
    meta.into_iter()
        .filter_map(|meta| match meta {
            m @ (taos::MetaUnit::Create(_) | taos::MetaUnit::Alter(_)) => {
                Some((m, offset.clone()).into())
            }
            m @ taos::MetaUnit::Drop(_) if with_meta_drop => Some((m, offset.clone()).into()),
            m @ taos::MetaUnit::Delete(_) if with_meta_delete => Some((m, offset.clone()).into()),
            _ => None,
        })
        .collect()
}

async fn process_data(data: Data, offset: &Offset) -> Result<Vec<Message>> {
    let offset: MessageOffset = offset.into();
    let mut ret = Vec::new();
    while let Some(block) = data.fetch_raw_block().await.context(FetchRawBlockSnafu)? {
        let table_name = block.table_name();
        for value in block.deserialize::<serde_json::Map<String, serde_json::Value>>() {
            match value {
                Ok(data) => {
                    ret.push(Message::new_data(data, offset.clone(), table_name));
                }
                Err(e) => {
                    tracing::error!("deserialize rawblock data error: {e}");
                }
            }
        }
    }

    Ok(ret)
}
