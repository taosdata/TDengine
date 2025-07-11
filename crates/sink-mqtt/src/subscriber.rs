use std::time::Duration;

use snafu::{OptionExt, ResultExt};
use taos::{
    AsAsyncConsumer, AsyncTBuilder, Consumer, Data, IsAsyncMeta, MessageSet, Meta, Offset, Timeout,
    TmqBuilder, taos_query::tmq::IsAsyncData,
};

use crate::{
    config::TmqConfig,
    message::{Message, MessageOffset},
};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("build from dsn error"))]
    FromDsn { source: taos::Error },
    #[snafu(display("build tmq subscriber error"))]
    BuildTmq { source: taos::Error },
    #[snafu(display("subscribe consumer error"))]
    Subscribe { source: taos::Error },
    #[snafu(display("fetch new message error"))]
    FetchMessage { source: taos::Error },
    #[snafu(display("subject in dsn not found"))]
    SubjectNotFound,
    #[snafu(display("fetch raw block error"))]
    FetchRawBlock { source: taos::Error },
}

type Result<T> = std::result::Result<T, Error>;

pub struct Subscriber {
    consumer: Consumer,
    with_meta: bool,
}

impl Subscriber {
    pub async fn new(mut config: TmqConfig) -> Result<Self> {
        if let Some(client_id) = config.dsn.get("client.id") {
            if config.concurrency > 1 {
                let client_id = format!("{client_id}_{}", uuid::Uuid::new_v4().simple());
                config.dsn.set("client.id", client_id);
            }
        }
        let topics = {
            config
                .dsn
                .subject
                .as_ref()
                .context(SubjectNotFoundSnafu)?
                .split(",")
                .collect::<Vec<_>>()
        };
        let mut consumer = TmqBuilder::from_dsn(&config.dsn)
            .context(FromDsnSnafu)?
            .build()
            .await
            .context(BuildTmqSnafu)?;
        consumer.subscribe(topics).await.context(SubscribeSnafu)?;

        Ok(Self {
            consumer,
            with_meta: config.with_meta,
        })
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
            MessageSet::Meta(meta) if self.with_meta => process_meta(meta, &offset).await,
            MessageSet::Data(data) => process_data(data, &offset).await?,
            MessageSet::MetaData(meta, data) => {
                let mut messages = Vec::new();
                if self.with_meta {
                    messages.extend(process_meta(meta, &offset).await);
                }
                messages.extend(process_data(data, &offset).await?);
                messages
            }
            _ => vec![],
        };
        Ok(Some((offset, messages)))
    }
}

async fn process_meta(meta: Meta, offset: &Offset) -> Vec<Message> {
    let Ok(meta) = meta.as_json_meta().await else {
        return vec![];
    };

    let offset: MessageOffset = offset.into();
    meta.into_iter()
        .map(|meta| (meta, offset.clone()).into())
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
