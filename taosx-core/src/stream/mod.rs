//! Streaming pipeline for data writer
//!
//! ```text
//!
//!   +++++++++ +           + + + + + +
//!   + reader  +  ------>  + parser  +
//!   + + + + + +           + + + + + +
//!
//! ```
//!
//!

use std::{any::Any, sync::Arc};

use taos::TaosPool;

pub struct DataSource;

pub enum Notify {
    Ok(Option<Vec<u8>>),
    Warn(String),
    Error(String),
}
pub trait DataSourceExt {
    /// A sink may notify the data source that a message has been consumed successfully.
    fn poll_commit(&self);

    /// A sink may poll metadata of a specific path from source.
    fn poll_metadata(&self, path: &str);

    /// A sink may notify the data source ok or warning or error of a message.
    fn notify(&self, notify: Notify);
}

impl futures::Stream for DataSource {
    type Item = CoreResult<DataBlock>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}
pub enum DataBlockType {}
pub struct DataBlock {
    ds: Arc<Option<DataSource>>,
    r#type: DataBlockType,
    body: Box<dyn Any>,
}

impl DataBlock {
    pub fn commit(&self) -> CoreResult<()> {
        Ok(())
    }

    pub fn warn(&self) {}

    pub fn metrics(&self) {}
}

pub struct Transformer {}

pub struct TransformerSink {
    transformers: Vec<Transformer>,
}

pub type CoreError = anyhow::Error;
pub type CoreResult<T> = std::result::Result<T, CoreError>;

impl futures::Sink<DataBlock> for TransformerSink {
    type Error = CoreError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: DataBlock) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }
}

impl futures::Stream for TransformerSink {
    type Item = DataBlock;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct Writer {}

impl futures::Sink<DataBlock> for Writer {
    type Error = anyhow::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: DataBlock) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }
}

pub struct WriterOptions;
pub struct MyWriter {
    pool: TaosPool,
    options: WriterOptions,
}

pub trait WriterStream {
    fn pool_write(&self) -> std::task::Poll<CoreResult<()>>;
}
