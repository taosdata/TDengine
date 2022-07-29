use derive_more::{Deref, DerefMut};
use mdsn::IntoDsn;
use std::{
    borrow::Cow,
    fmt::Debug,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};

use taos::prelude::{RawBlock, SchemalessPrecision, Taos, Value};

use super::transformer::Action;

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum SinkProtocol {
    /// TDengine result set type for [taos::Taos] only.
    ResultSet,
    /// A stream data block from tmq consumer.
    Block,
    /// Raw block as a single bytes.
    RawBlock,
    /// A record for a table or stable.
    Record,
    /// InfluxDB line protocol record.
    SmlLine,
    /// OpenTSDB telnet protocol record string.
    SmlTelnet,
    /// OpenTSDB json protocol.
    SmlJson,
    #[non_exhaustive]
    __NoneExhaustive,
}

#[derive(Debug, Default)]
pub struct Summary {
    pub blocks: AtomicUsize,
    pub rows: AtomicUsize,
}

impl Summary {
    pub fn blocks(&self) -> usize {
        self.blocks.load(Ordering::Acquire)
    }
    pub fn rows(&self) -> usize {
        self.rows.load(Ordering::Acquire)
    }
}

pub trait XSinkBuilder
where
    Self: Sized,
{
    type Error: std::error::Error;
    fn from_dsn<T: IntoDsn>(dsn: T) -> Result<Self, Self::Error>;

    fn with_transformer(self, transformers: Vec<Action>) -> Self;

    fn build_sink(&self) -> Result<XSink<Self::Error>, Self::Error>;

    fn summary(&self) -> &Summary;

    fn build_sink_for_protocol(&self, _: SinkProtocol) -> Result<XSink<Self::Error>, Self::Error> {
        self.build_sink()
    }
}

#[derive(Debug, Deref, DerefMut)]
pub struct XSink<E>(Box<dyn TaosxSink<Error = E>>);

impl<E, T> From<T> for XSink<E>
where
    E: 'static + Send + Sync + std::error::Error,
    T: 'static + TaosxSink<Error = E>,
{
    fn from(value: T) -> Self {
        XSink(Box::new(value))
    }
}

impl<E, I> futures::Sink<I> for XSink<E>
where
    E: 'static + Send + Sync + std::error::Error,
    I: TaosxSinkItem,
{
    type Error = E;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let sink = Pin::into_inner(self);
        match item.protocol() {
            SinkProtocol::ResultSet => todo!(),
            SinkProtocol::Block => {
                let (taos, block) = item.as_block();
                sink.consume_block(taos, block)
            }
            SinkProtocol::RawBlock => {
                if let Some(block) = item.as_raw_block() {
                    sink.consume_raw_block(&block)
                } else {
                    Ok(())
                }
            }
            SinkProtocol::Record => todo!(),
            SinkProtocol::SmlLine => {
                if let Some(line) = item.as_schemaless_line() {
                    sink.consume_schemaless_line(line, item.precision())
                } else {
                    Ok(())
                }
            }
            SinkProtocol::SmlTelnet => {
                if let Some(line) = item.as_schemaless_telnet() {
                    sink.consume_schemaless_telnet(line, item.precision())
                } else {
                    Ok(())
                }
            }
            SinkProtocol::SmlJson => {
                if let Some(line) = item.as_schemaless_json() {
                    sink.consume_schemaless_json(line, item.precision())
                } else {
                    Ok(())
                }
            }
            _ => todo!(),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

pub type TaosxSinkError = anyhow::Error;
pub trait TaosxSink: Debug + Send {
    type Error: 'static + Send + Sync + std::error::Error;
    fn batch_size(&self) -> usize {
        1
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn consume_block(&mut self, _: &Taos, _: &RawBlock) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn consume_raw_block(&mut self, _: &[u8]) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn consume_schemaless_line(
        &mut self,
        _: &str,
        _: SchemalessPrecision,
    ) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn consume_schemaless_telnet(
        &mut self,
        _: &str,
        _: SchemalessPrecision,
    ) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn consume_schemaless_json(
        &mut self,
        _: &str,
        _: SchemalessPrecision,
    ) -> Result<(), Self::Error> {
        unimplemented!()
    }
}

pub trait TaosxSinkItem: Send {
    const PROTOCOL: SinkProtocol;

    fn protocol(&self) -> SinkProtocol {
        Self::PROTOCOL
    }

    fn as_block(&self) -> (&Taos, &RawBlock) {
        unimplemented!()
    }

    fn as_raw_block(&self) -> Option<Cow<[u8]>> {
        None
    }

    fn as_record(&self) -> Option<Cow<[Value]>> {
        None
    }

    fn precision(&self) -> SchemalessPrecision {
        SchemalessPrecision::NonConfigured
    }

    fn as_schemaless_line(&self) -> Option<&str> {
        None
    }

    fn as_schemaless_telnet(&self) -> Option<&str> {
        None
    }

    fn as_schemaless_json(&self) -> Option<&str> {
        None
    }
}

impl TaosxSinkItem for (&Taos, RawBlock) {
    const PROTOCOL: SinkProtocol = SinkProtocol::Block;
    fn as_block(&self) -> (&Taos, &RawBlock) {
        (self.0, &self.1)
    }
}

impl TaosxSinkItem for (&Taos, &RawBlock) {
    const PROTOCOL: SinkProtocol = SinkProtocol::Block;
    fn as_block(&self) -> (&Taos, &RawBlock) {
        (self.0, self.1)
    }
}

pub struct XLine<'a>(&'a str, SchemalessPrecision);

impl<'a> XLine<'a> {
    pub fn new_line(input: &'a str) -> Self {
        XLine(input, SchemalessPrecision::Nanoseconds)
    }
    pub fn new_line_with_precision(input: &'a str, precision: SchemalessPrecision) -> Self {
        XLine(input, precision)
    }
}

impl<'a> TaosxSinkItem for XLine<'a> {
    const PROTOCOL: SinkProtocol = SinkProtocol::SmlLine;
    fn precision(&self) -> SchemalessPrecision {
        self.1
    }
    fn as_schemaless_line(&self) -> Option<&str> {
        Some(self.0)
    }
}
