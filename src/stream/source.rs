use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use mdsn::{DsnError, IntoDsn};

use std::{
    any::Any,
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    ffi::c_void,
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{Arc, Weak},
    task::Poll,
};

use futures::{Sink, Stream, StreamExt, TryStreamExt};
use taos::{
    block::{itypes::IsValue, BlockStream, Field, Ty},
    prelude::AsyncFetchable,
    query::Dsn,
    tmq::{Consumer, TmqBuilder},
};

use crate::{plugins::sink::taos::TaosSinkBuilder, stream::stream::XSinkBuilder, util::sync_table};

use super::stream::{SinkProtocol, TaosxSinkItem};

pub use taos::prelude::sync::*;

pub enum XSchema {
    STableBegin,
    STableSpan {
        name: String,
        tags: Vec<Field>,
        fields: Vec<Field>,
        options: Vec<(String, String)>,
    },
    STableEnd,
    TableBegin,
    Table {
        name: String,
        fields: Vec<Field>,
        options: Vec<(String, String)>,
    },
    TableEnd,
    ChildTableBegin,
    ChildTable {
        name: String,
        stable: String,
        fields: Vec<Value>,
    },
    ChildTableEnd,
}

pub trait XSourceBuilder
where
    Self: Sized,
{
    type Error: std::error::Error;
    type Item: TaosxSinkItem;
    type XSource: Stream<Item = Result<Self::Item, Self::Error>>;

    const NAME: &'static str;

    fn from_dsn<T: IntoDsn>(dsn: T) -> Result<Self, Self::Error>;

    fn dsn(&self) -> Cow<Dsn>;

    fn max_workers(&self) -> usize {
        0
    }

    fn database_options(&self) -> Vec<(String, String)> {
        unimplemented!()
    }

    fn schema_iter<I>(&self) -> I
    where
        I: Iterator<Item = XSchema>,
    {
        unimplemented!()
    }

    fn protocol(&self) -> SinkProtocol;

    fn build_source(&mut self) -> Result<Self::XSource, Self::Error>;
}
