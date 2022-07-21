use crate::{
    common::{itypes::IValue, Value},
    AsyncQueryable, Queryable,
};

mod column;
pub use column::*;

pub trait Bindable<Q>
where
    Q: Queryable,
    Self: Sized,
{
    type Error;

    fn init(taos: Q) -> Result<Self, Self::Error>;

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> Result<(), Self::Error>;

    fn set_tbname<S: AsRef<str>>(&mut self, sql: S) -> Result<(), Self::Error>;

    fn set_tags(&mut self, tags: &[Value]) -> Result<(), Self::Error>;

    fn bind(&mut self, params: &[RawMultiBind]) -> Result<(), Self::Error>;

    fn add_batch(&mut self) -> Result<(), Self::Error>;

    fn execute(&mut self) -> Result<usize, Self::Error>;

    fn result_set(&mut self) -> Result<Q::ResultSet, Self::Error>;
}

#[async_trait::async_trait]
pub trait AsyncBindable<Q>
where
    Q: AsyncQueryable,
    Self: Sized,
{
    type Error;

    async fn init(taos: Q) -> Result<Self, Self::Error>;

    async fn prepare<S: AsRef<str>>(&mut self, sql: S) -> Result<(), Self::Error>;

    async fn set_tbname<S: AsRef<str>>(&mut self, sql: S) -> Result<(), Self::Error>;

    async fn set_tags(&mut self, tags: &[Value]) -> Result<(), Self::Error>;

    async fn bind(&mut self, params: &[RawMultiBind]) -> Result<(), Self::Error>;

    async fn add_batch(&mut self) -> Result<(), Self::Error>;

    async fn execute(&mut self) -> Result<usize, Self::Error>;

    async fn result_set(&mut self) -> Result<Q::AsyncResultSet, Self::Error>;
}
