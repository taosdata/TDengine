use crate::{
    common::{views::ColumnView, Value},
    Queryable,
};

mod column;
pub use column::*;

pub trait Bindable<Q>
where
    Q: Queryable,
    Self: Sized,
{
    type Error;

    fn init(taos: &Q) -> Result<Self, Self::Error>;

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> Result<&mut Self, Self::Error>;

    fn set_tbname<S: AsRef<str>>(&mut self, name: S) -> Result<&mut Self, Self::Error>;

    fn set_tags(&mut self, tags: &[Value]) -> Result<&mut Self, Self::Error>;

    fn set_tbname_tags<S: AsRef<str>>(&mut self, name: S, tags: &[Value]) -> Result<&mut Self, Self::Error> {
        self.set_tbname(name)?.set_tags(tags)
    }

    fn bind(&mut self, params: &[ColumnView]) -> Result<&mut Self, Self::Error>;

    fn add_batch(&mut self) -> Result<&mut Self, Self::Error>;

    fn execute(&mut self) -> Result<usize, Self::Error>;

    fn affected_rows(&self) -> usize;

    fn result_set(&mut self) -> Result<Q::ResultSet, Self::Error> {
        todo!()
    }
}

// #[async_trait::async_trait]
// pub trait AsyncBindable<Q>
// where
//     Q: AsyncQueryable,
//     Self: Sized,
// {
//     type Error;

//     async fn init(taos: Q) -> Result<Self, Self::Error>;

//     async fn prepare<S: AsRef<str>>(&mut self, sql: S) -> Result<(), Self::Error>;

//     async fn set_tbname<S: AsRef<str>>(&mut self, sql: S) -> Result<(), Self::Error>;

//     async fn set_tags(&mut self, tags: &[Value]) -> Result<(), Self::Error>;

//     async fn bind(&mut self, params: &[ColumnView]) -> Result<(), Self::Error>;

//     async fn add_batch(&mut self) -> Result<(), Self::Error>;

//     async fn execute(&mut self) -> Result<usize, Self::Error>;

//     async fn result_set(&mut self) -> Result<Q::AsyncResultSet, Self::Error>;
// }
