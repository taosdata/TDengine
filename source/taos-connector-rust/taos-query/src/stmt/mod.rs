use crate::{AsyncQueryable, ColumnView, Queryable, RawResult, Value};

pub trait Bindable<Q>
where
    Q: Queryable,
    Self: Sized,
{
    fn init(taos: &Q) -> RawResult<Self>;

    fn init_with_req_id(taos: &Q, req_id: u64) -> RawResult<Self>;

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self>;

    fn set_tbname<S: AsRef<str>>(&mut self, name: S) -> RawResult<&mut Self>;

    fn set_tags(&mut self, tags: &[Value]) -> RawResult<&mut Self>;

    fn set_tbname_tags<S: AsRef<str>>(&mut self, name: S, tags: &[Value]) -> RawResult<&mut Self> {
        self.set_tbname(name)?.set_tags(tags)
    }

    fn bind(&mut self, params: &[ColumnView]) -> RawResult<&mut Self>;

    fn add_batch(&mut self) -> RawResult<&mut Self>;

    fn execute(&mut self) -> RawResult<usize>;

    fn affected_rows(&self) -> usize;

    fn result_set(&mut self) -> RawResult<Q::ResultSet> {
        todo!()
    }
}

#[async_trait::async_trait]
pub trait AsyncBindable<Q>
where
    Q: AsyncQueryable,
    Self: Sized,
{
    async fn init(taos: &Q) -> RawResult<Self>;

    async fn init_with_req_id(taos: &Q, req_id: u64) -> RawResult<Self>;

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self>;

    async fn set_tbname(&mut self, name: &str) -> RawResult<&mut Self>;

    async fn set_tags(&mut self, tags: &[Value]) -> RawResult<&mut Self>;

    async fn set_tbname_tags(&mut self, name: &str, tags: &[Value]) -> RawResult<&mut Self> {
        self.set_tbname(name).await?.set_tags(tags).await
    }

    async fn bind(&mut self, params: &[ColumnView]) -> RawResult<&mut Self>;

    async fn add_batch(&mut self) -> RawResult<&mut Self>;

    async fn execute(&mut self) -> RawResult<usize>;

    async fn affected_rows(&self) -> usize;

    async fn result_set(&mut self) -> RawResult<Q::AsyncResultSet> {
        todo!()
    }
}
