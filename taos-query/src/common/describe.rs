use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::helpers::ColumnMeta;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Describe(pub(crate) Vec<ColumnMeta>);

impl IntoIterator for Describe {
    type Item = ColumnMeta;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Describe {
    #[inline]
    fn fields(&self) -> &[ColumnMeta] {
        &self.0
    }
    pub fn is_stable(&self) -> bool {
        self.fields().iter().any(|f| f.is_tag())
    }
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.fields().iter().map(|f| f.field())
    }
    pub fn tag_names(&self) -> impl Iterator<Item = &str> {
        self.fields()
            .iter()
            .filter(|f| f.is_tag())
            .map(|f| f.field())
    }
    pub fn to_create_table_sql(&self, table: &str) -> String {
        let (cols, tags): (Vec<_>, Vec<_>) = self.fields().iter().partition(|f| !f.is_tag());
        let col_sql = cols.into_iter().map(|f| f.sql_repr()).join(",");

        if tags.len() == 0 {
            format!("create table if not exists {table} ({col_sql})")
        } else {
            let tags_sql = tags.into_iter().map(|f| f.sql_repr()).join(",");
            format!("create table if not exists {table} ({col_sql}) tags({tags_sql})")
        }
    }
}
