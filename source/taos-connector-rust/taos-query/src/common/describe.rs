use std::ops::{Deref, DerefMut};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::helpers::ColumnMeta;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct Describe(pub(crate) Vec<ColumnMeta>);

impl IntoIterator for Describe {
    type Item = ColumnMeta;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<ColumnMeta> for Describe {
    fn from_iter<T: IntoIterator<Item = ColumnMeta>>(iter: T) -> Self {
        Describe(iter.into_iter().collect())
    }
}

impl Deref for Describe {
    type Target = Vec<ColumnMeta>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Describe {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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

    pub fn split(&self) -> Option<(&[ColumnMeta], &[ColumnMeta])> {
        if let Some((at, _)) = self.deref().iter().find_position(|c| c.is_tag()) {
            Some(self.deref().split_at(at))
        } else {
            Some(self.deref().split_at(self.0.len()))
        }
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

        if tags.is_empty() {
            format!("create table if not exists `{table}` ({col_sql})")
        } else {
            let tags_sql = tags.into_iter().map(|f| f.short_sql_repr()).join(",");
            format!("create table if not exists `{table}` ({col_sql}) tags({tags_sql})")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{CompressOptions, Described};
    use crate::Ty;

    #[test]
    fn test_describe() {
        let desc = Describe(vec![
            ColumnMeta::Column(Described::new("ts", Ty::Timestamp, None)),
            ColumnMeta::Column(Described::new("v1", Ty::Int, None)),
        ]);
        assert_eq!(desc.is_stable(), false);
        assert_eq!(desc.names().collect::<Vec<_>>(), vec!["ts", "v1"]);
        assert_eq!(desc.tag_names().count(), 0);
        assert_eq!(
            desc.to_create_table_sql("tb1"),
            "create table if not exists `tb1` (`ts` TIMESTAMP,`v1` INT)"
        );

        let desc = Describe(vec![
            ColumnMeta::Column(Described::new("ts", Ty::Timestamp, None)),
            ColumnMeta::Column(Described::new("v1", Ty::Int, None)),
            ColumnMeta::Tag(
                Described::new("t1", Ty::Int, None).with_compression(CompressOptions::disabled()),
            ),
        ]);
        assert_eq!(desc.is_stable(), true);
        assert_eq!(desc.names().collect::<Vec<_>>(), vec!["ts", "v1", "t1"]);
        assert_eq!(desc.tag_names().count(), 1);
        assert_eq!(
            desc.to_create_table_sql("tb1"),
            "create table if not exists `tb1` (`ts` TIMESTAMP,`v1` INT) tags(`t1` INT)"
        );
    }
}
