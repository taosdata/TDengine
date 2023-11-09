use std::ops::Deref;

use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Modeler(#[serde(deserialize_with = "model_serde::deserialize")] Vec<Table>);

impl Deref for Modeler {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for Modeler {
    type Item = Table;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Modeler {
    type Item = &'a Table;
    type IntoIter = std::slice::Iter<'a, Table>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Op {
    eq: Option<String>,
    is: Option<String>,
    lt: Option<String>,
    lte: Option<String>,
    gt: Option<String>,
    gte: Option<String>,
    r#in: Option<Vec<String>>,
}
// #[serde(untagged)]
// pub enum Op {
//     // Type is
//     Eq { eq: String },
//     Is { is: String },
//     Lt { lt: String },
//     Lte { lte: String },
//     Gt { gt: String },
//     Gte { gte: String },
//     In { r#in: Vec<String> },
// }

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum FieldOp {
    Or { or: Vec<Op> },
    And { and: Vec<Op> },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Table {
    pub name: String,
    #[serde(default)]
    pub using: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default)]
    pub r#where: LinkedHashMap<String, Op>,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum Model {
    V(Vec<Table>),
    O(Table),
}

impl From<Model> for Vec<Table> {
    fn from(value: Model) -> Self {
        match value {
            Model::V(v) => v,
            Model::O(i) => vec![i],
        }
    }
}

mod model_serde {
    use super::{Model, Table};
    use serde::{self, Deserialize, Deserializer};

    type Target = Vec<Table>;
    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<D>(D) -> Result<T, D::Error> where D: Deserializer
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Target, D::Error>
    where
        D: Deserializer<'de>,
    {
        Model::deserialize(deserializer).map(Into::into)
    }
}
