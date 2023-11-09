use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;

use super::TransformExt;

/// TODO(@Yuanpai Zhang): implement map transform.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Filter(Vec<FilterImpl>);

impl TransformExt for Filter {
    fn transform_record_batch(
        &self,
        records: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, super::Error> {
        // TODO: implement
        Ok(records.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum FilterImpl {
    Expr(String),
    Match(LinkedHashMap<String, JsonValue>),
}

impl<'de> Deserialize<'de> for FilterImpl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterImplVisitor;
        impl<'de> serde::de::Visitor<'de> for FilterImplVisitor {
            type Value = FilterImpl;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or a map")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(FilterImpl::Expr(value.to_string()))
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let mut fields = LinkedHashMap::new();
                while let Some((key, value)) = map.next_entry::<String, JsonValue>()? {
                    fields.insert(key, value);
                }
                Ok(FilterImpl::Match(fields))
            }
        }
        deserializer.deserialize_any(FilterImplVisitor)
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterVisitor;
        impl<'de> serde::de::Visitor<'de> for FilterVisitor {
            type Value = Filter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("array or string or map")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(Filter(vec![FilterImpl::Expr(value.to_string())]))
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let mut fields = LinkedHashMap::new();
                while let Some((key, value)) = map.next_entry::<String, JsonValue>()? {
                    fields.insert(key, value);
                }
                Ok(Filter(vec![FilterImpl::Match(fields)]))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut v = Vec::new();
                while let Some(value) = seq.next_element::<FilterImpl>()? {
                    v.push(value)
                }
                Ok(Filter(v))
            }
        }
        deserializer.deserialize_any(FilterVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_impl() {
        let filter = r#""a > b && c != 0""#;
        let filter: FilterImpl = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#"{ "d": 5, "e": 6 }"#;
        let filter: FilterImpl = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#"{ "d": 5, "f": "^bc" }"#;
        let filter: FilterImpl = serde_json::from_str(filter).unwrap();
        dbg!(filter);
    }
    #[test]
    fn test_single_filter() {
        let filter = r#"["a > b && c != 0"]"#;
        let filter: Filter = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#"["a > b && c != 0", { "d": 5, "e": 6}]"#;
        let filter: Filter = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#""a > b && c != 0""#;
        let filter: Filter = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#"{ "a": "/abc/" }"#;
        let filter: Filter = serde_json::from_str(filter).unwrap();
        dbg!(filter);
    }
}
