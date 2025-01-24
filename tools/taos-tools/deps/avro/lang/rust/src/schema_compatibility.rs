// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Logic for checking schema compatibility
use crate::schema::{Schema, SchemaKind};
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::Hasher,
    ptr,
};

pub struct SchemaCompatibility;

struct Checker {
    recursion: HashSet<(u64, u64)>,
}

impl Checker {
    /// Create a new checker, with recursion set to an empty set.
    pub(crate) fn new() -> Self {
        Self {
            recursion: HashSet::new(),
        }
    }

    pub(crate) fn can_read(&mut self, writers_schema: &Schema, readers_schema: &Schema) -> bool {
        self.full_match_schemas(writers_schema, readers_schema)
    }

    pub(crate) fn full_match_schemas(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> bool {
        if self.recursion_in_progress(writers_schema, readers_schema) {
            return true;
        }

        if !SchemaCompatibility::match_schemas(writers_schema, readers_schema) {
            return false;
        }

        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        if w_type != SchemaKind::Union && (r_type.is_primitive() || r_type == SchemaKind::Fixed) {
            return true;
        }

        match r_type {
            SchemaKind::Record => self.match_record_schemas(writers_schema, readers_schema),
            SchemaKind::Map => {
                if let Schema::Map(w_m) = writers_schema {
                    if let Schema::Map(r_m) = readers_schema {
                        self.full_match_schemas(w_m, r_m)
                    } else {
                        unreachable!("readers_schema should have been Schema::Map")
                    }
                } else {
                    unreachable!("writers_schema should have been Schema::Map")
                }
            }
            SchemaKind::Array => {
                if let Schema::Array(w_a) = writers_schema {
                    if let Schema::Array(r_a) = readers_schema {
                        self.full_match_schemas(w_a, r_a)
                    } else {
                        unreachable!("readers_schema should have been Schema::Array")
                    }
                } else {
                    unreachable!("writers_schema should have been Schema::Array")
                }
            }
            SchemaKind::Union => self.match_union_schemas(writers_schema, readers_schema),
            SchemaKind::Enum => {
                // reader's symbols must contain all writer's symbols
                if let Schema::Enum {
                    symbols: w_symbols, ..
                } = writers_schema
                {
                    if let Schema::Enum {
                        symbols: r_symbols, ..
                    } = readers_schema
                    {
                        return !w_symbols.iter().any(|e| !r_symbols.contains(e));
                    }
                }
                false
            }
            _ => {
                if w_type == SchemaKind::Union {
                    if let Schema::Union(r) = writers_schema {
                        if r.schemas.len() == 1 {
                            return self.full_match_schemas(&r.schemas[0], readers_schema);
                        }
                    }
                }
                false
            }
        }
    }

    fn match_record_schemas(&mut self, writers_schema: &Schema, readers_schema: &Schema) -> bool {
        let w_type = SchemaKind::from(writers_schema);

        if w_type == SchemaKind::Union {
            return false;
        }

        if let Schema::Record {
            fields: w_fields,
            lookup: w_lookup,
            ..
        } = writers_schema
        {
            if let Schema::Record {
                fields: r_fields, ..
            } = readers_schema
            {
                for field in r_fields.iter() {
                    if let Some(pos) = w_lookup.get(&field.name) {
                        if !self.full_match_schemas(&w_fields[*pos].schema, &field.schema) {
                            return false;
                        }
                    } else if field.default.is_none() {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn match_union_schemas(&mut self, writers_schema: &Schema, readers_schema: &Schema) -> bool {
        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        assert_eq!(r_type, SchemaKind::Union);

        if w_type == SchemaKind::Union {
            if let Schema::Union(u) = writers_schema {
                u.schemas
                    .iter()
                    .all(|schema| self.full_match_schemas(schema, readers_schema))
            } else {
                unreachable!("writers_schema should have been Schema::Union")
            }
        } else if let Schema::Union(u) = readers_schema {
            u.schemas
                .iter()
                .any(|schema| self.full_match_schemas(writers_schema, schema))
        } else {
            unreachable!("readers_schema should have been Schema::Union")
        }
    }

    fn recursion_in_progress(&mut self, writers_schema: &Schema, readers_schema: &Schema) -> bool {
        let mut hasher = DefaultHasher::new();
        ptr::hash(writers_schema, &mut hasher);
        let w_hash = hasher.finish();

        hasher = DefaultHasher::new();
        ptr::hash(readers_schema, &mut hasher);
        let r_hash = hasher.finish();

        let key = (w_hash, r_hash);
        // This is a shortcut to add if not exists *and* return false. It will return true
        // if it was able to insert.
        !self.recursion.insert(key)
    }
}

impl SchemaCompatibility {
    /// `can_read` performs a full, recursive check that a datum written using the
    /// writers_schema can be read using the readers_schema.
    pub fn can_read(writers_schema: &Schema, readers_schema: &Schema) -> bool {
        let mut c = Checker::new();
        c.can_read(writers_schema, readers_schema)
    }

    /// `mutual_read` performs a full, recursive check that a datum written using either
    /// the writers_schema or the readers_schema can be read using the other schema.
    pub fn mutual_read(writers_schema: &Schema, readers_schema: &Schema) -> bool {
        SchemaCompatibility::can_read(writers_schema, readers_schema)
            && SchemaCompatibility::can_read(readers_schema, writers_schema)
    }

    ///  `match_schemas` performs a basic check that a datum written with the
    ///  writers_schema could be read using the readers_schema. This check only includes
    ///  matching the types, including schema promotion, and matching the full name for
    ///  named types. Aliases for named types are not supported here, and the rust
    ///  implementation of Avro in general does not include support for aliases (I think).
    pub(crate) fn match_schemas(writers_schema: &Schema, readers_schema: &Schema) -> bool {
        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        if w_type == SchemaKind::Union || r_type == SchemaKind::Union {
            return true;
        }

        if w_type == r_type {
            if r_type.is_primitive() {
                return true;
            }

            match r_type {
                SchemaKind::Record => {
                    if let Schema::Record { name: w_name, .. } = writers_schema {
                        if let Schema::Record { name: r_name, .. } = readers_schema {
                            return w_name.fullname(None) == r_name.fullname(None);
                        } else {
                            unreachable!("readers_schema should have been Schema::Record")
                        }
                    } else {
                        unreachable!("writers_schema should have been Schema::Record")
                    }
                }
                SchemaKind::Fixed => {
                    if let Schema::Fixed {
                        name: w_name,
                        size: w_size,
                    } = writers_schema
                    {
                        if let Schema::Fixed {
                            name: r_name,
                            size: r_size,
                        } = readers_schema
                        {
                            return w_name.fullname(None) == r_name.fullname(None)
                                && w_size == r_size;
                        } else {
                            unreachable!("readers_schema should have been Schema::Fixed")
                        }
                    } else {
                        unreachable!("writers_schema should have been Schema::Fixed")
                    }
                }
                SchemaKind::Enum => {
                    if let Schema::Enum { name: w_name, .. } = writers_schema {
                        if let Schema::Enum { name: r_name, .. } = readers_schema {
                            return w_name.fullname(None) == r_name.fullname(None);
                        } else {
                            unreachable!("readers_schema should have been Schema::Enum")
                        }
                    } else {
                        unreachable!("writers_schema should have been Schema::Enum")
                    }
                }
                SchemaKind::Map => {
                    if let Schema::Map(w_m) = writers_schema {
                        if let Schema::Map(r_m) = readers_schema {
                            return SchemaCompatibility::match_schemas(w_m, r_m);
                        } else {
                            unreachable!("readers_schema should have been Schema::Map")
                        }
                    } else {
                        unreachable!("writers_schema should have been Schema::Map")
                    }
                }
                SchemaKind::Array => {
                    if let Schema::Array(w_a) = writers_schema {
                        if let Schema::Array(r_a) = readers_schema {
                            return SchemaCompatibility::match_schemas(w_a, r_a);
                        } else {
                            unreachable!("readers_schema should have been Schema::Array")
                        }
                    } else {
                        unreachable!("writers_schema should have been Schema::Array")
                    }
                }
                _ => (),
            };
        }

        if w_type == SchemaKind::Int
            && vec![SchemaKind::Long, SchemaKind::Float, SchemaKind::Double]
                .iter()
                .any(|&t| t == r_type)
        {
            return true;
        }

        if w_type == SchemaKind::Long
            && vec![SchemaKind::Float, SchemaKind::Double]
                .iter()
                .any(|&t| t == r_type)
        {
            return true;
        }

        if w_type == SchemaKind::Float && r_type == SchemaKind::Double {
            return true;
        }

        if w_type == SchemaKind::String && r_type == SchemaKind::Bytes {
            return true;
        }

        if w_type == SchemaKind::Bytes && r_type == SchemaKind::String {
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"int"}"#).unwrap()
    }

    fn long_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"long"}"#).unwrap()
    }

    fn string_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"string"}"#).unwrap()
    }

    fn int_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"int"}"#).unwrap()
    }

    fn long_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"long"}"#).unwrap()
    }

    fn string_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"string"}"#).unwrap()
    }

    fn enum1_ab_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["A","B"]}"#).unwrap()
    }

    fn enum1_abc_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["A","B","C"]}"#).unwrap()
    }

    fn enum1_bc_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["B","C"]}"#).unwrap()
    }

    fn enum2_ab_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum2", "symbols":["A","B"]}"#).unwrap()
    }

    fn empty_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[]}"#).unwrap()
    }

    fn empty_record2_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record2", "fields": []}"#).unwrap()
    }

    fn a_int_record1_schema() -> Schema {
        Schema::parse_str(
            r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}]}"#,
        )
        .unwrap()
    }

    fn a_long_record1_schema() -> Schema {
        Schema::parse_str(
            r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"long"}]}"#,
        )
        .unwrap()
    }

    fn a_int_b_int_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int"}]}"#).unwrap()
    }

    fn a_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}]}"#).unwrap()
    }

    fn a_int_b_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int", "default":0}]}"#).unwrap()
    }

    fn a_dint_b_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}, {"name":"b", "type":"int", "default":0}]}"#).unwrap()
    }

    fn nested_record() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"parent","fields":[{"name":"attribute","type":{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}}]}"#).unwrap()
    }

    fn nested_optional_record() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"parent","fields":[{"name":"attribute","type":["null",{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}],"default":null}]}"#).unwrap()
    }

    fn int_list_record_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"List", "fields": [{"name": "head", "type": "int"},{"name": "tail", "type": "array", "items": "int"}]}"#).unwrap()
    }

    fn long_list_record_schema() -> Schema {
        Schema::parse_str(
            r#"
      {
        "type":"record", "name":"List", "fields": [
          {"name": "head", "type": "long"},
          {"name": "tail", "type": "array", "items": "long"}
      ]}
"#,
        )
        .unwrap()
    }

    fn union_schema(schemas: Vec<Schema>) -> Schema {
        let schema_string = schemas
            .iter()
            .map(|s| s.canonical_form())
            .collect::<Vec<String>>()
            .join(",");
        dbg!(&schema_string);
        Schema::parse_str(&format!("[{}]", schema_string)).unwrap()
    }

    fn empty_union_schema() -> Schema {
        union_schema(vec![])
    }

    // unused
    // fn null_union_schema() -> Schema { union_schema(vec![Schema::Null]) }

    fn int_union_schema() -> Schema {
        union_schema(vec![Schema::Int])
    }

    fn long_union_schema() -> Schema {
        union_schema(vec![Schema::Long])
    }

    fn string_union_schema() -> Schema {
        union_schema(vec![Schema::String])
    }

    fn int_string_union_schema() -> Schema {
        union_schema(vec![Schema::Int, Schema::String])
    }

    fn string_int_union_schema() -> Schema {
        union_schema(vec![Schema::String, Schema::Int])
    }

    #[test]
    fn test_broken() {
        assert!(!SchemaCompatibility::can_read(
            &int_string_union_schema(),
            &int_union_schema()
        ))
    }

    #[test]
    fn test_incompatible_reader_writer_pairs() {
        let incompatible_schemas = vec![
            // null
            (Schema::Null, Schema::Int),
            (Schema::Null, Schema::Long),
            // boolean
            (Schema::Boolean, Schema::Int),
            // int
            (Schema::Int, Schema::Null),
            (Schema::Int, Schema::Boolean),
            (Schema::Int, Schema::Long),
            (Schema::Int, Schema::Float),
            (Schema::Int, Schema::Double),
            // long
            (Schema::Long, Schema::Float),
            (Schema::Long, Schema::Double),
            // float
            (Schema::Float, Schema::Double),
            // string
            (Schema::String, Schema::Boolean),
            (Schema::String, Schema::Int),
            // bytes
            (Schema::Bytes, Schema::Null),
            (Schema::Bytes, Schema::Int),
            // array and maps
            (int_array_schema(), long_array_schema()),
            (int_map_schema(), int_array_schema()),
            (int_array_schema(), int_map_schema()),
            (int_map_schema(), long_map_schema()),
            // enum
            (enum1_ab_schema(), enum1_abc_schema()),
            (enum1_bc_schema(), enum1_abc_schema()),
            (enum1_ab_schema(), enum2_ab_schema()),
            (Schema::Int, enum2_ab_schema()),
            (enum2_ab_schema(), Schema::Int),
            //union
            (int_union_schema(), int_string_union_schema()),
            (string_union_schema(), int_string_union_schema()),
            //record
            (empty_record2_schema(), empty_record1_schema()),
            (a_int_record1_schema(), empty_record1_schema()),
            (a_int_b_dint_record1_schema(), empty_record1_schema()),
            (int_list_record_schema(), long_list_record_schema()),
            (nested_record(), nested_optional_record()),
        ];

        assert!(!incompatible_schemas
            .iter()
            .any(|(reader, writer)| SchemaCompatibility::can_read(writer, reader)));
    }

    #[test]
    fn test_compatible_reader_writer_pairs() {
        let compatible_schemas = vec![
            (Schema::Null, Schema::Null),
            (Schema::Long, Schema::Int),
            (Schema::Float, Schema::Int),
            (Schema::Float, Schema::Long),
            (Schema::Double, Schema::Long),
            (Schema::Double, Schema::Int),
            (Schema::Double, Schema::Float),
            (Schema::String, Schema::Bytes),
            (Schema::Bytes, Schema::String),
            (int_array_schema(), int_array_schema()),
            (long_array_schema(), int_array_schema()),
            (int_map_schema(), int_map_schema()),
            (long_map_schema(), int_map_schema()),
            (enum1_ab_schema(), enum1_ab_schema()),
            (enum1_abc_schema(), enum1_ab_schema()),
            (empty_union_schema(), empty_union_schema()),
            (int_union_schema(), int_union_schema()),
            (int_string_union_schema(), string_int_union_schema()),
            (int_union_schema(), empty_union_schema()),
            (long_union_schema(), int_union_schema()),
            (int_union_schema(), Schema::Int),
            (Schema::Int, int_union_schema()),
            (empty_record1_schema(), empty_record1_schema()),
            (empty_record1_schema(), a_int_record1_schema()),
            (a_int_record1_schema(), a_int_record1_schema()),
            (a_dint_record1_schema(), a_int_record1_schema()),
            (a_dint_record1_schema(), a_dint_record1_schema()),
            (a_int_record1_schema(), a_dint_record1_schema()),
            (a_long_record1_schema(), a_int_record1_schema()),
            (a_int_record1_schema(), a_int_b_int_record1_schema()),
            (a_dint_record1_schema(), a_int_b_int_record1_schema()),
            (a_int_b_dint_record1_schema(), a_int_record1_schema()),
            (a_dint_b_dint_record1_schema(), empty_record1_schema()),
            (a_dint_b_dint_record1_schema(), a_int_record1_schema()),
            (a_int_b_int_record1_schema(), a_dint_b_dint_record1_schema()),
            (int_list_record_schema(), int_list_record_schema()),
            (long_list_record_schema(), long_list_record_schema()),
            (long_list_record_schema(), int_list_record_schema()),
            (nested_optional_record(), nested_record()),
        ];

        assert!(compatible_schemas
            .iter()
            .all(|(reader, writer)| SchemaCompatibility::can_read(writer, reader)));
    }

    fn writer_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"},
        {"name":"oldfield2", "type":"string"}
      ]}
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_missing_field() {
        let reader_schema = Schema::parse_str(
            r#"
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"}
      ]}
"#,
        )
        .unwrap();
        assert!(SchemaCompatibility::can_read(
            &writer_schema(),
            &reader_schema,
        ));
        assert!(!SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ));
    }

    #[test]
    fn test_missing_second_field() {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield2", "type":"string"}
        ]}
"#,
        )
        .unwrap();
        assert!(SchemaCompatibility::can_read(
            &writer_schema(),
            &reader_schema
        ));
        assert!(!SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ));
    }

    #[test]
    fn test_all_fields() {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"oldfield2", "type":"string"}
        ]}
"#,
        )
        .unwrap();
        assert!(SchemaCompatibility::can_read(
            &writer_schema(),
            &reader_schema
        ));
        assert!(SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ));
    }

    #[test]
    fn test_new_field_with_default() {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"newfield1", "type":"int", "default":42}
        ]}
"#,
        )
        .unwrap();
        assert!(SchemaCompatibility::can_read(
            &writer_schema(),
            &reader_schema
        ));
        assert!(!SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ));
    }

    #[test]
    fn test_new_field() {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"newfield1", "type":"int"}
        ]}
"#,
        )
        .unwrap();
        assert!(!SchemaCompatibility::can_read(
            &writer_schema(),
            &reader_schema
        ));
        assert!(!SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ));
    }

    #[test]
    fn test_array_writer_schema() {
        let valid_reader = string_array_schema();
        let invalid_reader = string_map_schema();

        assert!(SchemaCompatibility::can_read(
            &string_array_schema(),
            &valid_reader
        ));
        assert!(!SchemaCompatibility::can_read(
            &string_array_schema(),
            &invalid_reader
        ));
    }

    #[test]
    fn test_primitive_writer_schema() {
        let valid_reader = Schema::String;
        assert!(SchemaCompatibility::can_read(
            &Schema::String,
            &valid_reader
        ));
        assert!(!SchemaCompatibility::can_read(
            &Schema::Int,
            &Schema::String
        ));
    }

    #[test]
    fn test_union_reader_writer_subset_incompatiblity() {
        // reader union schema must contain all writer union branches
        let union_writer = union_schema(vec![Schema::Int, Schema::String]);
        let union_reader = union_schema(vec![Schema::String]);

        assert!(!SchemaCompatibility::can_read(&union_writer, &union_reader));
        assert!(SchemaCompatibility::can_read(&union_reader, &union_writer));
    }

    #[test]
    fn test_incompatible_record_field() {
        let string_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
            {"name":"field1", "type":"string"}
        ]}
        "#,
        )
        .unwrap();

        let int_schema = Schema::parse_str(
            r#"
      {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
        {"name":"field1", "type":"int"}
      ]}
"#,
        )
        .unwrap();

        assert!(!SchemaCompatibility::can_read(&string_schema, &int_schema));
    }

    #[test]
    fn test_enum_symbols() {
        let enum_schema1 = Schema::parse_str(
            r#"
      {"type":"enum", "name":"MyEnum", "symbols":["A","B"]}
"#,
        )
        .unwrap();
        let enum_schema2 =
            Schema::parse_str(r#"{"type":"enum", "name":"MyEnum", "symbols":["A","B","C"]}"#)
                .unwrap();
        assert!(!SchemaCompatibility::can_read(&enum_schema2, &enum_schema1));
        assert!(SchemaCompatibility::can_read(&enum_schema1, &enum_schema2));
    }

    // unused
    /*
        fn point_2d_schema() -> Schema {
            Schema::parse_str(
                r#"
          {"type":"record", "name":"Point2D", "fields":[
            {"name":"x", "type":"double"},
            {"name":"y", "type":"double"}
          ]}
        "#,
            )
            .unwrap()
        }
    */

    fn point_2d_fullname_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "namespace":"written", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_3d_no_default_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    // unused
    /*
        fn point_3d_schema() -> Schema {
            Schema::parse_str(
                r#"
          {"type":"record", "name":"Point3D", "fields":[
            {"name":"x", "type":"double"},
            {"name":"y", "type":"double"},
            {"name":"z", "type":"double", "default": 0.0}
          ]}
        "#,
            )
            .unwrap()
        }

        fn point_3d_match_name_schema() -> Schema {
            Schema::parse_str(
                r#"
          {"type":"record", "name":"Point", "fields":[
            {"name":"x", "type":"double"},
            {"name":"y", "type":"double"},
            {"name":"z", "type":"double", "default": 0.0}
          ]}
        "#,
            )
            .unwrap()
        }
    */

    #[test]
    fn test_union_resolution_no_structure_match() {
        // short name match, but no structure match
        let read_schema = union_schema(vec![Schema::Null, point_3d_no_default_schema()]);
        assert!(!SchemaCompatibility::can_read(
            &point_2d_fullname_schema(),
            &read_schema
        ));
    }

    // TODO(nlopes): the below require named schemas to be fully supported. See:
    // https://github.com/flavray/avro-rs/pull/76
    //
    // #[test]
    // fn test_union_resolution_first_structure_match_2d() {
    //     // multiple structure matches with no name matches
    //     let read_schema = union_schema(vec![
    //         Schema::Null,
    //         point_3d_no_default_schema(),
    //         point_2d_schema(),
    //         point_3d_schema(),
    //     ]);
    //     assert!(
    //         !SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema)
    //     );
    // }

    // #[test]
    // fn test_union_resolution_first_structure_match_3d() {
    //     // multiple structure matches with no name matches
    //     let read_schema = union_schema(vec![
    //         Schema::Null,
    //         point_3d_no_default_schema(),
    //         point_3d_schema(),
    //         point_2d_schema(),
    //     ]);
    //     assert!(
    //         !SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema)
    //     );
    // }

    // #[test]
    // fn test_union_resolution_named_structure_match() {
    //     // multiple structure matches with a short name match
    //     let read_schema = union_schema(vec![
    //         Schema::Null,
    //         point_2d_schema(),
    //         point_3d_match_name_schema(),
    //         point_3d_schema(),
    //     ]);
    //     assert!(
    //         !SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema)
    //     );
    // }

    // #[test]
    // fn test_union_resolution_full_name_match() {
    //     // there is a full name match that should be chosen
    //     let read_schema = union_schema(vec![
    //         Schema::Null,
    //         point_2d_schema(),
    //         point_3d_match_name_schema(),
    //         point_3d_schema(),
    //         point_2d_fullname_schema(),
    //     ]);
    //     assert!(SchemaCompatibility::can_read(
    //         &point_2d_fullname_schema(),
    //         &read_schema
    //     ));
    // }
}
