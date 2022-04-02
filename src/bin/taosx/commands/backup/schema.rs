use libtaos::ColumnMeta;
use parquet::basic::{
    ConvertedType, LogicalType, Repetition, TimeUnit, TimestampType, Type as PhysicalType,
};
use parquet::schema::types::Type;
use std::sync::Arc;

use super::fetch::TableInfo;

pub fn get_database_schema() -> Arc<Type> {
    let mut fields = vec![];
    fields.push(Arc::new(
        Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("property", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap(),
    ));
    Arc::new(
        Type::group_type_builder("database")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    )
}

pub fn get_field_schema(name: &str) -> Arc<Type> {
    let mut fields = vec![];
    fields.push(Arc::new(
        Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("type", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("length", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap(),
    ));
    Arc::new(
        Type::group_type_builder(name)
            .with_fields(&mut fields)
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap(),
    )
}

pub fn get_subtable_schema() -> Arc<Type> {
    let mut fields = vec![];
    fields.push(Arc::new(
        Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("tag", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap(),
    ));
    Arc::new(
        Type::group_type_builder("sub_tables")
            .with_fields(&mut fields)
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap(),
    )
}

fn get_tableinfo_schema() -> Arc<Type> {
    let mut fields = vec![];
    fields.push(Arc::new(
        Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    ));
    fields.push(get_field_schema("cols"));
    fields.push(get_field_schema("tags"));
    // fields.push(get_subtable_schema());
    Arc::new(
        Type::group_type_builder("table_info")
            .with_fields(&mut fields)
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap(),
    )
}

pub fn get_table_schema() -> Arc<Type> {
    let mut fields = vec![];
    fields.push(get_tableinfo_schema());

    Arc::new(
        Type::group_type_builder("table")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    )
}

pub fn get_data_schema() -> Arc<Type> {
    let mut fields = vec![];
    fields.push(Arc::new(
        Type::primitive_type_builder("value", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("is_null", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    ));
    Arc::new(
        Type::group_type_builder("data")
            .with_fields(&mut fields)
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap(),
    )
}

fn get_block_schema() -> Arc<Type> {
    let mut fields = vec![];
    fields.push(Arc::new(
        Type::primitive_type_builder("tbname", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    ));
    fields.push(get_data_schema());
    Arc::new(
        Type::group_type_builder("block")
            .with_fields(&mut fields)
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap(),
    )
}

pub fn get_chunk_schema() -> Arc<Type> {
    let mut fields = vec![];
    fields.push(get_block_schema());
    Arc::new(
        Type::group_type_builder("chunk")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    )
}
