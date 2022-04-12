use parquet::basic::{Repetition, Type as PhysicalType};
use parquet::schema::types::Type;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct TaosParquetSchema {
    fields: Vec<Arc<Type>>,
}

impl TaosParquetSchema {
    fn add_required_byte_array_col(mut self, name: &str) -> Self {
        self.fields.push(Arc::new(
            Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ));
        self
    }

    fn add_required_int_col(mut self, name: &str) -> Self {
        self.fields.push(Arc::new(
            Type::primitive_type_builder(name, PhysicalType::INT32)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ));
        self
    }

    fn add_required_bool_col(mut self, name: &str) -> Self {
        self.fields.push(Arc::new(
            Type::primitive_type_builder(name, PhysicalType::BOOLEAN)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ));
        self
    }

    fn add_group(mut self, group: Arc<Type>) -> Self {
        self.fields.push(group);
        self
    }

    fn build_repeated_group(mut self, name: &str) -> Arc<Type> {
        Arc::new(
            Type::group_type_builder(name)
                .with_fields(&mut self.fields)
                .with_repetition(Repetition::REPEATED)
                .build()
                .unwrap(),
        )
    }

    fn build_schema(mut self, name: &str) -> Arc<Type> {
        Arc::new(
            Type::group_type_builder(name)
                .with_fields(&mut self.fields)
                .build()
                .unwrap(),
        )
    }

    pub fn build_table_schema(self) -> Arc<Type> {
        self.add_group(
            TaosParquetSchema::default()
                .add_required_byte_array_col("name")
                .add_group(
                    TaosParquetSchema::default()
                        .add_required_byte_array_col("name")
                        .add_required_int_col("type")
                        .add_required_int_col("length")
                        .add_required_bool_col("is_tag")
                        .build_repeated_group("meta"),
                )
                .build_repeated_group("table_info"),
        )
        .build_schema("table")
    }

    pub fn build_tag_schema(self) -> Arc<Type> {
        self.add_group(
            TaosParquetSchema::default()
                .add_required_byte_array_col("stbname")
                .add_group(
                    TaosParquetSchema::default()
                        .add_group(
                            TaosParquetSchema::default()
                                .add_required_byte_array_col("value")
                                .add_required_bool_col("is_nulls")
                                .build_repeated_group("data"),
                        )
                        .build_repeated_group("subtable"),
                )
                .build_repeated_group("stable"),
        )
        .build_schema("tags")
    }

    pub fn build_chunk_schema(self) -> Arc<Type> {
        self.add_group(
            TaosParquetSchema::default()
                .add_required_byte_array_col("name")
                .add_group(
                    TaosParquetSchema::default()
                        .add_required_byte_array_col("value")
                        .add_required_byte_array_col("is_nulls")
                        .build_repeated_group("data"),
                )
                .build_repeated_group("block"),
        )
        .build_schema("chunk")
    }
}
