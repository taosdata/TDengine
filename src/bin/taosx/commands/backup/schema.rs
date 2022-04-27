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

    pub fn build(self) -> Arc<Type> {
        self.add_group(
            TaosParquetSchema::default()
                .add_required_byte_array_col("data")
                .build_repeated_group("table_info"),
        )
        .build_schema("table")
    }
}
