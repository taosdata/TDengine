use arrow::datatypes::Field;

use crate::stream::components::StructArrayBuilder;

pub type AttrsBuilder = StructArrayBuilder;
// pub struct AttrsBuilder(StructArrayBuilder);

// impl AttrsBuilder {
//     pub fn new(fields: Vec<Field>) -> Self {
//         Self(StructArrayBuilder::new(fields, 1))
//     }
// }
