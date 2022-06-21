use std::fmt::Debug;

pub trait Insertable: Debug {
    type Error;
    fn insert(&self, sql: &str) -> Result<usize, Self::Error>;

    fn insert_raw(&self, table: &str, raw: &[u8]) -> Result<usize, Self::Error>;

    fn insert_named<'a>(&self, table: &str, fields: [&dyn IntoNamedField<'a>]);

    // fn insert_many<T: Any>(&self, table: &str, records: &[dyn Any]) -> Result<usize, Self::Error>;

    // fn insert_progressive(&self, stable: &str, ) -> Inserter;
}

pub trait IntoField: Debug {}

impl IntoField for i32 {}
impl IntoField for f32 {}
impl IntoField for f64 {}

pub trait IntoNamedField<'a>: IntoField {
    fn name(&'a self) -> &'a str;
}
impl<T: IntoField> IntoField for (&str, T) {}

impl<'a, T: IntoField> IntoNamedField<'a> for (&str, T) {
    fn name(&'a self) -> &'a str {
        self.0
    }
}

/// Insert macros.
///
/// insert! {
///    "ts" => ["2022-10-10T10:10:10.000", "2022-10-10T10:10:11", "2022-10-10T10:10:12"],
///    "n" => [0, 1, 2],
///    "f" => [0.1, 0.2, 0.3],
///    "g" => ["abc", "def", "desc"]: VarChar,
/// }
// macro_rules! {
//     ($conn:expr, $sql:expr, $raw: )
// }

#[test]
fn obj() {
    fn t1(_fields: &[&dyn IntoField]) {}

    let v = vec![&10 as _, &0.0 as _];
    t1(&v);

    fn insert_named<'a>(_fields: &[&dyn IntoNamedField<'a>]) {}

    let v = vec![&("a", 0) as _, &("b", 0.0) as _];
    insert_named(&v);

    for e in v {
        dbg!(e.name());
    }
}
