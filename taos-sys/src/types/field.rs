use std::ffi::CStr;

use super::TaosDataType;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TAOS_FIELD {
    pub name: [u8; 65usize],
    pub type_: u8,
    pub bytes: i16,
}

impl TAOS_FIELD {
    pub fn name(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.name.as_ptr() as _) }
        // CStr::from_bytes_with_nul(&self.name).expect("field name should always be valid C-str")
    }
    pub fn type_(&self) -> TaosDataType {
        self.type_.into()
    }

    pub fn bytes(&self) -> u32 {
        self.bytes as _
    }
}

#[cfg(feature = "serde")]
impl<'de, 'a> serde::de::Deserializer<'de> for &'a TAOS_FIELD {
    type Error = taos_error::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.name()
            .to_str()
            .map_err(|err| taos_error::Error::from_string(format!("{}", err)))
            .and_then(|s| visitor.visit_str(s))
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}
#[cfg(feature = "serde")]
impl<'de, 'a> serde::de::IntoDeserializer<'de, taos_error::Error> for &'a TAOS_FIELD {
    type Deserializer = &'a TAOS_FIELD;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}
