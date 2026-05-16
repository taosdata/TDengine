use pyo3::prelude::*;

use ::taos::Field;

#[pyclass]
pub(crate) struct TaosField {
    _inner: Field,
}

impl From<Field> for TaosField {
    fn from(value: Field) -> Self {
        Self { _inner: value }
    }
}

impl From<&Field> for TaosField {
    fn from(value: &Field) -> Self {
        Self {
            _inner: value.clone(),
        }
    }
}

impl TaosField {
    #[allow(dead_code)]
    fn new(inner: &Field) -> Self {
        Self {
            _inner: inner.clone(),
        }
    }
}

#[pymethods]
impl TaosField {
    /// Field name.
    fn name(&self) -> &str {
        self._inner.name()
    }

    /// Field type name
    fn r#type(&self) -> &str {
        self._inner.ty().name()
    }

    /// Declaration max-bytes in field.
    fn bytes(&self) -> u32 {
        self._inner.bytes()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "{{name: {}, type: {}, bytes: {}}}",
            self.name(),
            self.r#type(),
            self.bytes()
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(format!(
            "{{name: {}, type: {}, bytes: {}}}",
            self.name(),
            self.r#type(),
            self.bytes()
        ))
    }
}
