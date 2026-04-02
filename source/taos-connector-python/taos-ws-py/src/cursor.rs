use chrono_tz::Tz;
use pyo3::{
    prelude::*,
    types::{PyDict, PySequence, PyString, PyTuple},
};
use taos::{
    sync::{Fetchable, Queryable},
    Itertools, RawBlock, ResultSet, Taos,
};

use crate::{
    common::{get_all_of_block, get_row_of_block, get_slice_of_block},
    ConnectionError, FetchError, NotSupportedError, OperationalError,
};

#[pyclass]
pub(crate) struct Cursor {
    inner: Option<Taos>,
    row_count: usize,
    result_set: Option<ResultSet>,
    block: Option<RawBlock>,
    row_in_block: usize,
    tz: Option<Tz>,
}

impl Cursor {
    pub fn new(taos: Taos, tz: Option<Tz>) -> Self {
        Self {
            inner: Some(taos),
            row_count: 0,
            result_set: None,
            block: None,
            row_in_block: 0,
            tz,
        }
    }

    fn inner(&self) -> PyResult<&Taos> {
        Ok(self
            .inner
            .as_ref()
            .ok_or_else(|| ConnectionError::new_err("Cursor was already closed"))?)
    }

    fn current_result_set(&mut self) -> PyResult<&mut ResultSet> {
        Ok(self
            .result_set
            .as_mut()
            .ok_or_else(|| ConnectionError::new_err("Cursor was already closed"))?)
    }

    fn assert_block(&mut self) -> PyResult<()> {
        let need_fetch = match self.block.as_ref() {
            Some(block) => self.row_in_block >= block.nrows(),
            None => true,
        };
        if need_fetch {
            self.row_in_block = 0;
            self.block = self
                .current_result_set()?
                .fetch_raw_block()
                .map_err(|err| FetchError::new_err(err.to_string()))?
                .map(|b| b.with_timezone(self.tz));
        }
        Ok(())
    }
}

#[pymethods]
impl Cursor {
    /// This read-only attribute is a sequence of 7-item sequences.
    ///
    /// Each of these sequences contains information describing one result column:
    ///
    /// - name
    /// - type_code
    /// - display_size
    /// - internal_size
    /// - precision
    /// - scale
    /// - null_ok
    ///
    /// The first two items (name and type_code) are mandatory, the other five are optional and are set to None if no meaningful values can be provided.
    ///
    /// This attribute will be None for operations that do not return rows or if the cursor has not had an operation invoked via the .execute*() method yet.
    ///
    /// The type_code can be interpreted by comparing it to the Type Objects specified in the section below.
    #[getter]
    pub fn description(&mut self) -> PyResult<Vec<PyObject>> {
        Python::with_gil(|py| {
            Ok(self
                .current_result_set()?
                .fields()
                .iter()
                .map(|field| {
                    PyTuple::new(
                        py,
                        [
                            field.name().to_object(py),
                            (field.ty() as u8).to_object(py),
                            field.bytes().to_object(py),
                        ],
                    )
                    .to_object(py)
                })
                .collect())
        })
    }

    /// This read-only attribute specifies the number of rows that the last .execute*() produced
    ///  (for DQL statements like SELECT) or affected (for DML statements like UPDATE or INSERT).
    #[getter]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// PEP249 void method
    pub fn call_proc(&self) -> PyResult<()> {
        Err(NotSupportedError::new_err(
            "Cursor.call_proc() method is not supported",
        ))
    }

    /// Close the cursor now (rather than whenever `__del__` is called).
    pub fn close(&mut self) {
        self.inner.take();
    }

    #[args(py_args = "*", parameters = "**")]
    pub fn execute(
        &mut self,
        operation: &PyString,
        py_args: &PyTuple,
        parameters: Option<&PyDict>,
    ) -> PyResult<usize> {
        let sql = Python::with_gil(|py| {
            let sql: String = if let Some(parameters) = parameters {
                let local = PyDict::new(py);
                local.set_item("parameters", parameters)?;
                local.set_item("operation", operation)?;
                local.set_item("args", py_args)?;
                let sql = py.eval("operation.format(*args, **parameters)", None, Some(local))?;
                sql.extract()?
            } else {
                let local = PyDict::new(py);
                local.set_item("operation", operation)?;
                local.set_item("args", py_args)?;
                let sql = py.eval("operation.format(*args)", None, Some(local))?;
                sql.extract()?
            };
            Ok::<_, PyErr>(sql)
        })?;
        let result_set = self
            .inner()?
            .query(sql)
            .map_err(|err| OperationalError::new_err(err.to_string()))?;
        let affected_rows = result_set.affected_rows();
        self.result_set.replace(result_set);
        self.row_count = affected_rows as _;
        Ok(affected_rows as _)
    }

    #[args(py_args = "*", parameters = "**")]
    pub fn execute_with_req_id(
        &mut self,
        operation: &PyString,
        py_args: &PyTuple,
        parameters: Option<&PyDict>,
        req_id: u64,
    ) -> PyResult<usize> {
        let sql = Python::with_gil(|py| {
            let sql: String = if let Some(parameters) = parameters {
                let local = PyDict::new(py);
                local.set_item("parameters", parameters)?;
                local.set_item("operation", operation)?;
                local.set_item("args", py_args)?;
                let sql = py.eval("operation.format(*args, **parameters)", None, Some(local))?;
                sql.extract()?
            } else {
                let local = PyDict::new(py);
                local.set_item("operation", operation)?;
                local.set_item("args", py_args)?;
                let sql = py.eval("operation.format(*args)", None, Some(local))?;
                sql.extract()?
            };
            Ok::<_, PyErr>(sql)
        })?;
        let result_set = self
            .inner()?
            .query_with_req_id(sql, req_id)
            .map_err(|err| OperationalError::new_err(err.to_string()))?;
        let affected_rows = result_set.affected_rows();
        self.result_set.replace(result_set);
        self.row_count = affected_rows as _;
        Ok(affected_rows as _)
    }

    #[args(py_args = "*", parameters = "**")]
    pub fn execute_many(
        &mut self,
        operation: &PyString,
        seq_of_parameters: &PySequence,
    ) -> PyResult<usize> {
        let sql = Python::with_gil(|py| {
            let vec: Vec<_> = seq_of_parameters
                .iter()?
                .map(|row| -> PyResult<String> {
                    let row = row?;
                    if row.is_instance_of::<PyDict>()? {
                        let local = PyDict::new(py);
                        local.set_item("args", row)?;
                        local.set_item("operation", operation)?;
                        let sql = py.eval("operation.format(**args)", None, Some(local))?;
                        sql.extract()
                    } else {
                        let local = PyDict::new(py);
                        local.set_item("args", row)?;
                        local.set_item("operation", operation)?;
                        let sql = py.eval("operation.format(*args)", None, Some(local))?;
                        sql.extract()
                    }
                })
                .try_collect()?;
            Ok::<_, PyErr>(vec)
        })?;
        let affected_rows = self
            .inner()?
            .exec_many(sql)
            .map_err(|err| OperationalError::new_err(err.to_string()))?;
        self.row_count = affected_rows;
        Ok(affected_rows)
    }

    #[args(py_args = "*", parameters = "**")]
    pub fn execute_many_with_req_id(
        &mut self,
        operation: &PyString,
        seq_of_parameters: &PySequence,
        req_id: u64,
    ) -> PyResult<usize> {
        let sql = Python::with_gil(|py| {
            let vec: Vec<_> = seq_of_parameters
                .iter()?
                .map(|row| -> PyResult<String> {
                    let row = row?;
                    if row.is_instance_of::<PyDict>()? {
                        let local = PyDict::new(py);
                        local.set_item("args", row)?;
                        local.set_item("operation", operation)?;
                        let sql = py.eval("operation.format(**args)", None, Some(local))?;
                        sql.extract()
                    } else {
                        let local = PyDict::new(py);
                        local.set_item("args", row)?;
                        local.set_item("operation", operation)?;
                        let sql = py.eval("operation.format(*args)", None, Some(local))?;
                        sql.extract()
                    }
                })
                .try_collect()?;
            Ok::<_, PyErr>(vec)
        })?;
        let affected_rows = sql
            .into_iter()
            .map(|sql| {
                self.inner()?
                    .query_with_req_id(sql, req_id)
                    .map_err(|err| OperationalError::new_err(err.to_string()))
            })
            .try_fold(0, |mut acc, aff| {
                acc += aff?.affected_rows() as usize;
                Ok::<usize, PyErr>(acc)
            })?;
        self.row_count = affected_rows;
        Ok(affected_rows)
    }

    /// PEP249 void method
    pub fn fetchone(&mut self) -> PyResult<Option<PyObject>> {
        self.assert_block()?;

        Ok(Python::with_gil(|py| -> Option<PyObject> {
            if let Some(block) = self.block.as_ref() {
                let row = get_row_of_block(py, block, self.row_in_block).unwrap();
                self.row_in_block += 1;
                return Some(row);
            }
            None
        }))
    }

    /// PEP249 void method
    pub fn fetchmany(&mut self, size: Option<usize>) -> PyResult<Option<Vec<PyObject>>> {
        self.assert_block()?;

        if let Some(size) = size {
            Python::with_gil(|py| {
                let mut range = self.row_in_block..self.row_in_block + size;
                let mut all = Vec::new();
                loop {
                    if let Some(block) = self.block.take() {
                        let (slice, remain) = get_slice_of_block(py, &block, range.clone());
                        all.extend(slice);
                        if remain.is_none() {
                            self.block = Some(block);
                            self.row_in_block = range.end;
                            self.row_count += range.end - range.start;
                            break;
                        } else {
                            let remain = remain.unwrap();
                            self.row_in_block = block.nrows();
                            self.row_count += block.nrows() - range.start;
                            self.assert_block()?;
                            range = 0..remain;
                        }
                    } else {
                        break;
                    }
                }
                Ok(Some(all))
            })
        } else {
            self.row_in_block = 0;
            if let Some(block) = self.block.take() {
                self.row_count += block.nrows();
                return Ok(Some(Python::with_gil(|py| get_all_of_block(py, &block))));
            } else {
                return Ok(None);
            }
        }
    }

    /// Fetch all rows into a sequence of tuple.
    pub fn fetchall(&mut self) -> PyResult<Option<Vec<PyObject>>> {
        self.fetchmany(Some(usize::MAX))
    }

    /// Fetch all rows in the current result set into a sequence of dict.
    ///
    /// Just an alias of `fetch_all_into_dict()`.
    pub fn fetchallintodict(&mut self) -> PyResult<Option<Vec<PyObject>>> {
        if let Some(all) = self.fetchall()? {
            let names: Vec<_> = self
                .current_result_set()?
                .fields()
                .iter()
                .map(|f| f.name())
                .collect();
            let list = all
                .into_iter()
                .map(|tuple| {
                    Python::with_gil(|py| -> PyResult<_> {
                        let tuple: Vec<PyObject> = tuple.extract(py)?;
                        let dict = PyDict::new(py);
                        for (key, value) in names.iter().zip(tuple) {
                            dict.set_item(key, value)?;
                        }
                        Ok(dict.to_object(py))
                    })
                })
                .try_collect()?;
            Ok(Some(list))
        } else {
            Ok(None)
        }
    }

    /// Fetch all rows in the current result set into a sequence of dict.
    pub fn fetch_all_into_dict(&mut self) -> PyResult<Option<Vec<PyObject>>> {
        self.fetchallintodict()
    }

    /// PEP249 void method, underline interface does not support multiple result sets.
    pub fn nextset(&self) -> PyResult<()> {
        Err(NotSupportedError::new_err(
            "Cursor.nextset() method is not supported, because it does not support multiple result sets",
        ))
    }

    /// Returns none by default.
    #[getter]
    pub fn arraysize(&self) -> Option<usize> {
        None
    }

    /// PEP249 void method
    #[getter]
    pub fn setinputsizes(&self) {}

    /// PEP249 void method
    #[getter]
    pub fn setoutputsizes(&self) {}
}
