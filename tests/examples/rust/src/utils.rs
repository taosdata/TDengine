#[path = "bindings.rs"]
pub mod bindings;
use bindings::*;

use std::fmt;
use std::fmt::Display;
use std::os::raw::{c_void, c_char, c_int};
use std::ffi::{CString, CStr};

// #[derive(Debug)]
pub enum Field {
    tinyInt(i8),
    smallInt(i16),
    normalInt(i32),
    bigInt(i64),
    float(f32),
    double(f64),
    binary(String),
    timeStamp(i64),
    boolType(bool),
}


impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            Field::tinyInt(v) => write!(f, "{}", v),
            Field::smallInt(v) => write!(f, "{}", v),
            Field::normalInt(v) => write!(f, "{}", v),
            Field::bigInt(v) => write!(f, "{}", v),
            Field::float(v) => write!(f, "{}", v),
            Field::double(v) => write!(f, "{}", v),
            Field::binary(v) => write!(f, "{}", v),
            Field::tinyInt(v) => write!(f, "{}", v),
            Field::timeStamp(v) => write!(f, "{}", v),
            Field::boolType(v) => write!(f, "{}", v),
        }
    }
}

// pub type Fields = Vec<Field>;
pub type Row = Vec<Field>;

pub fn format_row(row: &Row) -> String {
    let mut s = String::new();
    for field in row {
        s.push_str(format!("{} ", field).as_str());
        // println!("{}", field);
    }
    s
}

pub fn str_into_raw(s: &str) -> *mut c_char {
    if s.is_empty() {
        0 as *mut c_char
    } else {
        CString::new(s).unwrap().into_raw()
    }
}

pub fn raw_into_str<'a>(raw: *mut c_char) -> &'static str {
    unsafe {CStr::from_ptr(raw).to_str().unwrap()}
}


pub fn raw_into_field(raw: *mut TAOS_FIELD, fcount: c_int) -> Vec<taosField> {
    let mut fields: Vec<taosField> = Vec::new();

    for i in 0..fcount as isize {
        fields.push(
            taosField {
                name: unsafe {(*raw.offset(i as isize))}.name,
                bytes: unsafe {(*raw.offset(i as isize))}.bytes,
                type_: unsafe {(*raw.offset(i as isize))}.type_,
            }
        );
    }

    /// TODO: error[E0382]: use of moved value: `fields`
    // for field in &fields {
    //     println!("type: {}, bytes: {}", field.type_, field.bytes);
    // }

    fields
}

   pub fn raw_into_row(fields: *mut TAOS_FIELD, fcount: c_int, raw_row: &[*mut c_void]) -> Row {
        let mut row: Row= Vec::new();
        let fields = raw_into_field(fields, fcount);

        for (i, field) in fields.iter().enumerate() {
            // println!("index: {}, type: {}, bytes: {}", i, field.type_, field.bytes);
            unsafe {
                match field.type_ as u32 {
                    TSDB_DATA_TYPE_TINYINT => {
                        row.push(Field::tinyInt(*(raw_row[i] as *mut i8)));
                    }
                    TSDB_DATA_TYPE_SMALLINT => {
                        row.push(Field::smallInt(*(raw_row[i] as *mut i16)));
                    }
                    TSDB_DATA_TYPE_INT => {
                        row.push(Field::normalInt(*(raw_row[i] as *mut i32)));
                    }
                    TSDB_DATA_TYPE_BIGINT => {
                        row.push(Field::bigInt(*(raw_row[i] as *mut i64)));
                    }
                    TSDB_DATA_TYPE_FLOAT => {
                        row.push(Field::float(*(raw_row[i] as *mut f32)));
                    }
                    TSDB_DATA_TYPE_DOUBLE => {
                        row.push(Field::double(*(raw_row[i] as *mut f64)));
                    }
                    TSDB_DATA_TYPE_BINARY | TSDB_DATA_TYPE_NCHAR => {
                        // row.push(Field::binary(*(raw_row[i] as *mut f64)));
                    }
                    TSDB_DATA_TYPE_TIMESTAMP => {
                        row.push(Field::timeStamp(*(raw_row[i] as *mut i64)));
                    }
                    TSDB_DATA_TYPE_BOOL => {
                        // row.push(Field::boolType(*(raw_row[i] as *mut i8) as bool));
                    }
                    _ => println!(""),
                }
            }
        }
        row
    }