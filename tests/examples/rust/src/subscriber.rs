#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[path = "utils.rs"]
mod utils;
use utils::*;
use utils::bindings::*;

use std::os::raw::{c_void, c_char, c_int, c_long};

pub struct Subscriber {
    tsub: *mut c_void,
    fields: *mut taosField,
    fcount: c_int,
}

impl Subscriber {
    pub fn new(host: &str,
               username: &str,
               passwd: &str,
               db: &str,
               table:&str,
               time: i64,
               mseconds: i32
              ) -> Result<Subscriber, &'static str> {
        unsafe {
            let mut tsub = taos_subscribe(str_into_raw(host),
                                          str_into_raw(username),
                                          str_into_raw(passwd),
                                          str_into_raw(db),
                                          str_into_raw(table),
                                          time as c_long,
                                          mseconds as c_int);
            if tsub.is_null() {
                return Err("subscribe error")
            }
            println!("subscribed to {} user:{}, db:{}, tb:{}, time:{}, mseconds:{}",
                        host, username, db, table, time, mseconds);

            let mut fields = taos_fetch_subfields(tsub);
            if fields.is_null() {
                taos_unsubscribe(tsub);
                return Err("fetch subfields error")
            }

            let fcount = taos_subfields_count(tsub);
            if fcount == 0 {
                taos_unsubscribe(tsub);
                return Err("subfields count is 0")
            }

            Ok(Subscriber{tsub, fields, fcount})
        }
    }

    pub fn consume(self: &Subscriber) -> Result<Row, &'static str> {
        unsafe {
            let taosRow = taos_consume(self.tsub);
            if taosRow.is_null() {
                return Err("consume error")
            }
            let taosRow= std::slice::from_raw_parts(taosRow, self.fcount as usize);
            let row = raw_into_row(self.fields, self.fcount, &taosRow);
            Ok(row)
        }
    }

    pub fn print_row(self: &Subscriber, row: &Row) {
        println!("{}", format_row(row));
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        unsafe {taos_unsubscribe(self.tsub);}
    }
}