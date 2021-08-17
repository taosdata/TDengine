extern crate odbc;
// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
extern crate env_logger;
use odbc::*;
use odbc_safe::AutocommitOn;
use std::env;

fn main() {

    env_logger::init();

    let conn_str = env::var("DSN").unwrap();
    match connect(&conn_str) {
        Ok(()) => println!("Success"),
        Err(diag) => println!("Error: {}", diag),
    }
}

fn connect(conn_str: &str) -> std::result::Result<(), DiagnosticRecord> {

    let env = create_environment_v3().map_err(|e| e.unwrap())?;

    let conn = env.connect_with_connection_string(conn_str)?;
    execute_statement(&conn)
}

fn execute_statement<'env>(conn: &Connection<'env, AutocommitOn>) -> Result<()> {
    let stmt = Statement::with_parent(conn)?;

    match stmt.exec_direct("select * from m.t")? {
        Data(mut stmt) => {
            let cols = stmt.num_result_cols()?;
            println!("cols: {}", cols);
            while let Some(mut cursor) = stmt.fetch()? {
                for i in 1..(cols + 1) {
                    match cursor.get_data::<&str>(i as u16)? {
                        Some(val) => print!(" {}", val),
                        None => print!(" NULL"),
                    }
                }
                println!("");
            }
        }
        NoData(_) => println!("Query executed, no data returned"),
    }

    Ok(())
}

