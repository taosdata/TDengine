use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::{error::Error, io::BufReader};

use heck::*;
use regex::Regex;

fn code_from_header(
    header: impl AsRef<Path>,
    mut output: impl Write,
) -> Result<(), Box<dyn Error>> {
    let regex = Regex::new(
        r"#define TSDB_CODE_(?P<name>\S+)\s+TAOS_DEF_ERROR_CODE\(0, (?P<code>\S+)\)\s+//.(?P<reason>.*).\)",
    )?;

    let file = File::open(header)?;
    let buf = BufReader::new(file);
    const NAME: &str = "Code";
    writeln!(
        output,
        r#"use super::Code;
impl {NAME} {{
    /// Success, 0
    pub const Success: {NAME} = {NAME}(0x0000);
    /// Unknown fails, 0xFFFF
    pub const Failed: {NAME} = {NAME}(0xFFFF);"#
    )?;
    let codes: Vec<_> = buf
        .lines()
        .flat_map(Result::ok)
        .flat_map(|line| {
            regex.captures(&line).map(|caps| {
                (
                    caps.name("name").unwrap().as_str().to_string(),
                    caps.name("code").unwrap().as_str().to_string(),
                    caps.name("reason").unwrap().as_str().to_string(),
                )
            })
        })
        .collect();
    for (name, code, reason) in &codes {
        writeln!(output, "    /// {}: {}", name, reason)?;
        writeln!(
            output,
            "    pub const {}: {NAME} = {NAME}({});",
            name.to_upper_camel_case(),
            code
        )?;
    }
    writeln!(
        output,
        r#"
}}

impl {NAME} {{
    pub fn success(&self) -> bool {{
        *self == {NAME}::Success
    }}"#
    )?;

    for (name, _, reason) in &codes {
        writeln!(output, "    /// {}: {}", name, reason)?;
        writeln!(
            output,
            "    pub fn {}(&self) -> bool {{\n        *self == {NAME}::{}\n    }}",
            name.to_snake_case(),
            name.to_upper_camel_case()
        )?;
    }
    writeln!(output, r#"}}"#)?;

    writeln!(
        output,
        r#"
impl {NAME} {{
    pub fn as_error_str(&self) -> &'static str {{
        match self {{
            &Code::Success => "Success",
"#
    )?;
    for (name, _, reason) in &codes {
        writeln!(
            output,
            r#"            &Code::{} => "{reason}","#,
            name.to_upper_camel_case()
        )?;
    }
    writeln!(output, r#"            _ => "Unknown or common error","#)?;
    writeln!(
        output,
        r#"        }}
    }}
}}
"#
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let header = "src/taoserror.h";
    println!("cargo:rerun-if-changed={header}");
    println!("cargo:rerun-if-changed=build.rs");

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let rs = File::create(Path::new(&out_dir).join("code.rs"))?;
    code_from_header(header, rs)?;
    Ok(())
}
