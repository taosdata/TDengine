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
        r#"
use std::fmt;

use num_enum::{{FromPrimitive, IntoPrimitive}};

macro_rules! _impl_fmt {{
    ($fmt:ident) => {{
        impl fmt::$fmt for {NAME} {{
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {{
                let val = *self as i32;
                fmt::$fmt::fmt(&val, f)
            }}
        }}
    }};
}}

_impl_fmt!(Display);
_impl_fmt!(LowerHex);
_impl_fmt!(UpperHex);

/// TDengine error code.
#[derive(Debug, Clone, Copy, Eq, PartialEq, FromPrimitive, IntoPrimitive)]
#[repr(i32)]
#[derive(serde::Deserialize)]
pub enum {NAME} {{
    /// Success, 0
    Success = 0x0000,"#
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
        writeln!(output, "    {} = {},", name.to_upper_camel_case(), code)?;
    }
    writeln!(
        output,
        r#"
    #[num_enum(default)]
    Failed = 0xffff,
}}

impl {NAME} {{
    pub fn success(&self) -> bool {{
        matches!(self, {NAME}::Success)
    }}"#
    )?;

    for (name, _, reason) in &codes {
        writeln!(output, "    /// {}: {}", name, reason)?;
        writeln!(
            output,
            "    pub fn {}(&self) -> bool {{\n        matches!(self, {NAME}::{})\n    }}",
            name.to_snake_case(),
            name.to_upper_camel_case()
        )?;
    }
    writeln!(output, r#"}}"#)?;

    writeln!(
        output,
        r#"
impl {NAME} {{
    pub fn to_str(&self) -> &'static str {{
        use {NAME}::*;
        match self {{
            Success => "Success",
"#
    )?;
    for (name, _, reason) in &codes {
        writeln!(
            output,
            r#"            {} => "{reason}","#,
            name.to_upper_camel_case()
        )?;
    }
    writeln!(
        output,
        r#"            Failed => "Unknown or needn't tell detail error","#
    )?;
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
