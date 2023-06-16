use std::{fs::File, io::prelude::*, path::Path};

use shadow_rs::SdResult;

fn labeling(mut file: &File) -> SdResult<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_dir = Path::new(&manifest_dir);
    let readme = manifest_dir.join("src").join("CLI.md");
    let target_dir = manifest_dir.parent().unwrap().join("target");

    let cus_name = std::env::var("CUS_NAME").unwrap_or("taosX".to_string());
    let cus_prompt = std::env::var("CUS_PROMPT").unwrap_or("taosX".to_string());
    let cus_name = if cus_name.trim().is_empty() {
        "taosX"
    } else {
        cus_name.trim()
    };
    let cus_prompt = if cus_prompt.trim().is_empty() {
        "taosX"
    } else {
        cus_prompt.trim()
    };
    let content = std::fs::read_to_string(&readme)
        .unwrap()
        .replace("taos", &cus_prompt)
        .replace("TDengine", &cus_name);

    let service_template = manifest_dir.join("src").join("systemd.service");
    let service = std::fs::read_to_string(&service_template)
        .expect(&format!("{}", service_template.display()))
        .replace("taos", &cus_prompt)
        .replace("TDengine", &cus_name);
    std::fs::write(&target_dir.join(format!("taosx-agent.service")), service).unwrap();

    writeln!(file, r#"pub const CUS_NAME: &str = "{}";"#, cus_name)?;
    writeln!(file, r#"pub const CUS_PROMPT: &str = "{}";"#, cus_prompt)?;
    writeln!(
        file,
        r#"pub const VERBOSE_VERSION: &str = if GIT_CLEAN {{
     ::const_format::concatcp!(PKG_VERSION,"-",SHORT_COMMIT," (built ",BUILD_OS," ",BUILD_TIME,")")
}} else {{
    ::const_format::concatcp!(PKG_VERSION,"-",SHORT_COMMIT,"-dirty"," (built ",BUILD_OS," ",BUILD_TIME,")")
}};"#
    )?;
    writeln!(
        file,
        r#"pub const CUS_CLI_NAME: &str = "{}x-agent";"#,
        cus_prompt
    )?;
    writeln!(file, r#"pub const CUS_CLI_ABOUT: &str = "{}";"#, content)?;

    println!("cargo:rerun-if-env-changed=PKG_TIME");

    Ok(())
}
fn main() {
    dotenv::dotenv().ok();
    shadow_rs::new_hook(labeling).unwrap();
}
