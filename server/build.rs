use std::path::Path;

const DEFAULT_CUS_NAME: &str = "TDengine";
const DEFAULT_CUS_PROMPT: &str = "taos";
const DEFAULT_CUS_CONFIG: &str = "";
const DEFAULT_TD_VERSION: &str = "3.0.2.0";

fn main() -> shadow_rs::SdResult<()> {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_dir = Path::new(&out_dir);
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_dir = Path::new(&manifest_dir);
    let readme = manifest_dir.join("README.md");
    let target_dir = manifest_dir.join("..").join("target");

    let cus_name = std::env::var("CUS_NAME").unwrap_or(DEFAULT_CUS_NAME.to_string());
    let cus_prompt = std::env::var("CUS_PROMPT").unwrap_or(DEFAULT_CUS_PROMPT.to_string());
    let cus_config = std::env::var("CUS_CONFIG").unwrap_or(DEFAULT_CUS_CONFIG.to_string());
    let td_version = std::env::var("VER_NUMBER").unwrap_or(DEFAULT_TD_VERSION.to_string());
    let cus_name = if cus_name.trim().is_empty() {
        DEFAULT_CUS_NAME
    } else {
        cus_name.trim()
    };
    let cus_prompt = if cus_prompt.trim().is_empty() {
        DEFAULT_CUS_PROMPT
    } else {
        cus_prompt.trim()
    };
    let cus_config = if cus_config.trim().is_empty() {
        DEFAULT_CUS_CONFIG
    } else {
        cus_config.trim()
    };
    let td_version = if td_version.trim().is_empty() {
        DEFAULT_TD_VERSION
    } else {
        td_version.trim()
    };
    let content = std::fs::read_to_string(readme)
        .unwrap()
        .replace(DEFAULT_CUS_PROMPT, cus_prompt)
        .replace(DEFAULT_CUS_NAME, cus_name)
        .replace(DEFAULT_CUS_CONFIG, cus_config);
    let readme_out = out_dir.join("README.md");
    std::fs::write(&readme_out, content).unwrap();

    let service = std::fs::read_to_string(manifest_dir.join("examples").join("explorer.service"))
        .unwrap()
        .replace(DEFAULT_CUS_PROMPT, cus_prompt)
        .replace(DEFAULT_CUS_NAME, cus_name)
        .replace(DEFAULT_CUS_CONFIG, cus_config);
    let service_path = target_dir.join(format!("{cus_prompt}-explorer.service"));
    std::fs::write(&service_path, service).unwrap();
    println!("cargo:rustc-env=CUS_NAME={cus_name}");
    println!("cargo:rustc-env=CUS_PROMPT={cus_prompt}");
    println!("cargo:rustc-env=CUS_CONFIG={cus_config}");
    println!("cargo:rustc-env=CUS_CLI_NAME={cus_prompt}-explorer");
    println!("cargo:rustc-env=CUS_README={}", readme_out.display());
    println!("cargo:rustc-env=TD_VERSION={td_version}");
    println!("cargo:rerun-if-env-changed=CUS_NAME");
    println!("cargo:rerun-if-env-changed=CUS_PROMPT");
    println!("cargo:rerun-if-env-changed=CUS_CONFIG");
    println!("cargo:rerun-if-changed=README.md");
    println!("cargo:rerun-if-changed=../dist/");
    println!("cargo:rerun-if-changed=examples/explorer.service");
    println!("cargo:rerun-if-changed={}", service_path.display());
    shadow_rs::new()
}
