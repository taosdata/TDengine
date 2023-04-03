use std::path::Path;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_dir = Path::new(&out_dir);
    let readme = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("README.md");
    let cus_name = std::env::var("CUS_NAME").unwrap_or("TDengine".to_string());
    let cus_prompt = std::env::var("CUS_PROMPT").unwrap_or("taos".to_string());
    let cus_name = if cus_name.trim().is_empty() {
        "TDengine"
    } else {
        cus_name.trim()
    };
    let cus_prompt = if cus_prompt.trim().is_empty() {
        "taos"
    } else {
        cus_prompt.trim()
    };
    let content = std::fs::read_to_string(&readme)
        .unwrap()
        .replace("taos", &cus_prompt)
        .replace("TDengine", &cus_name);
    let readme_out = out_dir.join("README.md");
    std::fs::write(&readme_out, content).unwrap();
    println!("cargo:rustc-env=CUS_NAME={cus_name}");
    println!("cargo:rustc-env=CUS_PROMPT={cus_prompt}");
    println!("cargo:rustc-env=CUS_README={}", readme_out.display());
    println!("cargo:rerun-if-env-changed=CUS_NAME");
    println!("cargo:rerun-if-env-changed=CUS_PROMPT");
    println!("cargo:rerun-if-changed=README.md");
}
