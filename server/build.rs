fn main() {
    let cus_name = std::env::var("CUS_NAME").unwrap_or("TDengine".to_string());
    let cus_prompt = std::env::var("CUS_PROMPT").unwrap_or("taos".to_string());
    println!("cargo:rustc-env=CUS_NAME={cus_name}");
    println!("cargo:rustc-env=CUS_PROMPT={cus_prompt}");
		println!("cargo:rerun-if-env-changed=CUS_NAME");
		println!("cargo:rerun-if-env-changed=CUS_PROMPT");
}
