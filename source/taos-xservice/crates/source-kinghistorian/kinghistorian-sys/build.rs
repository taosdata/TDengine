fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").expect("failed to get cargo target os");
    if target_os == "windows" {
        println!("cargo:rerun-if-changed=KRTDBAPI.h");

        let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR not set"));
        let bindings_path = out_dir.join("bindings.rs");
        let bindings = bindgen::Builder::default()
            .header("KRTDBAPI.h")
            .generate()
            .expect("Unable to generate bindings");

        bindings
            .write_to_file(&bindings_path)
            .expect("Couldn't write bindings!");
    }
}
