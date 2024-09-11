fn main() {
    const ENV: &str = "ARROW_CAST_GUESSING_BOUND_YEARS";

    let years = std::env::var(ENV).unwrap_or_else(|_| "1000".to_string());

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest_path = std::path::Path::new(&out_dir).join("guessing_bound.rs");
    std::fs::write(
        &dest_path,
        format!("pub const GUESSING_BOUND_YEARS: i64 = {};", years),
    )
    .unwrap();
    println!("cargo:rerun-if-env-changed={}", ENV);
}
