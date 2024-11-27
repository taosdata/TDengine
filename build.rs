use std::error::Error;

#[cfg(not(feature = "has_bytes"))]
fn no_bytes() -> Result<(), Box<dyn Error>> {
    println!("No bytes");
    Ok(())
}

#[cfg(feature = "has_bytes")]
fn has_bytes() -> Result<(), Box<dyn Error>> {
    use std::env;
    use std::fs;
    use std::path::Path;

    let out_dir = env::var("OUT_DIR").unwrap();
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("Manifest {:}", manifest_dir);
    println!("Out {:}", out_dir);
    let mut prost_build = prost_build::Config::new();
    prost_build.protoc_arg("--experimental_allow_proto3_optional");
    prost_build.compile_protos(&["./proto/job.proto"], &["./proto/"])?;

    let src = Path::new(&out_dir).join("za.co.agriio.job.rs");
    let dst = Path::new(&manifest_dir).join("src/job/job_data_prost.rs");

    fs::copy(&src, &dst).expect("Could not copy Protobuf file over");

    println!("cargo:rerun-if-changed=proto/job.proto");
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(not(feature = "has_bytes"))]
    no_bytes().unwrap();

    #[cfg(feature = "has_bytes")]
    has_bytes().unwrap();

    Ok(())
}
