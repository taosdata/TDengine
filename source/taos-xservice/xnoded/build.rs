fn main() {
    shadow_rs::ShadowBuilder::builder()
        .hook(labeling)
        .build()
        .unwrap();
}

fn labeling(mut file: &std::fs::File) -> shadow_rs::SdResult<()> {
    use std::io::Write;

    let clippy_allow: &str =
        r"#[allow(clippy::all, clippy::pedantic, clippy::restriction, clippy::nursery)]";
    writeln!(file, "{clippy_allow}")?;

    let td_version = std::env::var("VER_NUMBER").ok();
    if let Some(version) = td_version {
        writeln!(file, r#"pub const TD_VERSION: &str = "{}";"#, version)?;
    } else {
        writeln!(file, r#"pub const TD_VERSION: &str = PKG_VERSION;"#)?;
    }
    Ok(())
}
