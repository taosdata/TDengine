fn main() {
    println!("cargo:rerun-if-env-changed=GIT_COMMIT_ID");

    shadow_rs::new().unwrap();

    let commit_id = std::env::var("GIT_COMMIT_ID").unwrap_or_else(|_| {
        std::process::Command::new("git")
            .args(["rev-parse", "--short=7", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "ncid000".to_string())
    });
    println!("cargo:rustc-env=GIT_COMMIT_ID={commit_id}");
}
