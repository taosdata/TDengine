fn main() {
    println!("cargo:rustc-check-cfg=cfg(nightly)");
    match rustc_version::version_meta().unwrap().channel {
        rustc_version::Channel::Nightly => {
            println!("cargo:rustc-cfg=nightly");
        }
        rustc_version::Channel::Dev
        | rustc_version::Channel::Beta
        | rustc_version::Channel::Stable => (),
    }
}
