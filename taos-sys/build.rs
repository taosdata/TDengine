use std::env;
use std::ffi::OsString;
use std::fmt::Display;

fn get_env(name: &str) -> Option<OsString> {
    let var = env::var_os(name);
    println!("cargo:rerun-if-env-changed={}", name);

    match var {
        Some(ref v) => println!("{} = {}", name, v.to_string_lossy()),
        None => println!("{} unset", name),
    }

    var
}

fn version2features<V: Into<Version>>(version: V) -> Vec<&'static str> {
    let version = version.into();
    let mut feats = Vec::new();
    if version.mainline == 3 {
        feats.push("v3");
        feats.push("tmq");
        feats.push("fetch_raw_block");
        feats.push("fetch_block_s");
    } else if version.mainline == 2 {
        feats.push("v2");
        if version >= Version::new(2, 4, 0, 4) {
            feats.push("result_block");
        }
        if version >= Version::new(2, 4, 0, 0) {
            feats.push("json_tag");
            feats.push("set_config");
            feats.push("is_update_query");
            feats.push("reset_db");
            feats.push("sml");
            feats.push("parse_time");
        }
    } else {
        panic!("unsupported TDengine client version {version}")
    }
    feats
}
fn taos_version() -> String {
    let lib_env = "TAOS_LIBRARY_PATH";
    if let Some(path) = get_env(lib_env) {
        println!("cargo:rustc-link-search={}", path.to_string_lossy());
    }
    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-search=C:\\TDengine\\driver");
    }
    let lib_name = if cfg!(target_os = "windows") {
        "taos.dll"
    } else if cfg!(target_os = "linux") {
        "libtaos.so"
    } else if cfg!(target_os = "darwin") {
        "libtaos.dylib"
    } else {
        unreachable!("the current os is not supported");
    };
    let lib = unsafe { libloading::Library::new(lib_name).unwrap() };
    let version = unsafe {
        let version: libloading::Symbol<unsafe extern "C" fn() -> *const std::os::raw::c_char> =
            lib.get(b"taos_get_client_info\0").unwrap();
        std::ffi::CStr::from_ptr(version()).to_string_lossy()
    };
    println!("cargo:rustc-cfg=taos_version=\"v{}\"", version);
    let parsed_version = Version::parse(&version).expect("invalid version of taos");
    for feat in version2features(parsed_version) {
        println!("cargo:rustc-cfg=taos_{feat}");
    }
    return version.to_string();
}

#[derive(Debug, PartialEq, PartialOrd)]
struct Version {
    mainline: u8,
    major: u8,
    minor: u8,
    patch: u8,
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Version {
            mainline,
            major,
            minor,
            patch,
        } = self;
        f.write_fmt(format_args!("{mainline}.{major}.{minor}.{patch}"))
    }
}

impl Version {
    fn new(mainline: u8, major: u8, minor: u8, patch: u8) -> Self {
        Self {
            mainline,
            major,
            minor,
            patch,
        }
    }
    fn parse(version: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let version_items: Vec<_> = version.split('.').collect();
        let items = version_items.len();
        if items == 0 || items > 4 {
            Err("parse version error: {version}")?
        }

        let mainline = version_items[0].parse()?;
        let major = version_items
            .get(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();
        let minor = version_items
            .get(2)
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();
        let patch = version_items
            .get(3)
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();

        Ok(Self::new(mainline, major, minor, patch))
    }
}

fn main() {
    taos_version();
    println!("cargo:rustc-link-lib=taos");
}
