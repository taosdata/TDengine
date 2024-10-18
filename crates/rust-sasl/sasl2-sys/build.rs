// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
#[cfg(unix)]
use std::ffi::OsString;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use duct::cmd;

#[cfg(unix)]
const LIBRARY_NAME: &str = "sasl2";

#[cfg(windows)]
const LIBRARY_NAME: &str = "libsasl";

#[derive(Debug, Clone)]
struct Metadata {
    host: String,
    target: String,
    want_static: Option<bool>,
    out_dir: PathBuf,
}

fn main() {
    println!("cargo:rerun-if-env-changed=SASL2_STATIC");

    let metadata = Metadata {
        host: env::var("HOST").unwrap(),
        target: env::var("TARGET").unwrap(),
        want_static: env::var_os("SASL2_STATIC").map(|v| v != "0"),
        out_dir: env::var("OUT_DIR").unwrap().into(),
    };

    if cfg!(feature = "vendored") {
        build_sasl(&metadata)
    } else {
        find_sasl(&metadata)
    };
}

#[cfg(unix)]
fn build_sasl(metadata: &Metadata) {
    let src_dir = metadata.out_dir.join("sasl2");
    if !src_dir.exists() {
        // We're not allowed to build in-tree directly, as ~/.cargo/registry is
        // globally shared, but sasl doesn't seem to support out-of-tree builds.
        // Work around the issue by copying sasl into OUT_DIR, and building
        // inside of *that* tree.
        cmd!("cp", "-R", "sasl2", &src_dir)
            .run()
            .expect("failed making copy of sasl2 tree");
    }

    let install_dir = metadata.out_dir.join("install");

    let mut cppflags = env::var("CPPFLAGS").ok().unwrap_or_else(String::new);
    let mut cflags = env::var("CFLAGS").ok().unwrap_or_else(String::new);

    // If OpenSSL has been vendored, point libsasl2 at the vendored headers.
    if cfg!(feature = "openssl-sys") {
        if let Ok(openssl_root) = env::var("DEP_OPENSSL_ROOT") {
            cppflags += &format!(" -I{}", Path::new(&openssl_root).join("include").display());
        }
    }

    // `--with-pic` only applies to libraries built with libtool, and when
    // linking statically the sasl2 build system subverts libtool to almagamate
    // plugins into the main library archive, so we need to request PIC in
    // CFLAGS too.
    cflags += " -fPIC";

    let mut configure_args = vec![
        format!("--prefix={}", install_dir.display()),
        "--enable-static".into(),
        "--disable-shared".into(),
        "--disable-sample".into(),
        "--disable-checkapop".into(),
        "--disable-cram".into(),
        "--disable-scram".into(),
        "--disable-digest".into(),
        "--disable-otp".into(),
        if cfg!(feature = "gssapi-vendored") {
            format!("--enable-gssapi={}", env::var("DEP_KRB5_SRC_ROOT").unwrap())
        } else {
            "--disable-gssapi".into()
        },
        "--with-gss_impl=mit".into(),
        if cfg!(feature = "plain") {
            "--enable-plain".into()
        } else {
            "--disable-plain".into()
        },
        if cfg!(feature = "scram") {
            "--enable-scram".into()
        } else {
            "--disable-scram".into()
        },
        "--disable-anon".into(),
        "--with-dblib=none".into(),
        "--with-pic".into(),
        format!("CPPFLAGS={}", cppflags),
        format!("CFLAGS={}", cflags),
    ];
    if metadata.target.contains("darwin") {
        configure_args.push("--disable-macos-framework".into());
    }
    if metadata.host != metadata.target {
        configure_args.push(format!("--host={}", metadata.target));
    }
    cmd(src_dir.join("configure"), &configure_args)
        .dir(&src_dir)
        .env_remove("CONFIG_SITE")
        .run()
        .expect("configure failed");

    let is_bsd = metadata.host.contains("dragonflybsd")
        || metadata.host.contains("freebsd")
        || metadata.host.contains("netbsd")
        || metadata.host.contains("openbsd");

    let make = if is_bsd { "gmake" } else { "make" };

    let mut make_flags = OsString::new();
    let mut make_args = vec![];
    if let Ok(s) = env::var("NUM_JOBS") {
        match env::var_os("CARGO_MAKEFLAGS") {
            // Only do this on non-Windows, since on Windows we could be
            // invoking mingw32-make which doesn't work with the jobserver.
            Some(s) if !cfg!(windows) => make_flags = s,

            // Otherwise, let's hope it understands `-jN`.
            _ => make_args.push(format!("-j{}", s)),
        }
    }

    // Try very hard to only build the components we need. We want to run
    // `cd lib && make install`, but that Makefile is incorrectly dependent
    // on targets in `include` and `common`, so build those directories first.
    for sub_dir in &["include", "common", "lib"] {
        cmd!(make, "install")
            .dir(src_dir.join(sub_dir))
            .env("MAKEFLAGS", &make_flags)
            .run()
            .expect("make failed");
    }

    validate_headers(&[install_dir.join("include")]);

    println!(
        "cargo:rustc-link-search=native={}",
        install_dir.join("lib").display(),
    );
    println!("cargo:rustc-link-lib=static={}", LIBRARY_NAME);
    println!("cargo:root={}", install_dir.display());

    #[cfg(all(feature = "gssapi-vendored", target_os = "linux"))]
    {
        // NOTE(zitsen): link against the vendored keyutils library to pass gssapi in centos7.
        let keyutils_src_dir = metadata.out_dir.join("keyutils");
        if !keyutils_src_dir.exists() {
            // We're not allowed to build in-tree directly, as ~/.cargo/registry is
            // globally shared, but sasl doesn't seem to support out-of-tree builds.
            // Work around the issue by copying sasl into OUT_DIR, and building
            // inside of *that* tree.
            cmd!("cp", "-R", "keyutils", &keyutils_src_dir)
                .run()
                .expect("failed making copy of keyutils tree");
        }

        cmd!(make, "libkeyutils.a", "CFLAGS=\"-fPIC\"")
            .dir(&keyutils_src_dir)
            .env("MAKEFLAGS", &make_flags)
            .run()
            .expect("make failed for keyutils");
        cmd!(
            "cp",
            keyutils_src_dir.join("libkeyutils.a"),
            install_dir.join("lib").join("libkeyutils.a")
        )
        .run()
        .expect("failed copying libkeyutils.a to install dir");
        println!("cargo:rustc-link-lib=static=keyutils");
    }
    #[cfg(feature = "gssapi-vendored")]
    {
        // NOTE(benesch): linking gssapi_krb5 and its dependencies should one
        // day be the responsibility of a libgssapi-sys project. Unfortunately
        // none of the several options on crates.io are presently up to snuff.
        println!(
            "cargo:rustc-link-search=native={}",
            PathBuf::from(env::var("DEP_KRB5_SRC_ROOT").unwrap())
                .join("lib")
                .display(),
        );
        println!("cargo:rustc-link-lib=static=gssapi_krb5");
        println!("cargo:rustc-link-lib=static=krb5");
        println!("cargo:rustc-link-lib=static=k5crypto");
        println!("cargo:rustc-link-lib=static=com_err");
        println!("cargo:rustc-link-lib=static=krb5support");
        // libresolv does not exist on BSD platforms; the relevant functions are
        // part of libc instead.
        if !is_bsd {
            println!("cargo:rustc-link-lib=resolv")
        }
    }
}

#[cfg(windows)]
fn build_sasl(metadata: &Metadata) {
    let build_dir = metadata.out_dir.join("build");
    let install_dir = metadata.out_dir.join("install");

    if metadata.host != metadata.target {
        panic!("cross-compilation on a Windows host is not supported");
    }

    if cfg!(feature = "gssapi-vendored") {
        panic!("the \"gssapi-vendored\" feature is not supported on Windows")
    }

    // The Windows build system doesn't seem to support out-of-tree builds, so
    // copy the source tree into the build directory since we're not allowed to
    // build in the checkout directly.
    let output = cmd!("robocopy", "sasl2", &build_dir, "/s", "/e")
        .unchecked()
        .run()
        .unwrap_or_else(|e| panic!("copying source tree failed: {}", e));
    // https://docs.microsoft.com/en-us/troubleshoot/windows-server/backup-and-storage/return-codes-used-robocopy-utility
    if !matches!(output.status.code(), Some(0..=7)) {
        panic!("copying source tree failed: {:?}", output);
    }

    // If OpenSSL has been vendored, point libsasl2 at the vendored headers.
    let mut openssl_flags = vec![];
    if cfg!(feature = "openssl-sys") {
        if let Ok(openssl_root) = env::var("DEP_OPENSSL_ROOT") {
            openssl_flags.push(format!(
                "OPENSSL_INCLUDE={}",
                Path::new(&openssl_root).join("include").display()
            ));
            openssl_flags.push(format!(
                "OPENSSL_LIBPATH={}",
                Path::new(&openssl_root).join("lib").display()
            ));
        }
    }

    let nmake = |args_in: &[&str]| {
        let mut args: Vec<String> = vec![
            "/f".into(),
            "NTMakefile".into(),
            format!("prefix={}", install_dir.display()),
        ];
        if cfg!(feature = "plain") {
            args.push("STATIC_PLAIN=1".into());
        }
        if cfg!(feature = "scram") {
            args.push("STATIC_SCRAM=1".into());
        }
        args.extend(openssl_flags.clone());
        for arg in args_in {
            args.push((*arg).into());
        }
        cmd("nmake", &args).dir(&build_dir)
    };

    // Build.
    nmake(&[])
        .run()
        .unwrap_or_else(|e| panic!("nmake build failed: {}", e));

    // Install.
    nmake(&["install"])
        .run()
        .unwrap_or_else(|e| panic!("nmake install failed: {}", e));

    validate_headers(&[install_dir.join("include")]);

    println!(
        "cargo:rustc-link-search=native={}",
        install_dir.join("lib").display(),
    );
    println!("cargo:rustc-link-lib=static={}", LIBRARY_NAME);
    println!("cargo:root={}", install_dir.display());
}

fn find_sasl(metadata: &Metadata) {
    if let (Some(lib_dir), Some(include_dir)) = (
        env::var_os("SASL2_LIB_DIR"),
        env::var_os("SASL2_INCLUDE_DIR"),
    ) {
        emit_found_sasl(metadata, PathBuf::from(lib_dir), PathBuf::from(include_dir));
        return;
    } else if let Some(install_dir) = env::var_os("SASL2_DIR") {
        emit_found_sasl(
            metadata,
            Path::new(&install_dir).join("lib"),
            Path::new(&install_dir).join("include"),
        );
        return;
    }

    #[cfg(feature = "pkg-config")]
    {
        if let Ok(pkg) = pkg_config::Config::new()
            .print_system_libs(false)
            .env_metadata(true)
            .probe("libsasl2")
        {
            validate_headers(&pkg.include_paths);
            return;
        }
    }

    if metadata.host == metadata.target
        && metadata.target.contains("darwin")
        && metadata.want_static != Some(true)
    {
        // We blindly trust that all macOS hosts have libsasl2 available in the
        // expected place. We used to actually check for the presence of the
        // library on the filesystem, but since macOS Big Sur, libraries no
        // longer actually exist on the filesystem.
        // See: https://news.ycombinator.com/item?id=23612772
        let lib_dir = PathBuf::from("/usr/lib");
        let include_dir =
            PathBuf::from("/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include");
        let metadata = Metadata {
            // Force a dynamic link, since the checks in `emit_found_sasl` won't
            // find a dylib.
            want_static: Some(false),
            ..metadata.clone()
        };
        emit_found_sasl(&metadata, lib_dir, include_dir);
        return;
    }

    for prefix in &[Path::new("/usr"), Path::new("/usr/local")] {
        for lib_dir in vec![
            prefix.join("lib"),
            prefix.join("lib64"),
            prefix.join("lib").join(&metadata.target),
            prefix
                .join("lib")
                .join(&metadata.target.replace("unknown-linux-gnu", "linux-gnu")),
        ] {
            let include_dir = prefix.join("include");
            if (lib_dir.join("libsasl2.a").exists()
                || lib_dir.join("libsasl2.so").exists()
                || lib_dir.join("libsasl2.dylib").exists())
                && include_dir.join("sasl").join("sasl.h").exists()
            {
                emit_found_sasl(metadata, lib_dir, include_dir);
                return;
            }
        }
    }

    panic!(
        "Unable to find libsasl2 on your system. Hints:

  * Have you installed the libsasl2 development package for your platform?
    On Debian-based systems, try libsasl2-dev. On RHEL-based systems, try
    cyrus-sasl-devel. On macOS with Homebrew, try cyrus-sasl.

  * Have you incorrectly set the SASL2_STATIC environment variable when your
    system only supports dynamic linking?

  * Are you willing to enable the `vendored` feature to instead build and link
    against a bundled copy of libsasl2?
"
    )
}

fn emit_found_sasl(metadata: &Metadata, lib_dir: PathBuf, include_dir: PathBuf) {
    validate_headers(&[include_dir]);

    let link_kind = match metadata.want_static {
        Some(true) => "static",
        Some(false) => "dylib",
        None if lib_dir.join("libsasl2.dylib").exists() || lib_dir.join("libsasl2.so").exists() => {
            "dylib"
        }
        None => "static",
    };

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib={}={}", link_kind, LIBRARY_NAME);
}

fn validate_headers(include_dirs: &[PathBuf]) {
    let mut cc = cc::Build::new();
    for dir in include_dirs {
        cc.include(dir);
    }
    cc.file("version.c");
    let mut lines = cc.expand().lines().collect::<Result<Vec<_>, _>>().unwrap();
    let step: u8 = lines.pop().unwrap().parse().unwrap();
    let minor: u8 = lines.pop().unwrap().parse().unwrap();
    let major: u8 = lines.pop().unwrap().parse().unwrap();
    if major != 2 || minor != 1 || !(26..=28).contains(&step) {
        panic!(
            "system libsasl is v{}.{}.{}, but this version of sasl2-sys \
             requires v2.1.26-v2.1.28",
            major, minor, step
        );
    }
    // Hack: encode version components as a single byte, so we can decode
    // them at compile-time in Rust.
    print!("cargo:rustc-env=SASL_VERSION_MAJOR=");
    io::stdout().write(&[major, b'\n']).unwrap();
    print!("cargo:rustc-env=SASL_VERSION_MINOR=");
    io::stdout().write(&[minor, b'\n']).unwrap();
    print!("cargo:rustc-env=SASL_VERSION_STEP=");
    io::stdout().write(&[step, b'\n']).unwrap();
}
